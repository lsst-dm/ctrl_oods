# This file is part of ctrl_oods
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import asyncio
import collections
import concurrent
import logging
import os
import shutil

import astropy.units as u
from astropy.time import Time, TimeDelta
from lsst.ctrl.oods.butlerIngester import ButlerIngester
from lsst.ctrl.oods.imageData import ImageData
from lsst.ctrl.oods.timeInterval import TimeInterval
from lsst.daf.butler import Butler
from lsst.daf.butler.registry import CollectionType
from lsst.obs.base.ingest import RawIngestConfig, RawIngestTask
from lsst.pipe.base import Instrument

LOGGER = logging.getLogger(__name__)


class Gen3ButlerIngester(ButlerIngester):
    """Processes files on behalf of a Gen3 Butler.

    Parameters
    ----------
    config: `dict`
        configuration of this butler ingester
    csc: `OodsCSC`
        Observatory Operations Data Service Commandable SAL component
    """

    def __init__(self, config, csc=None):
        self.csc = csc
        self.config = config

        repo = self.config["repoDirectory"]
        self.instrument = self.config["instrument"]
        self.scanInterval = self.config["scanInterval"]
        self.olderThan = self.config["filesOlderThan"]
        self.collections = self.config["collections"]
        self.cleanCollections = self.config.get("cleanCollections", None)

        self.staging_dir = self.config["stagingDirectory"]
        self.bad_file_dir = self.config["badFileDirectory"]

        try:
            self.butlerConfig = Butler.makeRepo(repo)
        except FileExistsError:
            self.butlerConfig = repo

        try:
            self.butler = self.createButler()
        except Exception as exc:
            cause = self.extract_cause(exc)
            asyncio.create_task(self.csc.call_fault(code=2, report=f"failure: {cause}"))
            return

        cfg = RawIngestConfig()
        cfg.transfer = "direct"
        self.task = RawIngestTask(
            config=cfg,
            butler=self.butler,
            on_success=self.on_success,
            on_ingest_failure=self.on_ingest_failure,
            on_metadata_failure=self.on_metadata_failure,
        )

    def rawexposure_info(self, data):
        """Return a sparsely initialized dictionary

        Parameters
        ----------
        data: `RawFileData`
            information about the raw file

        Returns
        -------
        info: `dict`
            Dictionary with file name and dataId elements
        """
        info = dict()
        dataset = data.datasets[0]
        info["FILENAME"] = os.path.basename(data.filename.ospath)
        dataId = dataset.dataId
        info["CAMERA"] = dataId.get("instrument", "??")
        info["OBSID"] = dataId.get("exposure", "??")
        info["RAFT"] = self.extract_info_val(dataId, "raft", "R")
        info["SENSOR"] = self.extract_info_val(dataId, "detector", "S")
        return info

    def undef_metadata(self, filename):
        """Return a sparsely initialized metadata dictionary

        Parameters
        ----------
        filename: `str`
            name of the file specified by ingest

        Returns
        -------
        info: `dict`
            Dictionary containing file name and placeholders
        """
        info = dict()
        info["FILENAME"] = os.path.basename(filename)
        info["CAMERA"] = "UNDEF"
        info["OBSID"] = "??"
        info["RAFT"] = "R??"
        info["SENSOR"] = "S??"
        return info

    def on_success(self, datasets):
        """Callback used on successful ingest. Used to transmit
        successful data ingestion status

        Parameters
        ----------
        datasets: `list`
            list of DatasetRefs
        """
        for dataset in datasets:
            LOGGER.info("file %s successfully ingested", dataset.path.ospath)
            image_data = ImageData(dataset)
            LOGGER.debug("image_data.get_info() = %s", image_data.get_info())
            self.transmit_status(image_data.get_info(), code=0, description="file ingested")

    def on_ingest_failure(self, exposures, exc):
        """Callback used on ingest failure. Used to transmit
        unsuccessful data ingestion status

        Parameters
        ----------
        exposures: `RawExposureData`
            exposures that failed in ingest
        exc: `Exception`
            Exception which explains what happened

        """
        for f in exposures.files:
            real_file = f.filename.ospath
            self.move_file_to_bad_dir(real_file)
            cause = self.extract_cause(exc)
            info = self.rawexposure_info(f)
            self.transmit_status(info, code=1, description=f"ingest failure: {cause}")

    def on_metadata_failure(self, filename, exc):
        """Callback used on metadata extraction failure. Used to transmit
        unsuccessful data ingestion status

        Parameters
        ----------
        filename: `ButlerURI`
            ButlerURI that failed in ingest
        exc: `Exception`
            Exception which explains what happened
        """
        real_file = filename.ospath
        self.move_file_to_bad_dir(real_file)

        cause = self.extract_cause(exc)
        info = self.undef_metadata(real_file)
        self.transmit_status(info, code=2, description=f"metadata failure: {cause}")

    def move_file_to_bad_dir(self, filename):
        bad_dir = self.create_bad_dirname(self.bad_file_dir, self.staging_dir, filename)
        try:
            shutil.move(filename, bad_dir)
        except Exception as e:
            LOGGER.info("Failed to move %s to %s: %s", filename, bad_dir, e)
