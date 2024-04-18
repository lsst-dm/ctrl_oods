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


class S3ButlerIngester(ButlerIngester):
    """Processes files from S3 into the Butler.

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
        self.scanInterval = self.config["scanInterval"]
        self.olderThan = self.config["filesOlderThan"]
        self.instrument = self.config["instrument"]
        self.collections = self.config["collections"]
        self.cleanCollections = self.config.get("cleanCollections", None)

        try:
            self.butlerConfig = Butler.makeRepo(repo)
        except FileExistsError:
            self.butlerConfig = repo

        try:
            self.butler = self.createButler()
        except Exception as exc:
            cause = self.extract_cause(exc)
            asyncio.create_task(self.csc.call_fault(code=2, report=f"failed to create Butler: {cause}"))
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
            Dictionary with file name and data_id elements
        """
        info = dict()
        dataset = data.datasets[0]
        info["FILENAME"] = f"{data.filename}"
        data_id = dataset.dataId
        info["CAMERA"] = data_id.get("instrument", "??")
        info["OBSID"] = data_id.get("exposure", "??")
        info["RAFT"] = self.extract_info_val(data_id, "raft", "R")
        info["SENSOR"] = self.extract_info_val(data_id, "detector", "S")
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
        info["FILENAME"] = filename
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
        LOGGER.info("on_success")
        for dataset in datasets:
            LOGGER.info(f"file {dataset.path} successfully ingested")
            image_data = ImageData(dataset)
            LOGGER.debug(f"{image_data.get_info()=}")
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
        LOGGER.info("on_ingest_failure")
        for f in exposures.files:
            cause = self.extract_cause(exc)
            info = self.rawexposure_info(f)
            self.transmit_status(info, code=1, description=f"ingest failure: {cause}")

    def on_metadata_failure(self, resource_path, exc):
        """Callback used on metadata extraction failure. Used to transmit
        unsuccessful data ingestion status

        Parameters
        ----------
        filename: `ResourcePath`
            ResourcePath that failed in ingest
        exc: `Exception`
            Exception which explains what happened
        """
        LOGGER.info("on_metadata_failure")
        real_file = f"{resource_path}"
        cause = self.extract_cause(exc)
        info = self.undef_metadata(real_file)
        self.transmit_status(info, code=2, description=f"metadata failure: {cause}")
