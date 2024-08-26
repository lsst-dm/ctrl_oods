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
import logging
import os
import shutil

import astropy.units as u
from astropy.time import Time, TimeDelta
from lsst.ctrl.oods.butlerIngester import ButlerIngester
from lsst.ctrl.oods.imageData import ImageData
from lsst.ctrl.oods.timeInterval import TimeInterval
from lsst.daf.butler import Butler, CollectionType
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

    def createButler(self):
        instr = Instrument.from_string(self.instrument)
        run = instr.makeDefaultRawIngestRunName()
        opts = dict(run=run, writeable=True, collections=self.collections)
        butler = Butler(self.butlerConfig, **opts)

        # Register an instrument.
        instr.register(butler.registry)

        return butler

    def extract_info_val(self, dataId, key, prefix):
        if key in dataId:
            return f"{prefix}{dataId[key]:02d}"
        return f"{prefix}??"

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

    async def print_msg(self, msg):
        """Print message dictionary - used if a CSC has not been created

        Parameters
        ----------
        msg: `dict`
            Dictionary to print
        """

        LOGGER.info(f"would have sent {msg=}")
        await asyncio.sleep(0)

    async def transmit_status(self, metadata, code, description):
        """Transmit a message with given metadata, status code and description

        Parameters
        ----------
        metadata: `dict`
            dictionary containing meta data about the image
        code: `int`
            status code
        description: `str`
            description of the ingestion status
        """
        msg = dict(metadata)
        msg["MSG_TYPE"] = "IMAGE_IN_OODS"
        msg["STATUS_CODE"] = code
        msg["DESCRIPTION"] = description
        LOGGER.info("msg: %s, code: %s, description: %s", msg, code, description)
        if self.csc is None:
            await self.print_msg(msg)
            return
        await self.csc.send_imageInOODS(msg)

    def on_success(self, datasets):
        asyncio.create_task(self._on_success(datasets))

    async def _on_success(self, datasets):
        """Callback used on successful ingest. Used to transmit
        successful data ingestion status

        Parameters
        ----------
        datasets: `list`
            list of DatasetRefs
        """
        for dataset in datasets:
            await asyncio.sleep(0)
            LOGGER.info("file %s successfully ingested", dataset.path.ospath)
            image_data = ImageData(dataset)
            LOGGER.debug("image_data.get_info() = %s", image_data.get_info())
            await self.transmit_status(image_data.get_info(), code=0, description="file ingested")

    def on_ingest_failure(self, exposures, exc):
        asyncio.create_task(self._on_ingest_failure(exposures, exc))

    async def _on_ingest_failure(self, exposures, exc):
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
            await asyncio.sleep(0)
            real_file = f.filename.ospath
            self.move_file_to_bad_dir(real_file)
            cause = self.extract_cause(exc)
            info = self.rawexposure_info(f)
            await self.transmit_status(info, code=1, description=f"ingest failure: {cause}")

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
        asyncio.create_task(self.transmit_status(info, code=2, description=f"metadata failure: {cause}"))

    def move_file_to_bad_dir(self, filename):
        bad_dir = self.create_bad_dirname(self.bad_file_dir, self.staging_dir, filename)
        try:
            shutil.move(filename, bad_dir)
        except Exception as e:
            LOGGER.info("Failed to move %s to %s: %s", filename, bad_dir, e)

    async def ingest(self, file_list):
        """Ingest a list of files into a butler

        Parameters
        ----------
        file_list: `list`
            files to ingest
        """

        # Ingest image.
        await asyncio.sleep(0)
        self.task.run(file_list)

    def getName(self):
        """Return the name of this ingester

        Returns
        -------
        ret: `str`
            name of this ingester
        """
        return "gen3"

    async def clean_task(self):
        """run the clean() method at the configured interval"""
        seconds = TimeInterval.calculateTotalSeconds(self.scanInterval)
        while True:
            if self.csc:
                self.csc.log.info("calling butler repo clean started")

            await self.clean()

            if self.csc:
                self.csc.log.info("done cleaning butler repo; sleeping for %d seconds", seconds)
            await asyncio.sleep(seconds)

    async def clean(self):
        """Remove all the datasets in the butler that
        were ingested before the configured Interval
        """

        await asyncio.sleep(0)
        # calculate the time value which is Time.now - the
        # "olderThan" configuration
        t = Time.now()
        interval = collections.namedtuple("Interval", self.olderThan.keys())(*self.olderThan.values())
        td = TimeDelta(
            interval.days * u.d + interval.hours * u.h + interval.minutes * u.min + interval.seconds * u.s
        )
        t = t - td

        LOGGER.info("about to createButler()")
        butler = self.createButler()
        await asyncio.sleep(0)

        LOGGER.info("about to refresh()")
        butler.registry.refresh()
        await asyncio.sleep(0)

        LOGGER.info("about to run queryDatasets")
        # get all datasets in these collections
        allCollections = self.collections if self.cleanCollections is None else self.cleanCollections
        all_datasets = set(
            butler.registry.queryDatasets(
                datasetType=...,
                collections=allCollections,
                where="ingest_date < ref_date",
                bind={"ref_date": t},
            )
        )
        LOGGER.info("done running queryDatasets")
        await asyncio.sleep(0)

        LOGGER.info("about to run queryCollections")
        # get all TAGGED collections
        tagged_cols = list(butler.registry.queryCollections(collectionTypes=CollectionType.TAGGED))
        LOGGER.info("done running queryCollections")

        await asyncio.sleep(0)
        # Note: The code below is to get around an issue where passing
        # an empty list as the collections argument to queryDatasets
        # returns all datasets.
        if tagged_cols:
            # get all TAGGED datasets
            LOGGER.info("about to run queryDatasets for TAGGED collections")
            tagged_datasets = set(butler.registry.queryDatasets(datasetType=..., collections=tagged_cols))
            LOGGER.info("done running queryDatasets for TAGGED collections; differencing datasets")
            await asyncio.sleep(0)

            # get a set of datasets in all_datasets, but not in tagged_datasets
            ref = all_datasets.difference(tagged_datasets)
            LOGGER.info("done differencing datasets")
            await asyncio.sleep(0)
        else:
            # no TAGGED collections, so use all_datasets
            ref = all_datasets

        # References outside of the Butler's datastore
        # need to be cleaned up, since the Butler will
        # not delete those file artifacts.
        #
        # retrieve the URI,  prune the dataset from
        # the Butler, and if the URI was available,
        # remove it.
        for x in ref:
            await asyncio.sleep(0)
            uri = None
            try:
                uri = butler.getURI(x, collections=x.run)
            except Exception as e:
                LOGGER.warning("butler is missing uri for %s: %s", x, e)

            if uri is not None:
                LOGGER.info("removing %s", uri)
                try:
                    uri.remove()
                except Exception as e:
                    LOGGER.info("error removing %s: %s", uri, e)

        await asyncio.sleep(0)
        LOGGER.info("about to run pruneDatasets")
        butler.pruneDatasets(ref, purge=True, unstore=True)
        LOGGER.info("done running pruneDatasets")
