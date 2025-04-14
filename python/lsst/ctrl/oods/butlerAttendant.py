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
import os.path
from concurrent.futures import ThreadPoolExecutor

import astropy.units as u
from astropy.time import Time, TimeDelta
from lsst.ctrl.oods.timeInterval import TimeInterval
from lsst.ctrl.oods.utils import Utils
from lsst.daf.butler import Butler, CollectionType
from lsst.obs.base import DefineVisitsTask
from lsst.obs.base.ingest import RawIngestConfig, RawIngestTask
from lsst.pipe.base import Instrument

LOGGER = logging.getLogger(__name__)


class ButlerAttendant:
    """Interface class for processing files for a butler."""

    SUCCESS = 0
    FAILURE = 1

    def __init__(self, config, csc=None):
        self.csc = csc
        self.config = config

        self.status_queue = asyncio.Queue()
        repo = self.config["repoDirectory"]
        self.instrument = self.config["instrument"]
        self.scanInterval = self.config["scanInterval"]
        self.collections = self.config["collections"]
        self.cleanCollections = self.config.get("cleanCollections")
        self.s3profile = self.config.get("s3profile", None)

        LOGGER.info(f"Using Butler repo located at {repo}")
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
        define_visits_config = DefineVisitsTask.ConfigClass()
        define_visits_config.groupExposures = "one-to-one-and-by-counter"
        self.visit_definer = DefineVisitsTask(config=define_visits_config, butler=self.butler)

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

    async def ingest(self, file_list):
        """Ingest a list of files into a butler

        Parameters
        ----------
        file_list: `list`
            files to ingest
        """

        # Ingest images.
        await asyncio.sleep(0)
        new_list = file_list
        if self.s3profile:
            # rewrite URI to add s3profile
            new_list = [s.replace(netloc=f"{self.s3profile}@{s.netloc}") for s in file_list]
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            try:
                LOGGER.info("about to ingest")
                await loop.run_in_executor(executor, self.task.run, new_list)
                LOGGER.info("done with ingest")
            except RuntimeError as re:
                LOGGER.info(f"{re}")
            except Exception as e:
                LOGGER.exception(f"Exception! {e=}")

    def create_bad_dirname(self, bad_dir_root, staging_dir_root, original):
        """Create a full path to a directory contained in the
        'bad directory' hierarchy; this retains the subdirectory structure
        created where the file was staged, where the uningestable file will
        be placed.

        Parameters
        ----------
        bad_dir_root : `str`
            Root of the bad directory hierarchy
        staging_dir_root : `str`
            Root of the staging directory hierarchy
        original : `str`
            Original directory location of bad file

        Returns
        -------
        newdir : `str`
            new directory name
        """
        # strip the original directory location, except for the date
        newfile = Utils.strip_prefix(original, staging_dir_root)

        # split into subdir and filename
        head, tail = os.path.split(newfile)

        # create subdirectory path name for directory with date
        newdir = os.path.join(bad_dir_root, head)

        # create the directory, and hand the name back
        os.makedirs(newdir, exist_ok=True)

        return newdir

    def extract_cause(self, e):
        """extract the cause of an exception

        Parameters
        ----------
        e : `BaseException`
            exception to extract cause from

        Returns
        -------
        s : `str`
            A string containing the cause of an exception
        """
        if e.__cause__ is None:
            return f"{e}"
        cause = self.extract_cause(e.__cause__)
        if cause is None:
            return f"{str(e.__cause__)}"
        else:
            return f"{str(e.__cause__)};  {cause}"

    def print_msg(self, msg):
        """Print message dictionary - used if a CSC has not been created

        Parameters
        ----------
        msg: `dict`
            Dictionary to print
        """

        LOGGER.info(f"would have sent {msg=}")

    def transmit_status(self, metadata, code, description):
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
        LOGGER.debug("msg: %s, code: %s, description: %s", msg, code, description)
        if self.csc is None:
            self.print_msg(msg)
            return

        loop = asyncio.new_event_loop()

        try:
            loop.run_until_complete(self.status_queue.put(msg))
        except Exception as e:
            LOGGER.info(e)
        finally:
            loop.close()

    async def clean_task(self):
        """run the clean() method at the configured interval"""
        seconds = TimeInterval.calculateTotalSeconds(self.scanInterval)
        LOGGER.info("clean_task created!")
        while True:
            LOGGER.debug("cleaning")
            await self.clean()
            LOGGER.debug("sleeping for %d seconds", seconds)
            await asyncio.sleep(seconds)

    async def clean(self):
        """Remove all the datasets in the butler that
        were ingested before the configured time interval
        """
        for entry in self.cleanCollections:
            collection = entry["collection"]
            olderThan = entry["filesOlderThan"]
            loop = asyncio.get_event_loop()
            try:
                with ThreadPoolExecutor() as executor:
                    await loop.run_in_executor(executor, self.cleanCollection, collection, olderThan)
            except Exception as e:
                LOGGER.error(e)

    async def send_status_task(self):
        LOGGER.debug("send_status_task started")
        while True:
            try:
                msg = await self.status_queue.get()
                await self.csc.send_imageInOODS(msg)
                self.status_queue.task_done()
            except asyncio.exceptions.CancelledError:
                LOGGER.info("status get task cancelled")
                return
            except Exception as e:
                LOGGER.info(f"{e=}")

    def cleanCollection(self, collection, olderThan):
        """Remove all the datasets in the butler that
        were ingested before the configured Interval

        Parameters
        ----------
        collection: `str`
            collection to clean up
        olderThan: `dict`
            time interval
        """

        # calculate the time value which is Time.now - the
        # "olderThan" configuration
        t = Time.now()
        interval = collections.namedtuple("Interval", olderThan.keys())(*olderThan.values())
        td = TimeDelta(
            interval.days * u.d + interval.hours * u.h + interval.minutes * u.min + interval.seconds * u.s
        )
        t = t - td

        LOGGER.info("cleaning collections")
        LOGGER.debug("about to createButler()")
        butler = self.createButler()

        LOGGER.debug("about to refresh()")
        butler.registry.refresh()

        # get all datasets in these collections
        LOGGER.debug("about to call queryDatasets")
        all_datasets = set(
            butler.registry.queryDatasets(
                datasetType=...,
                collections=[collection],
                where="ingest_date < ref_date",
                bind={"ref_date": t},
            )
        )
        LOGGER.debug("done calling queryDatasets")

        # get all TAGGED collections
        LOGGER.debug("about to call queryCollections")
        tagged_cols = list(butler.registry.queryCollections(collectionTypes=CollectionType.TAGGED))
        LOGGER.debug("done calling queryCollections")

        # Note: The code below is to get around an issue where passing
        # an empty list as the collections argument to queryDatasets
        # returns all datasets.
        if tagged_cols:
            # get all TAGGED datasets
            LOGGER.debug("about to run queryDatasets for TAGGED collections")
            tagged_datasets = set(butler.registry.queryDatasets(datasetType=..., collections=tagged_cols))
            LOGGER.debug("done running queryDatasets for TAGGED collections; differencing datasets")

            # get a set of datasets in all_datasets, but not in tagged_datasets
            ref = all_datasets.difference(tagged_datasets)
            LOGGER.debug("done differencing datasets")
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
            uri = None
            try:
                uri = butler.getURI(x)
            except Exception as e:
                LOGGER.warning("butler is missing uri for %s: %s", x, e)

            if uri is not None:
                LOGGER.info("removing %s", uri)
                try:
                    uri.remove()
                except Exception as e:
                    LOGGER.warning("couldn't remove %s: %s", uri, e)

        LOGGER.debug("about to run pruneDatasets")
        butler.pruneDatasets(ref, purge=True, unstore=True)
        LOGGER.debug("done running pruneDatasets")
        LOGGER.info("done cleaning collections")

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
        info["FILENAME"] = "??"
        dataId = dataset.dataId
        info["CAMERA"] = dataId.get("instrument", "??")
        info["OBSID"] = str(dataId.get("exposure", "??"))
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
        info["FILENAME"] = filename
        info["CAMERA"] = "UNDEF"
        info["OBSID"] = "??"
        info["RAFT"] = "R??"
        info["SENSOR"] = "S??"
        return info

    def definer_run(self, file_datasets):
        ids = []
        for fds in file_datasets:
                refs = fds.refs
                ids.extend([ref.dataId for ref in refs])
        try:
            self.visit_definer.run(ids)
            LOGGER.debug("Defined visits for %s", ids)
        except Exception as e:
            LOGGER.exception(e)
