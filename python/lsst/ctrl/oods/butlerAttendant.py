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
import os.path

import astropy.units as u
from astropy.time import Time, TimeDelta
from lsst.ctrl.oods.timeInterval import TimeInterval
from lsst.ctrl.oods.utils import Utils
from lsst.daf.butler import Butler
from lsst.daf.butler.registry import CollectionType
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

        repo = self.config["repoDirectory"]
        self.instrument = self.config["instrument"]
        self.scanInterval = self.config["scanInterval"]
        self.olderThan = self.config["filesOlderThan"]
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
        define_visits_config = DefineVisitsTask.ConfigClass()
        define_visits_config.groupExposures = "one-to-one"
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
        try:
            loop = asyncio.get_running_loop()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                await loop.run_in_executor(pool, self.task.run, file_list)
        except Exception as e:
            LOGGER.info("Ingestion issue %s", e)

    def on_success(self, datasets):
        pass

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
            Original directory location

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
        LOGGER.info("msg: %s, code: %s, description: %s", msg, code, description)
        if self.csc is None:
            return
        asyncio.run(self.csc.send_imageInOODS(msg))

    async def clean_task(self):
        """run the clean() method at the configured interval"""
        seconds = TimeInterval.calculateTotalSeconds(self.scanInterval)
        LOGGER.info("clean_task created!")
        while True:
            LOGGER.debug("cleaning")
            try:
                loop = asyncio.get_running_loop()
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    await loop.run_in_executor(pool, self.clean)
            except Exception as e:
                LOGGER.info("Exception: %s", e)
            LOGGER.debug("sleeping for %d seconds", seconds)
            await asyncio.sleep(seconds)

    def clean(self):
        """Remove all the datasets in the butler that
        were ingested before the configured Interval
        """

        # calculate the time value which is Time.now - the
        # "olderThan" configuration
        t = Time.now()
        interval = collections.namedtuple("Interval", self.olderThan.keys())(*self.olderThan.values())
        td = TimeDelta(
            interval.days * u.d + interval.hours * u.h + interval.minutes * u.min + interval.seconds * u.s
        )
        t = t - td

        butler = self.createButler()

        butler.registry.refresh()

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

        # get all TAGGED collections
        tagged_cols = list(butler.registry.queryCollections(collectionTypes=CollectionType.TAGGED))

        # Note: The code below is to get around an issue where passing
        # an empty list as the collections argument to queryDatasets
        # returns all datasets.
        if tagged_cols:
            # get all TAGGED datasets
            tagged_datasets = set(butler.registry.queryDatasets(datasetType=..., collections=tagged_cols))

            # get a set of datasets in all_datasets, but not in tagged_datasets
            ref = all_datasets.difference(tagged_datasets)
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
                # uri = butler.getURI(x, collections=x.run)
                uri = butler.getURI(x)
            except Exception as e:
                LOGGER.warning("butler is missing uri for %s: %s", x, e)

            if uri is not None:
                LOGGER.info("removing %s", uri)
                try:
                    uri.remove()
                except Exception as e:
                    LOGGER.warning("couldn't remove %s: %s", uri, e)

        butler.pruneDatasets(ref, purge=True, unstore=True)

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
        info["FILENAME"] = filename
        info["CAMERA"] = "UNDEF"
        info["OBSID"] = "??"
        info["RAFT"] = "R??"
        info["SENSOR"] = "S??"
        return info

    def definer_run(self, file_datasets):
        for fds in file_datasets:
            refs = fds.refs
            ids = [ref.dataId for ref in refs]
            self.visit_definer.run(ids)
            LOGGER.info("Defined visits for %s", ids)