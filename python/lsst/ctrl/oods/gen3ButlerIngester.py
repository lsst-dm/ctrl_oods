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
from lsst.ctrl.oods.butlerIngester import ButlerIngester
from lsst.ctrl.oods.imageData import ImageData
from lsst.ctrl.oods.timeInterval import TimeInterval
from lsst.ctrl.oods.utils import Utils
from lsst.daf.butler import Butler
from lsst.daf.butler.registry import CollectionType
from lsst.obs.base.ingest import RawIngestTask, RawIngestConfig
from lsst.obs.base.utils import getInstrument
from astropy.time import Time
from astropy.time import TimeDelta
import astropy.units as u

LOGGER = logging.getLogger(__name__)


class Gen3ButlerIngester(ButlerIngester):
    """Processes files for ingestion into a Gen3 Butler.

    Parameters
    ----------
    config: `dict`
        configuration of this butler ingester
    publisher: `Publisher`
        RabbitMQ publisher
    publisher_queue: `str`
        The queue used to publish messages
    """
    def __init__(self, config, publisher=None, publisher_queue=None):
        self.config = config
        self.publisher = publisher
        self.publisher_queue = publisher_queue
        repo = self.config["repoDirectory"]
        instrument = self.config["instrument"]
        self.scanInterval = self.config["scanInterval"]
        self.olderThan = self.config["filesOlderThan"]
        self.collections = self.config["collections"]
        self.bad_file_dir = self.config["badFileDirectory"]
        self.staging_dir = self.config["stagingDirectory"]
        self.archiver = "undefined"
        self.INGEST_FAILURE = 1
        self.METADATA_FAILURE = 2

        try:
            butlerConfig = Butler.makeRepo(repo)
        except FileExistsError:
            butlerConfig = repo

        instr = getInstrument(instrument)
        run = instr.makeDefaultRawIngestRunName()
        opts = dict(run=run, writeable=True, collections=self.collections)
        self.butler = Butler(butlerConfig, **opts)

        # Register an instrument.
        instr.register(self.butler.registry)

        cfg = RawIngestConfig()
        cfg.transfer = "direct"
        self.task = RawIngestTask(config=cfg,
                                  butler=self.butler,
                                  on_success=self.on_success,
                                  on_ingest_failure=self.on_ingest_failure,
                                  on_metadata_failure=self.on_metadata_failure)

    def undef_metadata(self, filename):
        """Return a sparsely initialized metadata dictionary

        Parameters
        ----------
        filename: `str`
            name of the file specified by ingest

        Returns
        -------
        info: `dict`
            Dictionary containing file name, and uninitialized elements
        """
        info = dict()
        info['FILENAME'] = os.path.basename(filename)
        info['CAMERA'] = 'UNDEF'
        info['OBSID'] = '??'
        info['RAFT'] = 'R??'
        info['SENSOR'] = 'S??'
        return info

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
        msg['MSG_TYPE'] = 'IMAGE_IN_OODS'
        msg['ARCHIVER'] = self.archiver
        msg['STATUS_CODE'] = code
        msg['DESCRIPTION'] = description
        LOGGER.info(f"msg = {msg}")
        if self.publisher is None:
            return
        asyncio.create_task(self.publisher.publish_message(self.publisher_queue, msg))

    def on_success(self, datasets):
        """Callback used on successful ingest. Used to transmit
        successful data ingestion status

        Parameters
        ----------
        datasets: `list`
            list of DatasetRefs
        """
        for dataset in datasets:
            LOGGER.info(f"{dataset.path.ospath} file ingested")
            image_data = ImageData(dataset)
            self.transmit_status(image_data.get_info(), code=0, description="file ingested")

    def on_failure(self, filename, exc, code, reason):
        """Callback used on ingest failure. Used to transmit
        unsuccessful data ingestion status

        Parameters
        ----------
        filename: `ButlerURI`
            ButlerURI that failed in ingest
        exc: `Exception`
            Exception which explains what happened
        code: `int`
            error code
        reason: `str`
            reason for failure
        """
        real_file = filename.ospath
        self.move_file_to_bad_dir(real_file)
        cause = self.extract_cause(exc)
        info = self.undef_metadata(real_file)
        description = f"{reason}: {cause}"
        LOGGER.error(description)
        self.transmit_status(info, code=code, description=description)

    def on_ingest_failure(self, filename, exc):
        """Callback used on ingest failure. Used to transmit
        unsuccessful data ingestion status

        Parameters
        ----------
        filename: `ButlerURI`
            ButlerURI that failed in ingest
        exc: `Exception`
            Exception which explains what happened
        """
        self.on_failure(filename, exc, self.INGEST_FAILURE, "ingest failure")

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
        self.on_failure(filename, exc, self.METADATA_FAILURE, "metadata failure")

    def move_file_to_bad_dir(self, filename):
        """Move filename to a the "bad file" directory

        Parameters
        ----------
        filename: `str`
            file name of the file to move
        """
        bad_dir = Utils.create_bad_dirname(self.bad_file_dir, self.staging_dir, filename)
        try:
            shutil.move(filename, bad_dir)
        except Exception as fmException:
            LOGGER.info(f"Failed to move {filename} to {self.bad_dir} {fmException}")

    def ingest(self, archiver, filename):
        """Ingest a file into a butler

        Parameters
        ----------
        filename: `str`
            file name of the file to ingest
        """

        # Ingest image.
        self.archiver = archiver
        self.task.run([filename])

    def getName(self):
        """Return the name of this ingester

        Returns
        -------
        ret: `str`
            name of this ingester
        """
        return "gen3"

    async def run_task(self):
        """run the clean() method at the configured interval
        """
        seconds = TimeInterval.calculateTotalSeconds(self.scanInterval)
        while True:
            self.clean()
            await asyncio.sleep(seconds)

    def clean(self):
        """Remove all the datasets in the butler that
        were ingested before the configured Interval
        """

        # calculate the time value which is Time.now - the
        # "olderThan" configuration
        t = Time.now()
        interval = collections.namedtuple("Interval", self.olderThan.keys())(*self.olderThan.values())
        td = TimeDelta(interval.days*u.d + interval.hours * u.h +
                       interval.minutes*u.min + interval.seconds*u.s)
        t = t - td

        self.butler.registry.refresh()

        # get all datasets in these collections
        all_datasets = set(self.butler.registry.queryDatasets(datasetType=...,
                                                              collections=self.collections,
                                                              where="ingest_date < ref_date",
                                                              bind={"ref_date": t}))
        # get all TAGGED collections
        tagged_cols = list(self.butler.registry.queryCollections(collectionTypes=CollectionType.TAGGED))

        # get all TAGGED datasets
        tagged_datasets = set(self.butler.registry.queryDatasets(datasetType=..., collections=tagged_cols))

        # get a set of datasets in all_datasets, but not in tagged_datasets
        ref = all_datasets.difference(tagged_datasets)

        # References outside of the Butler's datastore
        # need to be cleaned up, since the Butler will
        # not delete those file artifacts.
        #
        # retrieve the URI,  prune the dataset from
        # the Butler, and if the URI was available,
        # remove it.
        for x in ref:
            LOGGER.info(f"removing {x}")

            uri = None
            try:
                uri = self.butler.getURI(x, collections=x.run)
            except Exception as e:
                LOGGER.warning(f"butler is missing uri for {x}: {e}")

            if uri is not None:
                uri.remove()

        self.butler.pruneDatasets(ref, purge=True, unstore=True)
