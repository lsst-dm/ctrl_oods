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
import shutil
from lsst.ctrl.oods.butlerBroker import ButlerBroker
from lsst.ctrl.oods.timeInterval import TimeInterval
from lsst.ctrl.oods.imageFile import ImageFile
from lsst.daf.butler import Butler
from lsst.obs.base.ingest import RawIngestTask, RawIngestConfig
from lsst.obs.base.utils import getInstrument
from astropy.time import Time
from astropy.time import TimeDelta
import astropy.units as u

LOGGER = logging.getLogger(__name__)


class Gen3ButlerBroker(ButlerBroker):
    """Processes files on behalf of a Gen3 Butler.

    Parameters
    ----------
    config: `dict`
        configuration of this butler ingester
    """
    def __init__(self, config, publisher, publisher_queue):
        self.config = config
        self.publisher = publisher
        self.publisher_queue = publisher_queue

        repo = self.config["repoDirectory"]
        instrument = self.config["instrument"]
        self.scanInterval = self.config["scanInterval"]
        self.olderThan = self.config["filesOlderThan"]
        self.collections = self.config["collections"]

        self.repo_dir = self.config["repoDirectory"]
        self.staging_dir = self.config["stagingDirectory"]
        self.bad_file_dir = self.config["badFileDirectory"]

        register = False
        try:
            butlerConfig = Butler.makeRepo(repo)
            register = True
        except FileExistsError:
            butlerConfig = repo

        instr = getInstrument(instrument)
        run = instr.makeDefaultRawIngestRunName()
        opts = dict(run=run, writeable=True, collections=self.collections)
        self.butler = Butler(butlerConfig, **opts)

        if register:
            # Register an instrument.
            instr.register(self.butler.registry)

        cfg = RawIngestConfig()
        cfg.transfer = "direct"
        self.task = RawIngestTask(config=cfg,
                                  butler=self.butler,
                                  on_success=self.on_success,
                                  on_ingest_failure=self.on_ingest_failure,
                                  on_metadata_failure=self.on_metadata_failure)

    def transmitStatus(self, filename, code, description):
        if self.publisher is None:
            return
        image_file = ImageFile(filename)
        msg = dict()
        msg['ARCHIVER'] = image_file.archiver
        msg['CAMERA'] = image_file.camera
        msg['FILENAME'] = image_file.filename
        msg['OBSID'] = image_file.obsid
        msg['RAFT'] = image_file.raft
        msg['SENSOR'] = image_file.sensor
        msg['MSG_TYPE'] = 'IMAGE_IN_OODS'
        msg['STATUS_CODE'] = code
        msg['DESCRIPTION'] = description

        LOGGER.info(f"WOULD SEND: Sending message: {msg}")
        # task = asyncio.create_task(
        # self.publisher.publish_message(self.publisher_queue, msg))

    def on_success(self, datasets):
        # datasets is a list of lsst.daf.butler.core.fileDataset.FileDataset
        # dataset is a lsst.daf.butler.core._butlerUri.file.ButlerFileURI
        for dataset in datasets:
            LOGGER.info(f"{dataset.path.ospath} file ingested")
            self.transmit_status(dataset.path.ospath, code=0, description="file ingested")

    def on_ingest_failure(self, datasets, exc):
        self.move_to_bad_dir(datasets)
        for dataset in datasets:
            LOGGER.info(f"{dataset.path.ospath} ingest failure")
            cause = self.extract_cause(exc)
            self.transmit_status(dataset.path.ospath, code=1, description=f"ingest failure: {cause}")

    def on_metadata_failure(self, datasets, exc):
        self.move_to_bad_dir(datasets)
        for dataset in datasets:
            LOGGER.info(f"{dataset.path.ospath} metadata failure")
            cause = self.extract_cause(exc)
            self.transmit_status(dataset.path.ospath, code=2, description=f"metadata failure: {cause}")

    def move_to_bad_dir(self, datasets):
        for dataset in datasets:
            self.move_file_to_bad_dir(dataset.path.ospath)

    def move_file_to_bad_dir(self, filename):
        bad_dir = self.create_bad_dirname(self.bad_file_dir, self.staging_dir, filename)
        try:
            shutil.move(filename, bad_dir)
        except Exception as fmException:
            LOGGER.info(f"Failed to move {filename} to {self.bad_dir} {fmException}")

    def ingest(self, file_list):
        """Ingest a list of files into a butler

        Parameters
        ----------
        file_list: `list`
            files to ingest
        """

        # Ingest image.
        try:
            print(f"file_list: {type(file_list)}")
            self.task.run(file_list)
        except Exception as e:
            LOGGER.info(f"Ingestion failure: {e}")

    def getName(self):
        """Return the name of this ingester

        Returns
        -------
        ret: `str`
            name of this ingester
        """
        return "gen3"

    async def clean_task(self):
        """run the clean() method at the configured interval
        """
        print("gen3ButlerBroker: clean_task")
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
        print("gen3ButlerBroker: clean")
        t = Time.now()
        interval = collections.namedtuple("Interval", self.olderThan.keys())(*self.olderThan.values())
        td = TimeDelta(interval.days*u.d + interval.hours * u.h +
                       interval.minutes*u.min + interval.seconds*u.s)
        t = t - td

        # get the datasets
        ref = set(self.butler.registry.queryDatasets(datasetType=...,
                                                     collections=self.collections,
                                                     where="ingest_date < ref_date",
                                                     bind={"ref_date": t}))

        # References outside of the Butler's datastore
        # need to be cleaned up, since the Butler will
        # not delete those file artifacts.
        #
        # retrieve the URI,  prune the dataset from
        # the Butler, and if the URI was available,
        # remove it.
        for x in ref:
            print(f"removing {x}")

            uri = None
            try:
                uri = self.butler.getURI(x, collections=x.run)
            except Exception as e:
                print(f"butler is missing uri for {x}: {e}")

            if uri is not None:
                uri.remove()

        self.butler.pruneDatasets(ref, purge=True, unstore=True)
