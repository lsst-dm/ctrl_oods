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
import logging
import os
import shutil
import tempfile
from pathlib import PurePath

import lsst.utils.tests
import yaml
from lsst.ctrl.oods.directoryScanner import DirectoryScanner
from lsst.ctrl.oods.fileIngester import FileIngester
from lsst.daf.butler import Butler
from lsst.obs.base.ingest import RawIngestConfig, RawIngestTask
from lsst.pipe.base import Instrument
from heartbeat_base import HeartbeatBase


class CollectionTestCase(HeartbeatBase):
    """Test Collections

    These tests check whether or not files are deleted properly
    when specifying cleanCollections in the yaml configuration file
    """

    def setUp(self):
        """create a butler and ingest one file into it, outside
        of the OODS process.
        """
        self.repoDir = tempfile.mkdtemp()
        collections = ["LATISS/runs/quickLook"]

        config = Butler.makeRepo(self.repoDir)
        instr = Instrument.from_string("lsst.obs.lsst.Latiss")
        run = instr.makeDefaultRawIngestRunName()
        opts = dict(run=run, writeable=True, collections=collections)
        butler = Butler(config, **opts)

        # Register an instrument.
        instr.register(butler.registry)

        # load a file into LATISS/runs/quickLook
        cfg = RawIngestConfig()
        cfg.transfer = "direct"
        task = RawIngestTask(config=cfg, butler=butler)
        test_file = self.copy_to_test_location("2020032700020-det000.fits.fz")
        task.run(run="LATISS/runs/quickLook", files=[test_file])

    async def load(self, fits_name, config_name):
        """stage test data and set up ingester tasks

        Parameters
        ----------
        fits_name : `str`
            name of the fits file to load
        config_name : `str`
            name of the OODS configuration file to load

        Returns
        -------
        staged_file : `str`
            Path to file to after it has been staged for ingestion
        task_list : `list`
            A list of tasks to run for this butler ingester (ingest, cleanup)
        """

        # create a path to the configuration file

        testdir = os.path.abspath(os.path.dirname(__file__))
        config_file = os.path.join(testdir, "etc", config_name)

        # path to the FITS file to ingest

        test_dir = os.path.abspath(os.path.dirname(__file__))
        data_file = os.path.join(test_dir, "data", fits_name)

        # load the YAML configuration

        with open(config_file, "r") as f:
            config = yaml.safe_load(f)

        # extract parts of the ingester configuration
        # and alter the image staging directory to point
        # at the temporary directories created for this test

        ingesterConfig = config["ingester"]
        self.imageStagingDir = tempfile.mkdtemp()
        ingesterConfig["imageStagingDirectory"] = self.imageStagingDir

        self.badDir = tempfile.mkdtemp()
        butlerConfig = ingesterConfig["butlers"][0]["butler"]
        butlerConfig["badFileDirectory"] = self.badDir
        self.stagingDirectory = tempfile.mkdtemp()
        butlerConfig["stagingDirectory"] = self.stagingDirectory

        butlerConfig["repoDirectory"] = self.repoDir

        self.collections = butlerConfig["collections"]
        logging.info(f"{self.collections=}")

        # copy the FITS file to it's test location

        subDir = tempfile.mkdtemp(dir=self.imageStagingDir)
        self.destFile = os.path.join(subDir, fits_name)
        shutil.copyfile(data_file, self.destFile)

        # setup directory to scan for files in the image staging directory
        # and ensure one file is there
        ingesterConfig = config["ingester"]
        image_staging_dir = ingesterConfig["imageStagingDirectory"]
        scanner = DirectoryScanner([image_staging_dir])
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        # create the file ingester, get all tasks associated with it, and
        # create the tasks
        ingester = FileIngester(config)
        butler_clean = ingester.getButlerCleanMethods()

        # check to see that the file is there before ingestion
        self.assertTrue(os.path.exists(self.destFile))

        # trigger the ingester
        staged_files = ingester.stageFiles([self.destFile])
        await ingester.ingest(staged_files)

        # make sure staging area is now empty
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        # Check to see that the file was ingested.
        # Recall that files start in the image staging area, and are
        # moved to the OODS staging area before ingestion. On "direct"
        # ingestion, this is where the file is located.  This is a check
        # to be sure that happened.
        name = self.strip_prefix(self.destFile, self.imageStagingDir)
        staged_file = os.path.join(self.stagingDirectory, name)
        self.assertTrue(os.path.exists(staged_file))

        # this file should now not exist
        self.assertFalse(os.path.exists(self.destFile))

        return staged_file, butler_clean

    async def testCollectionsTestCase(self):
        """Test that all collections are removed"""
        fits_name = "AT_O_20221122_000951_R00_S00.fits.fz"
        stage_file, butler_clean = await self.load(fits_name, "collection_test_1.yaml")

        self.check_exposure_count("2020032700020", "LATISS/runs/quickLook", 1)
        self.check_exposure_count("2022112200951", "LATISS/raw/all", 1)

        await asyncio.sleep(5)
        for clean in butler_clean:
            await asyncio.create_task(clean())

        self.assertFalse(os.path.exists(stage_file))

        self.check_exposure_count("2020032700020", "LATISS/runs/quickLook", 0)
        self.check_exposure_count("2022112200951", "LATISS/raw/all", 0)

    async def testDoNoDeleteCollectionsTestCase(self):
        """Test that some collections are not removed"""
        fits_name = "AT_O_20221122_000951_R00_S00.fits.fz"
        stage_file, butler_clean = await self.load(fits_name, "collection_test_2.yaml")

        self.check_exposure_count("2020032700020", "LATISS/runs/quickLook", 1)
        self.check_exposure_count("2022112200951", "LATISS/raw/all", 1)

        await asyncio.sleep(5)
        for clean in butler_clean:
            await asyncio.create_task(clean())

        self.assertFalse(os.path.exists(stage_file))

        self.check_exposure_count("2020032700020", "LATISS/runs/quickLook", 1)
        self.check_exposure_count("2022112200951", "LATISS/raw/all", 0)

    def copy_to_test_location(self, fits_name):
        # location of test file
        test_dir = os.path.abspath(os.path.dirname(__file__))
        data_file = os.path.join(test_dir, "data", fits_name)

        # create a temp space, and copy the file there
        sub_dir = tempfile.mkdtemp()
        dest_file = os.path.join(sub_dir, fits_name)
        logging.info("copying file {data_file=} to {dest_file=}")
        shutil.copyfile(data_file, dest_file)
        logging.info("file copied")
        return dest_file

    def check_exposure_count(self, exposure, collections, num_expected):
        """check the number of exposures we expect to find

        Parameters
        ----------
        exposure : `str`
            the name of the exposure to check
        collections : `list`
            a list of collections to check
        num_expected : `int`
            the number of matches to expect to find in all the collections
        """

        butler = Butler(self.repoDir, writeable=True)

        # get the dataset
        logging.info(f"{collections=}")
        logging.info(f"{exposure=}")
        results = set(
            butler.registry.queryDatasets(
                datasetType=...,
                collections=collections,
                where=f"exposure={exposure} and instrument='LATISS'",
            )
        )

        # should just be "num_expected")
        self.assertEqual(len(results), num_expected)

    def strip_prefix(self, name, prefix):
        """strip prefix from name

        Parameters
        ----------
        name : `str`
           path of a file
        prefix : `str`
           prefix to strip

        Returns
        -------
        ret : `str`
            remainder of string
        """
        p = PurePath(name)
        ret = str(p.relative_to(prefix))
        return ret

class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
