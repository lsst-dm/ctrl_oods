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
import os
import shutil
import tempfile

import lsst.utils.tests
from heartbeat_base import HeartbeatBase
from lsst.ctrl.oods.directoryScanner import DirectoryScanner
from lsst.ctrl.oods.fileIngester import FileIngester
from lsst.ctrl.oods.oods_config import OODSConfig
from lsst.ctrl.oods.utils import Utils
from lsst.daf.butler import Butler


class Gen3ComCamIngesterTestCase(HeartbeatBase):
    """Test Gen3 Butler Ingest"""

    def createConfig(self, config_name):
        """create a standard configuration file, using temporary directories

        Parameters
        ----------
        config_name: `str`
            name of the OODS configuration file

        Returns
        -------
        config: `dict`
            An OODS configuration to use for testing
        """

        # create a path to the configuration file

        testdir = os.path.abspath(os.path.dirname(__file__))
        config_file = os.path.join(testdir, "etc", config_name)

        # load the YAML configuration

        config = OODSConfig.load(config_file)

        # extract parts of the ingester configuration
        # and alter the image staging directory to point
        # at the temporary directories created for his test

        ingesterConfig = config.file_ingester
        self.imageStagingDir = tempfile.mkdtemp()
        ingesterConfig.image_staging_directory = self.imageStagingDir

        self.badDir = tempfile.mkdtemp()
        butlerConfig = ingesterConfig.butler
        ingesterConfig.bad_file_directory = self.badDir
        self.stagingDirectory = tempfile.mkdtemp()
        ingesterConfig.staging_directory = self.stagingDirectory

        self.repoDir = tempfile.mkdtemp()
        Butler.makeRepo(self.repoDir)
        butlerConfig.repo_directory = self.repoDir

        # copy the FITS file to it's test location

        self.subDir = tempfile.mkdtemp(dir=self.imageStagingDir)

        return config

    def placeFitsFile(self, subDir, fits_name):
        """Place a fits file in a subdirectory

        Parameters
        ----------
        fits_name: `str`
            name of the test FITS file

        """
        testdir = os.path.abspath(os.path.dirname(__file__))
        fitsFile = os.path.join(testdir, "data", fits_name)
        destFile = os.path.join(subDir, fits_name)
        shutil.copyfile(fitsFile, destFile)
        return destFile

    def setUp(self):
        self.destFile = None
        self.imageStagingDir = None
        self.badDir = None
        self.stagingDirectory = None
        self.repoDir = None
        self.subDir = None

    def tearDown(self):
        if self.destFile:
            shutil.rmtree(self.destFile, ignore_errors=True)
        if self.imageStagingDir:
            shutil.rmtree(self.imageStagingDir, ignore_errors=True)
        if self.badDir:
            shutil.rmtree(self.badDir, ignore_errors=True)
        if self.stagingDirectory:
            shutil.rmtree(self.stagingDirectory, ignore_errors=True)
        if self.repoDir:
            shutil.rmtree(self.repoDir, ignore_errors=True)
        if self.subDir:
            shutil.rmtree(self.subDir, ignore_errors=True)

    async def testAuxTelIngest(self):
        """test ingesting an auxtel file"""
        fits_name = "2020032700020-det000.fits.fz"
        config = self.createConfig("ingest_auxtel_gen3.yaml")
        self.destFile = self.placeFitsFile(self.subDir, fits_name)

        # setup directory to scan for files in the image staging directory
        # and ensure one file is there
        ingesterConfig = config.file_ingester
        image_staging_dir = ingesterConfig.image_staging_directory
        scanner = DirectoryScanner([image_staging_dir])
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        # create a FileIngester
        ingester = FileIngester(config)

        staged_files = ingester.stageFiles([self.destFile])
        await ingester.ingest(staged_files)

        # check to make sure the file was moved from the staging directory
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        # check to be sure the file didn't land in the "bad file" directory
        bad_path = os.path.join(self.badDir, fits_name)
        self.assertFalse(os.path.exists(bad_path))

    async def testComCamIngest(self):
        fits_name = "3019053000001-R22-S00-det000.fits.fz"
        config = self.createConfig("ingest_comcam_gen3.yaml")
        self.destFile = self.placeFitsFile(self.subDir, fits_name)

        # setup directory to scan for files in the image staging directory
        # and ensure one file is there
        ingesterConfig = config.file_ingester
        image_staging_dir = ingesterConfig.image_staging_directory
        scanner = DirectoryScanner([image_staging_dir])
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        # create the file ingester, get all tasks associated with it, and
        # create the tasks
        ingester = FileIngester(config)
        # check to see that the file is there before ingestion
        self.assertTrue(os.path.exists(self.destFile))

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
        name = Utils.strip_prefix(self.destFile, self.imageStagingDir)
        file_to_ingest = os.path.join(self.stagingDirectory, name)
        self.assertTrue(os.path.exists(file_to_ingest))

        # this file should now not exist
        self.assertFalse(os.path.exists(self.destFile))

        await asyncio.sleep(1)
        await self.perform_clean(config)

        # Ingestion and clean up tasks were performed.
        # That "clean up" time is set in the config file
        # loaded for this FileIngester.
        # And, when "cleaned up", the file that was originally there
        # is now gone.  Check for that.
        self.assertFalse(os.path.exists(file_to_ingest))

        # check to be sure that the file wasn't "bad" (and
        # therefore, not ingested)
        bad_path = os.path.join(self.badDir, fits_name)
        self.assertFalse(os.path.exists(bad_path))

    async def testCleanTask(self):
        fits_name = "2020032700020-det000.fits.fz"
        fits_name2 = "AT_O_20221122_000951_R00_S00.fits.fz"
        config = self.createConfig("ingest_auxtel_clean.yaml")
        self.destFile = self.placeFitsFile(self.subDir, fits_name)
        self.destFile2 = self.placeFitsFile(self.subDir, fits_name2)

        # setup directory to scan for files in the image staging directory
        # and ensure one file is there
        ingesterConfig = config.file_ingester
        image_staging_dir = ingesterConfig.image_staging_directory
        scanner = DirectoryScanner([image_staging_dir])
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 2)

        # create the file ingester, get all tasks associated with it, and
        # create the tasks
        ingester = FileIngester(config)

        # check to see that the file is there before ingestion
        self.assertTrue(os.path.exists(self.destFile))
        self.assertTrue(os.path.exists(self.destFile2))

        staged_files = ingester.stageFiles([self.destFile, self.destFile2])
        await ingester.ingest(staged_files)

        # make sure staging area is now empty
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        keyList = list(staged_files.keys())
        self.assertEqual(len(keyList), 1)

        key = keyList[0]

        self.assertTrue(os.path.exists(staged_files[key][0]))
        self.assertTrue(os.path.exists(staged_files[key][1]))

        # remove these files;  the cleaner task should
        # be able to handle files that don't exist, and continue
        os.remove(staged_files[key][0])
        os.remove(staged_files[key][1])

        await asyncio.sleep(1)
        await self.perform_clean(config)

        # Ingestion and clean up tasks were performed.
        # That "clean up" time is set in the config file
        # loaded for this FileIngester.
        # And, when "cleaned up", the file that was originally there
        # is now gone.  Check for that.
        keyList = list(staged_files.keys())
        self.assertEqual(len(keyList), 1)

        key = keyList[0]

        self.assertFalse(os.path.exists(staged_files[key][0]))
        self.assertFalse(os.path.exists(staged_files[key][1]))

    async def testBadIngest(self):
        fits_name = "bad.fits.fz"
        config = self.createConfig("ingest_comcam_gen3.yaml")
        self.destFile = self.placeFitsFile(self.subDir, fits_name)

        # setup directory to scan for files in the image staging directory
        ingesterConfig = config.file_ingester
        image_staging_dir = ingesterConfig.image_staging_directory
        scanner = DirectoryScanner([image_staging_dir])
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(config)

        staged_files = ingester.stageFiles([self.destFile])
        print(f"{staged_files=}")
        print(f"{ingester=}")

        await ingester.ingest(staged_files)
        await asyncio.sleep(0)  # appease coverage
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        name = Utils.strip_prefix(self.destFile, image_staging_dir)
        bad_path = os.path.join(self.badDir, name)
        self.assertTrue(os.path.exists(bad_path))

    async def testRepoExists(self):
        fits_name = "bad.fits.fz"
        config = self.createConfig("ingest_comcam_gen3.yaml")
        self.destFile = self.placeFitsFile(self.subDir, fits_name)

        FileIngester(config)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
