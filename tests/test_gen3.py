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

        ingester_config = config.file_ingester
        self.image_staging_dir = tempfile.mkdtemp()
        ingester_config.image_staging_directory = self.image_staging_dir

        self.bad_dir = tempfile.mkdtemp()
        butler_config = ingester_config.butler
        ingester_config.bad_file_directory = self.bad_dir
        self.staging_directory = tempfile.mkdtemp()
        ingester_config.staging_directory = self.staging_directory

        self.repoDir = tempfile.mkdtemp()
        Butler.makeRepo(self.repoDir)
        butler_config.repo_directory = self.repoDir

        # copy the FITS file to it's test location

        self.sub_dir = tempfile.mkdtemp(dir=self.image_staging_dir)

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
        self.dest_file = None
        self.image_staging_dir = None
        self.bad_dir = None
        self.staging_directory = None
        self.repoDir = None
        self.sub_dir = None

    def tearDown(self):
        if self.dest_file:
            shutil.rmtree(self.dest_file, ignore_errors=True)
        if self.image_staging_dir:
            shutil.rmtree(self.image_staging_dir, ignore_errors=True)
        if self.bad_dir:
            shutil.rmtree(self.bad_dir, ignore_errors=True)
        if self.staging_directory:
            shutil.rmtree(self.staging_directory, ignore_errors=True)
        if self.repoDir:
            shutil.rmtree(self.repoDir, ignore_errors=True)
        if self.sub_dir:
            shutil.rmtree(self.sub_dir, ignore_errors=True)

    async def testAuxTelIngest(self):
        """test ingesting an auxtel file"""
        fits_name = "2020032700020-det000.fits.fz"
        config = self.createConfig("ingest_auxtel_gen3.yaml")
        self.dest_file = self.placeFitsFile(self.sub_dir, fits_name)

        # setup directory to scan for files in the image staging directory
        # and ensure one file is there
        ingester_config = config.file_ingester
        image_staging_dir = ingester_config.image_staging_directory
        scanner = DirectoryScanner([image_staging_dir])
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        # create a FileIngester
        ingester = FileIngester(config)

        staged_files = ingester.stageFiles([self.dest_file])
        await ingester.ingest(staged_files)

        # check to make sure the file was moved from the staging directory
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        # check to be sure the file didn't land in the "bad file" directory
        bad_path = os.path.join(self.bad_dir, fits_name)
        self.assertFalse(os.path.exists(bad_path))

    async def testComCamIngest(self):
        fits_name = "3019053000001-R22-S00-det000.fits.fz"
        config = self.createConfig("ingest_comcam_gen3.yaml")
        self.dest_file = self.placeFitsFile(self.sub_dir, fits_name)

        # setup directory to scan for files in the image staging directory
        # and ensure one file is there
        ingester_config = config.file_ingester
        image_staging_dir = ingester_config.image_staging_directory
        scanner = DirectoryScanner([image_staging_dir])
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        # create the file ingester, get all tasks associated with it, and
        # create the tasks
        ingester = FileIngester(config)
        # check to see that the file is there before ingestion
        self.assertTrue(os.path.exists(self.dest_file))

        staged_files = ingester.stageFiles([self.dest_file])
        await ingester.ingest(staged_files)

        # make sure staging area is now empty
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        # Check to see that the file was ingested.
        # Recall that files start in the image staging area, and are
        # moved to the OODS staging area before ingestion. On "direct"
        # ingestion, this is where the file is located.  This is a check
        # to be sure that happened.
        name = Utils.strip_prefix(self.dest_file, self.image_staging_dir)
        file_to_ingest = os.path.join(self.staging_directory, name)
        self.assertTrue(os.path.exists(file_to_ingest))

        # this file should now not exist
        self.assertFalse(os.path.exists(self.dest_file))

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
        bad_path = os.path.join(self.bad_dir, fits_name)
        self.assertFalse(os.path.exists(bad_path))

    async def testCleanTask(self):
        fits_name = "2020032700020-det000.fits.fz"
        fits_name2 = "AT_O_20221122_000951_R00_S00.fits.fz"
        config = self.createConfig("ingest_auxtel_clean.yaml")
        self.dest_file = self.placeFitsFile(self.sub_dir, fits_name)
        self.dest_file2 = self.placeFitsFile(self.sub_dir, fits_name2)

        # setup directory to scan for files in the image staging directory
        # and ensure one file is there
        ingester_config = config.file_ingester
        image_staging_dir = ingester_config.image_staging_directory
        scanner = DirectoryScanner([image_staging_dir])
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 2)

        # create the file ingester, get all tasks associated with it, and
        # create the tasks
        ingester = FileIngester(config)

        # check to see that the file is there before ingestion
        self.assertTrue(os.path.exists(self.dest_file))
        self.assertTrue(os.path.exists(self.dest_file2))

        staged_files = ingester.stageFiles([self.dest_file, self.dest_file2])
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
        self.dest_file = self.placeFitsFile(self.sub_dir, fits_name)

        # setup directory to scan for files in the image staging directory
        ingester_config = config.file_ingester
        image_staging_dir = ingester_config.image_staging_directory
        scanner = DirectoryScanner([image_staging_dir])
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(config)

        staged_files = ingester.stageFiles([self.dest_file])
        print(f"{staged_files=}")
        print(f"{ingester=}")

        await ingester.ingest(staged_files)
        await asyncio.sleep(0)  # appease coverage
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        name = Utils.strip_prefix(self.dest_file, image_staging_dir)
        bad_path = os.path.join(self.bad_dir, name)
        self.assertTrue(os.path.exists(bad_path))

    async def testRepoExists(self):
        fits_name = "bad.fits.fz"
        config = self.createConfig("ingest_comcam_gen3.yaml")
        self.dest_file = self.placeFitsFile(self.sub_dir, fits_name)

        FileIngester(config)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
