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


class AutoIngestTestCase(HeartbeatBase):
    """Test Gen3 Butler Ingest"""

    def createConfig(self, config_name, fits_name):
        """create a standard configuration file, using temporary directories

        Parameters
        ----------
        config_name: `str`
            name of the OODS configuration file
        fits_name: `str`
            name of the test FITS file

        Returns
        -------
        config: `dict`
            An OODS configuration to use for testing
        """

        # create a path to the configuration file

        testdir = os.path.abspath(os.path.dirname(__file__))
        config_file = os.path.join(testdir, "etc", config_name)

        # path to the FITS file to ingest

        fitsFile = os.path.join(testdir, "data", fits_name)

        # load the YAML configuration

        config = OODSConfig.load(config_file)

        # extract parts of the ingester configuration
        # and alter the image staging directory to point
        # at the temporary directories created for his test

        ingester_config = config.file_ingester
        self.image_dir = tempfile.mkdtemp()
        ingester_config.image_staging_directory = self.image_dir

        self.bad_dir = tempfile.mkdtemp()
        butler_config = ingester_config.butler
        ingester_config.bad_file_directory = self.bad_dir
        self.staging_dir = tempfile.mkdtemp()
        ingester_config.staging_directory = self.staging_dir

        self.repo_dir = tempfile.mkdtemp()
        Butler.makeRepo(self.repo_dir)

        butler_config.repo_directory = self.repo_dir

        # copy the FITS file to it's test location

        self.sub_dir = tempfile.mkdtemp(dir=self.image_dir)
        self.dest_file = os.path.join(self.sub_dir, fits_name)
        shutil.copyfile(fitsFile, self.dest_file)

        return config

    def tearDown(self):
        """Remove directories created by createConfig"""
        shutil.rmtree(self.image_dir, ignore_errors=True)
        shutil.rmtree(self.bad_dir, ignore_errors=True)
        shutil.rmtree(self.staging_dir, ignore_errors=True)
        shutil.rmtree(self.repo_dir, ignore_errors=True)
        shutil.rmtree(self.sub_dir, ignore_errors=True)

    async def testAuxTelIngest(self):
        """test ingesting an auxtel file"""

        fits_name = "2020032700020-det000.fits.fz"
        config = self.createConfig("ingest_auxtel_gen3.yaml", fits_name)

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

        # check to make sure file was moved from image staging directory
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        # check to be sure the file didn't land in the "bad file" directory
        bad_path = os.path.join(self.bad_dir, fits_name)
        self.assertFalse(os.path.exists(bad_path))

    async def testComCamIngest(self):
        """test ingesting an ComCam file"""
        fits_name = "3019053000001-R22-S00-det000.fits.fz"
        config = self.createConfig("ingest_comcam_gen3.yaml", fits_name)

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

        # make sure image staging area is now empty
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        # Check to see that the file was ingested.
        # Recall that files start in the image staging area, and are
        # moved to the butler staging area before ingestion. On "direct"
        # ingestion, this is where the file is located.  This is a check
        # to be sure that happened.
        name = Utils.strip_prefix(self.dest_file, self.image_dir)
        file_to_ingest = os.path.join(self.staging_dir, name)
        self.assertTrue(os.path.exists(file_to_ingest))

        # this file should now not exist
        self.assertFalse(os.path.exists(self.dest_file))

        await asyncio.sleep(1)
        clean_methods = ingester.getButlerCleanMethods()
        for clean in clean_methods:
            await asyncio.create_task(clean())

        # Ingestion and clean up tasks have been run.
        # When "cleaned up", the file that was originally there
        # is now gone.  Check for that.
        self.assertFalse(os.path.exists(file_to_ingest))

        # check to be sure that the file wasn't "bad" (and
        # therefore, not ingested)
        bad_path = os.path.join(self.bad_dir, fits_name)
        self.assertFalse(os.path.exists(bad_path))

    async def testBadIngest(self):
        """test ingesting a bad file"""
        fits_name = "bad.fits.fz"
        config = self.createConfig("ingest_comcam_gen3.yaml", fits_name)

        # setup directory to scan for files in the image staging directory
        ingester_config = config.file_ingester
        image_staging_dir = ingester_config.image_staging_directory
        scanner = DirectoryScanner([image_staging_dir])
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(config)

        staged_files = ingester.stageFiles([self.dest_file])
        await ingester.ingest(staged_files)
        await asyncio.sleep(0)  # appease coverage
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        name = Utils.strip_prefix(self.dest_file, image_staging_dir)
        bad_path = os.path.join(self.bad_dir, name)
        self.assertTrue(os.path.exists(bad_path))

    async def testRepoExists(self):
        """test that a repository exists"""
        fits_name = "bad.fits.fz"
        config = self.createConfig("ingest_comcam_gen3.yaml", fits_name)

        FileIngester(config)
        # tests the path that the previously created repo (above) exists
        FileIngester(config)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
