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
import yaml
from heartbeat_base import HeartbeatBase
from lsst.ctrl.oods.directoryScanner import DirectoryScanner
from lsst.ctrl.oods.fileIngester import FileIngester
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
        configFile = os.path.join(testdir, "etc", config_name)

        # path to the FITS file to ingest

        fitsFile = os.path.join(testdir, "data", fits_name)

        # load the YAML configuration

        with open(configFile, "r") as f:
            config = yaml.safe_load(f)

        # extract parts of the ingester configuration
        # and alter the image staging directory to point
        # at the temporary directories created for his test

        ingesterConfig = config["ingester"]
        self.imageDir = tempfile.mkdtemp()
        ingesterConfig["imageStagingDirectory"] = self.imageDir

        self.badDir = tempfile.mkdtemp()
        butlerConfig = ingesterConfig["butlers"][0]["butler"]
        butlerConfig["badFileDirectory"] = self.badDir
        self.stagingDir = tempfile.mkdtemp()
        butlerConfig["stagingDirectory"] = self.stagingDir

        self.repoDir = tempfile.mkdtemp()
        Butler.makeRepo(self.repoDir)

        butlerConfig["repoDirectory"] = self.repoDir

        # copy the FITS file to it's test location

        self.subDir = tempfile.mkdtemp(dir=self.imageDir)
        self.destFile = os.path.join(self.subDir, fits_name)
        shutil.copyfile(fitsFile, self.destFile)

        return config

    def tearDown(self):
        """Remove directories created by createConfig"""
        shutil.rmtree(self.imageDir, ignore_errors=True)
        shutil.rmtree(self.badDir, ignore_errors=True)
        shutil.rmtree(self.stagingDir, ignore_errors=True)
        shutil.rmtree(self.repoDir, ignore_errors=True)
        shutil.rmtree(self.subDir, ignore_errors=True)

    async def testAuxTelIngest(self):
        """test ingesting an auxtel file"""

        fits_name = "2020032700020-det000.fits.fz"
        config = self.createConfig("ingest_auxtel_gen3.yaml", fits_name)

        # setup directory to scan for files in the image staging directory
        # and ensure one file is there
        ingesterConfig = config["ingester"]
        image_staging_dir = ingesterConfig["imageStagingDirectory"]
        scanner = DirectoryScanner([image_staging_dir])
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        # create a FileIngester
        ingester = FileIngester(config)

        staged_files = ingester.stageFiles([self.destFile])
        await ingester.ingest(staged_files)

        # check to make sure file was moved from image staging directory
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        # check to be sure the file didn't land in the "bad file" directory
        bad_path = os.path.join(self.badDir, fits_name)
        self.assertFalse(os.path.exists(bad_path))

    async def testComCamIngest(self):
        """test ingesting an ComCam file"""
        fits_name = "3019053000001-R22-S00-det000.fits.fz"
        config = self.createConfig("ingest_comcam_gen3.yaml", fits_name)

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

        # check to see that the file is there before ingestion
        self.assertTrue(os.path.exists(self.destFile))

        staged_files = ingester.stageFiles([self.destFile])
        await ingester.ingest(staged_files)

        # make sure image staging area is now empty
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        # Check to see that the file was ingested.
        # Recall that files start in the image staging area, and are
        # moved to the butler staging area before ingestion. On "direct"
        # ingestion, this is where the file is located.  This is a check
        # to be sure that happened.
        name = Utils.strip_prefix(self.destFile, self.imageDir)
        file_to_ingest = os.path.join(self.stagingDir, name)
        self.assertTrue(os.path.exists(file_to_ingest))

        # this file should now not exist
        self.assertFalse(os.path.exists(self.destFile))

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
        bad_path = os.path.join(self.badDir, fits_name)
        self.assertFalse(os.path.exists(bad_path))

    async def testBadIngest(self):
        """test ingesting a bad file"""
        fits_name = "bad.fits.fz"
        config = self.createConfig("ingest_comcam_gen3.yaml", fits_name)

        # setup directory to scan for files in the image staging directory
        ingesterConfig = config["ingester"]
        image_staging_dir = ingesterConfig["imageStagingDirectory"]
        scanner = DirectoryScanner([image_staging_dir])
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(config)

        staged_files = ingester.stageFiles([self.destFile])
        await ingester.ingest(staged_files)
        await asyncio.sleep(0)  # appease coverage
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        name = Utils.strip_prefix(self.destFile, image_staging_dir)
        bad_path = os.path.join(self.badDir, name)
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
