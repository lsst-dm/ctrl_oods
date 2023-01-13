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

import asynctest
import lsst.utils.tests
import yaml
from lsst.ctrl.oods.directoryScanner import DirectoryScanner
from lsst.ctrl.oods.fileIngester import FileIngester
from lsst.ctrl.oods.utils import Utils


class BadTestCase(asynctest.TestCase):
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
        self.fits_name = fits_name

        # path to the FITS file to ingest

        self.fitsFile = os.path.join(testdir, "data", fits_name)

        # load the YAML configuration

        with open(configFile, "r") as f:
            config = yaml.safe_load(f)

        # extract parts of the ingester configuration
        # and alter the image staging directory to point
        # at the temporary directories created for this test

        ingesterConfig = config["ingester"]
        self.imageDir = tempfile.mkdtemp()
        ingesterConfig["imageStagingDirectory"] = self.imageDir

        self.badDir = tempfile.mkdtemp()
        butlerConfig = ingesterConfig["butlers"][0]["butler"]
        butlerConfig["badFileDirectory"] = self.badDir
        self.stagingDir = tempfile.mkdtemp()
        butlerConfig["stagingDirectory"] = self.stagingDir

        self.repoDir = tempfile.mkdtemp()
        butlerConfig["repoDirectory"] = self.repoDir

        # copy the FITS file to its test location

        self.subDir = tempfile.mkdtemp(dir=self.imageDir)
        self.destFile = os.path.join(self.subDir, fits_name)
        shutil.copyfile(self.fitsFile, self.destFile)

        return config

    def tearDown(self):
        """Remove directories created by createConfig"""
        shutil.rmtree(self.imageDir, ignore_errors=True)
        shutil.rmtree(self.badDir, ignore_errors=True)
        shutil.rmtree(self.stagingDir, ignore_errors=True)
        shutil.rmtree(self.repoDir, ignore_errors=True)
        shutil.rmtree(self.subDir, ignore_errors=True)

    async def testBadIngest(self):
        """test ingesting a bad file"""
        name = "AT_O_20221122_000951_R00_S00.fits.fz"
        config = self.createConfig("ingest_auxtel_gen3.yaml", name)

        # setup directory to scan for files in the image staging directory
        ingesterConfig = config["ingester"]
        image_staging_dir = ingesterConfig["imageStagingDirectory"]
        scanner = DirectoryScanner([image_staging_dir])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(config["ingester"])

        staged_files = ingester.stageFiles([self.destFile])
        await ingester.ingest(staged_files)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        # copy the same file to ingest again, which should trigger
        # an "on_ingest_failure" and move the file to self.badDir
        self.subDir = tempfile.mkdtemp(dir=self.imageDir)
        self.destFile = os.path.join(self.subDir, self.fits_name)
        shutil.copyfile(self.fitsFile, self.destFile)

        # now stage and try and ingest
        staged_files = ingester.stageFiles([self.destFile])
        await ingester.ingest(staged_files)

        name = Utils.strip_prefix(self.destFile, image_staging_dir)
        bad_path = os.path.join(self.badDir, name)
        self.assertTrue(os.path.exists(bad_path))

    async def interrupt_me(self):
        """Used to interrupt asyncio.gather() so that test can be halted"""
        await asyncio.sleep(10)
        raise RuntimeError("I'm interrupting")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
