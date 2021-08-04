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
import os
import tempfile
from pathlib import PurePath
from shutil import copyfile
import yaml
from lsst.ctrl.oods.directoryScanner import DirectoryScanner
from lsst.ctrl.oods.fileIngester import FileIngester
import lsst.utils.tests
import asynctest
import utils


class Gen2TestCase(asynctest.TestCase):
    """Test Scanning directory"""

    def createConfig(self, config_name, fits_name, mapper):
        testdir = os.path.abspath(os.path.dirname(__file__))
        testFile = os.path.join(testdir, "etc", config_name)

        fitsFile = os.path.join(testdir, "data", fits_name)

        with open(testFile, "r") as f:
            config = yaml.safe_load(f)

        ingesterConfig = config["ingester"]
        self.forwarderStagingDirectory = tempfile.mkdtemp()
        ingesterConfig["forwarderStagingDirectory"] = self.forwarderStagingDirectory

        butlerConfig = config["ingester"]["butlers"][0]["butler"]

        self.badDir = tempfile.mkdtemp()
        butlerConfig["badFileDirectory"] = self.badDir

        self.stagingRootDir = tempfile.mkdtemp()
        butlerConfig["stagingDirectory"] = self.stagingRootDir

        self.repoDir = tempfile.mkdtemp()
        butlerConfig["repoDirectory"] = self.repoDir

        self.subDir = tempfile.mkdtemp(dir=self.forwarderStagingDirectory)
        self.destFile = os.path.join(self.subDir, fits_name)
        copyfile(fitsFile, self.destFile)

        mapperFileName = os.path.join(self.repoDir, "_mapper")
        with open(mapperFileName, 'w') as mapper_file:
            mapper_file.write(mapper)

        return config

    def tearDown(self):
        utils.removeEntries(self.forwarderStagingDirectory)
        utils.removeEntries(self.badDir)
        utils.removeEntries(self.stagingRootDir)
        utils.removeEntries(self.repoDir)
        utils.removeEntries(self.subDir)

    def strip_prefix(self, name, prefix):
        p = PurePath(name)
        ret = str(p.relative_to(prefix))
        return ret

    async def _testATIngest(self):
        fits_name = "2020032700020-det000.fits.fz"
        config = self.createConfig("ingest_gen2.yaml", fits_name, "lsst.obs.lsst.latiss.LatissMapper")

        ingesterConfig = config["ingester"]
        forwarder_staging_dir = ingesterConfig["forwarderStagingDirectory"]
        scanner = DirectoryScanner([forwarder_staging_dir])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(ingesterConfig)

        await ingester.ingest([self.destFile])

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        name = self.strip_prefix(self.destFile, forwarder_staging_dir)
        bad_path = os.path.join(self.badDir, name)
        self.assertFalse(os.path.exists(bad_path))

    async def _testCCIngest(self):
        fits_name = "3019053000001-R22-S00-det000.fits.fz"
        config = self.createConfig("ingest_gen2.yaml", fits_name, "lsst.obs.lsst.comCam.LsstComCamMapper")

        ingesterConfig = config["ingester"]
        forwarder_staging_dir = ingesterConfig["forwarderStagingDirectory"]
        scanner = DirectoryScanner([forwarder_staging_dir])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(ingesterConfig)

        await ingester.ingest([self.destFile])

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        name = self.strip_prefix(self.destFile, forwarder_staging_dir)
        bad_path = os.path.join(self.badDir, name)
        self.assertFalse(os.path.exists(bad_path))

    async def testBadIngest(self):
        fits_name = "bad.fits.fz"
        config = self.createConfig("ingest_gen2.yaml", fits_name, "lsst.obs.lsst.comCam.LsstComCamMapper")

        ingesterConfig = config["ingester"]
        forwarder_staging_dir = ingesterConfig["forwarderStagingDirectory"]
        scanner = DirectoryScanner([forwarder_staging_dir])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(ingesterConfig)

        await ingester.ingest([self.destFile])

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        name = self.strip_prefix(self.destFile, forwarder_staging_dir)
        bad_path = os.path.join(self.badDir, name)
        self.assertTrue(os.path.exists(bad_path))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
