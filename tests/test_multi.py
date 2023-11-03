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
import shutil
import tempfile
import unittest

import lsst.utils.tests
import yaml
from lsst.ctrl.oods.directoryScanner import DirectoryScanner
from lsst.ctrl.oods.fileIngester import FileIngester


class MultiComCamIngesterTestCase(unittest.IsolatedAsyncioTestCase):
    """Test multiple butler ingest"""

    def createConfig(self, config_name, fits_name):
        self.multi_dirs = []
        testdir = os.path.abspath(os.path.dirname(__file__))
        configFile = os.path.join(testdir, "etc", config_name)

        fitsFile = os.path.join(testdir, "data", fits_name)

        with open(configFile, "r") as f:
            config = yaml.safe_load(f)

        ingesterConfig = config["ingester"]
        self.dataDir = tempfile.mkdtemp()
        ingesterConfig["imageStagingDirectory"] = self.dataDir

        for x in ingesterConfig["butlers"]:
            butlerConfig = x["butler"]

            self.badDir = tempfile.mkdtemp()
            butlerConfig["badFileDirectory"] = self.badDir

            self.stagingRootDir = tempfile.mkdtemp()
            butlerConfig["stagingDirectory"] = self.stagingRootDir

            self.repoDir = tempfile.mkdtemp()

            butlerConfig["repoDirectory"] = self.repoDir

            self.multi_dirs.append(self.badDir)
            self.multi_dirs.append(self.stagingRootDir)
            self.multi_dirs.append(self.repoDir)

        self.subDir = tempfile.mkdtemp(dir=self.dataDir)
        destFile = os.path.join(self.subDir, fits_name)

        shutil.copyfile(fitsFile, destFile)

        return config, destFile

    def tearDown(self):
        shutil.rmtree(self.dataDir, ignore_errors=True)
        shutil.rmtree(self.subDir, ignore_errors=True)
        for md in self.multi_dirs:
            shutil.rmtree(md, ignore_errors=True)

    async def testComCamIngest(self):
        """Test that a ComCam file can be ingested into multiple butlers"""
        fits_name = "3019053000001-R22-S00-det000.fits.fz"
        config, destFile = self.createConfig("cc_oods_multi.yaml", fits_name)

        ingesterConfig = config["ingester"]
        image_staging_dir = ingesterConfig["imageStagingDirectory"]
        scanner = DirectoryScanner([image_staging_dir])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(ingesterConfig)

        staged_files = ingester.stageFiles([destFile])
        await ingester.ingest(staged_files)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
