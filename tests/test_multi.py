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
from shutil import copyfile
import yaml
from lsst.ctrl.oods.directoryScanner import DirectoryScanner
from lsst.ctrl.oods.fileIngester import FileIngester
import lsst.utils.tests
import asynctest
import utils


class MultiComCamIngesterTestCase(asynctest.TestCase):
    """Test Scanning directory"""

    def createConfig(self, config_name, fits_name):
        self.multi_dirs = []
        testdir = os.path.abspath(os.path.dirname(__file__))
        configFile = os.path.join(testdir, "etc", config_name)

        fitsFile = os.path.join(testdir, "data", fits_name)

        with open(configFile, "r") as f:
            config = yaml.safe_load(f)

        ingesterConfig = config["ingester"]
        self.dataDir = tempfile.mkdtemp()
        ingesterConfig["forwarderStagingDirectory"] = self.dataDir

        for x in ingesterConfig["butlers"]:
            butlerConfig = x["butler"]

            self.badDir = tempfile.mkdtemp()
            butlerConfig["badFileDirectory"] = self.badDir

            self.stagingRootDir = tempfile.mkdtemp()
            butlerConfig["stagingDirectory"] = self.stagingRootDir

            self.repoDir = tempfile.mkdtemp()

            butlerConfig["repoDirectory"] = self.repoDir
            if butlerConfig["class"]["import"] == "lsst.ctrl.oods.gen2ButlerBroker":
                mapperFileName = os.path.join(self.repoDir, "_mapper")
                with open(mapperFileName, 'w') as mapper_file:
                    mapper_file.write("lsst.obs.lsst.comCam.LsstComCamMapper")
            self.multi_dirs.append(self.badDir)
            self.multi_dirs.append(self.stagingRootDir)
            self.multi_dirs.append(self.repoDir)

        self.subDir = tempfile.mkdtemp(dir=self.dataDir)
        destFile = os.path.join(self.subDir, fits_name)

        copyfile(fitsFile, destFile)

        return config, destFile

    def tearDown(self):
        utils.removeEntries(self.dataDir)
        utils.removeEntries(self.subDir)
        for md in self.multi_dirs:
            utils.removeEntries(md)

    async def testComCamIngest(self):
        fits_name = "3019053000001-R22-S00-det000.fits.fz"
        config, destFile = self.createConfig("cc_oods_multi.yaml", fits_name)

        ingesterConfig = config["ingester"]
        forwarder_staging_dir = ingesterConfig["forwarderStagingDirectory"]
        scanner = DirectoryScanner([forwarder_staging_dir])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(ingesterConfig)

        await ingester.ingest(destFile)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
