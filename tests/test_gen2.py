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


class Gen2TestCase(asynctest.TestCase):
    """Test Scanning directory"""

    def createConfig(self, config_name, fits_name, mapper):
        testdir = os.path.abspath(os.path.dirname(__file__))
        testFile = os.path.join(testdir, "etc", config_name)

        fitsFile = os.path.join(testdir, "data", fits_name)

        with open(testFile, "r") as f:
            config = yaml.safe_load(f)

        ingesterConfig = config["ingester"]
        dataDir = tempfile.mkdtemp()
        ingesterConfig["forwarderStagingDirectory"] = dataDir

        badDir = tempfile.mkdtemp()
        butlerConfig = config["ingester"]["butlers"][0]["butler"]
        butlerConfig["badFileDirectory"] = badDir

        butlerStageDir = tempfile.mkdtemp()
        butlerConfig["stagingDirectory"] = butlerStageDir

        repoDir = tempfile.mkdtemp()
        butlerConfig["repoDirectory"] = repoDir

        destFile = os.path.join(dataDir, fits_name)
        copyfile(fitsFile, destFile)

        mapperFileName = os.path.join(repoDir, "_mapper")
        with open(mapperFileName, 'w') as mapper_file:
            mapper_file.write(mapper)

        return config, destFile, repoDir, badDir

    async def testATIngest(self):
        fits_name = "2020032700020-det000.fits.fz"
        config, destFile, repoDir, badDir = self.createConfig("ingest_gen2.yaml",
                                                              fits_name,
                                                              "lsst.obs.lsst.latiss.LatissMapper")

        ingesterConfig = config["ingester"]
        forwarder_staging_dir = ingesterConfig["forwarderStagingDirectory"]
        scanner = DirectoryScanner([forwarder_staging_dir])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(ingesterConfig)

        msg = {}
        msg['CAMERA'] = "LATISS"
        msg['OBSID'] = "AT_C_20180920_000028"
        msg['FILENAME'] = destFile
        msg['ARCHIVER'] = "AT"
        await ingester.ingest(msg)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        bad_path = os.path.join(badDir, fits_name)
        self.assertFalse(os.path.exists(bad_path))

    async def testCCIngest(self):
        fits_name = "3019053000001-R22-S00-det000.fits.fz"
        config, destFile, repoDir, badDir = self.createConfig("ingest_gen2.yaml",
                                                              fits_name,
                                                              "lsst.obs.lsst.comCam.LsstComCamMapper")

        ingesterConfig = config["ingester"]
        forwarder_staging_dir = ingesterConfig["forwarderStagingDirectory"]
        scanner = DirectoryScanner([forwarder_staging_dir])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(ingesterConfig)

        msg = {}
        msg['CAMERA'] = "COMCAM"
        msg['OBSID'] = "CC_C_20190530_000001"
        msg['FILENAME'] = destFile
        msg['ARCHIVER'] = "CC"
        await ingester.ingest(msg)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        bad_path = os.path.join(badDir, fits_name)
        self.assertFalse(os.path.exists(bad_path))

    async def testBadIngest(self):
        fits_name = "bad.fits.fz"
        config, destFile, repoDir, badDir = self.createConfig("ingest_gen2.yaml",
                                                              fits_name,
                                                              "lsst.obs.lsst.comCam.LsstComCamMapper")

        ingesterConfig = config["ingester"]
        forwarder_staging_dir = ingesterConfig["forwarderStagingDirectory"]
        scanner = DirectoryScanner([forwarder_staging_dir])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(ingesterConfig)

        msg = {}
        msg['CAMERA'] = "COMCAM"
        msg['OBSID'] = "CC_C_20190530_000001"
        msg['FILENAME'] = destFile
        msg['ARCHIVER'] = "CC"
        await ingester.ingest(msg)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        bad_path = os.path.join(badDir, fits_name)
        self.assertTrue(os.path.exists(bad_path))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
