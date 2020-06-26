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
import unittest
from shutil import copyfile
import yaml
from lsst.ctrl.oods.directoryScanner import DirectoryScanner
from lsst.ctrl.oods.fileIngester import FileIngester
import lsst.utils.tests
import asynctest


class Gen3ComCamIngesterTestCase(asynctest.TestCase):
    """Test Scanning directory"""

    def createConfig(self, config_name, fits_name):
        package = lsst.utils.getPackageDir("ctrl_oods")
        configFile = os.path.join(package, "tests", "etc", config_name)

        with open(configFile, "r") as f:
            config = yaml.safe_load(f)

        badDir = tempfile.mkdtemp()
        config["ingester"]["badFileDirectory"] = badDir

        dataDir = tempfile.mkdtemp()
        config["ingester"]["directories"] = [dataDir]

        repoDir = tempfile.mkdtemp()
        config["ingester"]["butler"]["repoDirectory"] = repoDir

        fitsFile = os.path.join(package, "tests", "data", fits_name)

        destFile = os.path.join(dataDir, fits_name)
        copyfile(fitsFile, destFile)

        return config, destFile, badDir

    async def testAuxTelIngest(self):
        fits_name = "2020032700020-det000.fits.fz"
        config, destFile, badDir = self.createConfig("ingest_auxtel_gen3.yaml", fits_name)

        scanner = DirectoryScanner(config["ingester"])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(config["ingester"])

        msg = {}
        msg['CAMERA'] = "LATISS"
        msg['OBSID'] = "AT_C_20180920_000028"
        msg['FILENAME'] = destFile
        msg['ARCHIVER'] = "AT"
        await ingester.ingest_file(msg)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        bad_path = os.path.join(badDir, fits_name)
        self.assertFalse(os.path.exists(bad_path))

    async def testComCamIngest(self):
        fits_name = "3019053000001-R22-S00-det000.fits.fz"
        config, destFile, badDir = self.createConfig("ingest_comcam_gen3.yaml", fits_name)

        scanner = DirectoryScanner(config["ingester"])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(config["ingester"])

        msg = {}
        msg['CAMERA'] = "COMCAM"
        msg['OBSID'] = "CC_C_20190530_000001"
        msg['FILENAME'] = destFile
        msg['ARCHIVER'] = "CC"
        await ingester.ingest_file(msg)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        bad_path = os.path.join(badDir, fits_name)
        self.assertFalse(os.path.exists(bad_path))

    async def testBadIngest(self):
        fits_name = "bad.fits.fz"
        config, destFile, badDir = self.createConfig("ingest_comcam_gen3.yaml", fits_name)

        scanner = DirectoryScanner(config["ingester"])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(config["ingester"])

        msg = {}
        msg['CAMERA'] = "COMCAM"
        msg['OBSID'] = "CC_C_20190530_000001"
        msg['FILENAME'] = destFile
        msg['ARCHIVER'] = "CC"
        await ingester.ingest_file(msg)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        bad_path = os.path.join(badDir, fits_name)
        self.assertTrue(os.path.exists(bad_path))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
