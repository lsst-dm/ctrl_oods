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

    async def testAuxTelIngest(self):
        package = lsst.utils.getPackageDir("ctrl_oods")
        testFile = os.path.join(package, "tests", "etc", "ingest_auxtel_gen3.yaml")

        fitsFileName = "ats_exp_0_AT_C_20180920_000028.fits.fz"
        fitsFile = os.path.join(package, "tests", "data", fitsFileName)

        self.config = None
        with open(testFile, "r") as f:
            self.config = yaml.safe_load(f)

        dataDir = tempfile.mkdtemp()
        self.config["ingester"]["directories"] = [dataDir]

        repoDir = tempfile.mkdtemp()
        self.config["ingester"]["butler"]["repoDirectory"] = repoDir

        self.destFile1 = os.path.join(dataDir, fitsFileName)
        copyfile(fitsFile, self.destFile1)

        scanner = DirectoryScanner(self.config["ingester"])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(self.config["ingester"])

        msg = {}
        msg['CAMERA'] = "LATISS"
        msg['OBSID'] = "AT_C_20180920_000028"
        msg['FILENAME'] = self.destFile1
        msg['ARCHIVER'] = "AT"
        await ingester.ingest_file(msg)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

    async def testComCamIngest(self):
        package = lsst.utils.getPackageDir("ctrl_oods")
        testFile = os.path.join(package, "tests", "etc", "ingest_comcam_gen3.yaml")

        # fitsFileName = "2020061900002-R22-S22-det008.fits"
        # fitsFile = os.path.join(package, "tests", "etc", fitsFileName)
        fitsFileName = "3019053000001-R22-S00-det000.fits.fz"
        fitsFile = os.path.join(package, "tests", "data", fitsFileName)

        self.config = None
        with open(testFile, "r") as f:
            self.config = yaml.safe_load(f)

        dataDir = tempfile.mkdtemp()
        self.config["ingester"]["directories"] = [dataDir]

        repoDir = tempfile.mkdtemp()
        self.config["ingester"]["butler"]["repoDirectory"] = repoDir

        self.destFile1 = os.path.join(dataDir, fitsFileName)
        copyfile(fitsFile, self.destFile1)

        scanner = DirectoryScanner(self.config["ingester"])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(self.config["ingester"])

        msg = {}
        msg['CAMERA'] = "COMCAM"
        msg['OBSID'] = "CC_O_20200618_000001"
        msg['FILENAME'] = self.destFile1
        msg['ARCHIVER'] = "CC"
        await ingester.ingest_file(msg)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
