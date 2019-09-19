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
import logging


class Gen2IngesterTestCase(lsst.utils.tests.TestCase):
    """Test Scanning directory"""

    def setUp(self):
        self.logger = logging.getLogger("gen2IngesterTestCase")

        package = lsst.utils.getPackageDir("ctrl_oods")
        testFile = os.path.join(package, "tests", "etc", "ingest.yaml")

        fitsFileName = "ats_exp_0_AT_C_20180920_000028.fits.fz"
        fitsFile = os.path.join(package, "tests", "etc", fitsFileName)

        mapperFileName = "_mapper"
        mapperPath = os.path.join(package, "tests", "etc", "_mapper")

        self.config = None
        with open(testFile, "r") as f:
            self.config = yaml.safe_load(f)

        dataDir = tempfile.mkdtemp()
        self.config["ingester"]["directories"] = [dataDir]

        repoDir = tempfile.mkdtemp()
        self.config["ingester"]["butler"]["repoDirectory"] = repoDir

        destFile = os.path.join(dataDir, fitsFileName)
        copyfile(fitsFile, destFile)

        destFile = os.path.join(repoDir, mapperFileName)
        copyfile(mapperPath, destFile)

    def testIngest(self):
        scanner = DirectoryScanner(self.config["ingester"])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(self.logger, self.config["ingester"])
        ingester.run_task()

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        ingester.run_task()

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

    def testBatchSize(self):
        scanner = DirectoryScanner(self.config["ingester"])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        self.config["ingester"]["batchSize"] = -1

        ingester = FileIngester(self.logger, self.config["ingester"])
        ingester.run_task()

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
