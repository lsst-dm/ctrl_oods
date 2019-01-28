#
# LSST Data Management System
#
# Copyright 2008-2019  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#

import os
import lsst.ctrl.oods.directoryScanner as ds
import lsst.ctrl.oods.fileIngester as fi
import lsst.utils.tests
import tempfile
from shutil import copyfile
import yaml


def setup_module(module):
    lsst.utils.tests.init()


class Gen2IngesterTestCase(lsst.utils.tests.TestCase):
    """Test Scanning directory"""

    def setUp(self):
        package = lsst.utils.getPackageDir("ctrl_oods")
        testFile = os.path.join(package, "tests", "etc", "ingest.yaml")

        fitsFileName = "ats_exp_0_AT_C_20180920_000028.fits.fz"
        fitsFile = os.path.join(package, "tests", "etc", fitsFileName)

        mapperFileName = "_mapper"
        mapperPath = os.path.join(package, "tests", "etc", "_mapper")

        self.config = None
        with open(testFile, "r") as f:
            self.config = yaml.load(f)

        dataDir = tempfile.mkdtemp()
        self.config["ingester"]["directories"] = [dataDir]

        repoDir = tempfile.mkdtemp()
        self.config["ingester"]["butler"]["repoDirectory"] = repoDir

        destFile = os.path.join(dataDir, fitsFileName)
        copyfile(fitsFile, destFile)

        destFile = os.path.join(repoDir, mapperFileName)
        copyfile(mapperPath, destFile)

    def testIngest(self):
        scanner = ds.DirectoryScanner(self.config["ingester"])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = fi.FileIngester(self.config["ingester"])
        ingester.runTask()

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        ingester.runTask()

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

    def testVerbosityBatch(self):
        scanner = ds.DirectoryScanner(self.config["ingester"])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = fi.FileIngester(self.config["ingester"], verbose=True)
        ingester.runTask()

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

    def testVerbosityNoBatch(self):
        scanner = ds.DirectoryScanner(self.config["ingester"])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        self.config["ingester"]["batchSize"] = -1
        ingester = fi.FileIngester(self.config["ingester"], verbose=True)
        ingester.runTask()

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

    def testBatchSize(self):
        scanner = ds.DirectoryScanner(self.config["ingester"])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        self.config["ingester"]["batchSize"] = -1

        ingester = fi.FileIngester(self.config["ingester"])
        ingester.runTask()

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass
