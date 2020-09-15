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
import asyncio


class FakeParent:
    def send_imageInOODS(self, filename, msgDict, errorCode):
        pass


class Gen2IngesterTestCase(asynctest.TestCase):
    """Test Scanning directory"""

    def setUp(self):

        package = lsst.utils.getPackageDir("ctrl_oods")
        testFile = os.path.join(package, "tests", "etc", "ingest.yaml")

        self.fitsFileName = "ats_exp_0_AT_C_20180920_000028.fits.fz"
        self.fitsFile = os.path.join(package, "tests", "etc", self.fitsFileName)

        self.mapperFileName = "_mapper"
        self.mapperPath = os.path.join(package, "tests", "etc", "_mapper")

        self.config = None
        with open(testFile, "r") as f:
            self.config = yaml.safe_load(f)

        self.dataDir = tempfile.mkdtemp()
        print(f"looking in self.dataDir = {self.dataDir}")
        self.config["ingester"]["directories"] = [self.dataDir]

        self.repoDir = tempfile.mkdtemp()
        self.config["ingester"]["butler"]["repoDirectory"] = self.repoDir
        self.config["ingester"]["badFileDirectory"] = tempfile.mkdtemp()

        destFile2 = os.path.join(self.repoDir, self.mapperFileName)
        copyfile(self.mapperPath, destFile2)

        self.destFile1 = os.path.join(self.dataDir, self.fitsFileName)

    async def ingest(self):

        copyfile(self.fitsFile, self.destFile1)
        scanner = DirectoryScanner(self.config["ingester"])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(FakeParent(), self.config["ingester"])

        await ingester.ingest_file(self.destFile1)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)
        return 0

    def test_ingest(self):
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(self.ingest())
        self.assertEqual(result, 0)

    async def ingest_task(self):
        tempDir = tempfile.mkdtemp()
        tempFile = os.path.join(tempDir, self.fitsFileName)
        copyfile(self.fitsFile, tempFile)

        scanner = DirectoryScanner(self.config["ingester"])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        ingester = FileIngester(None, self.config["ingester"])
        await ingester.enable()

        await asyncio.sleep(3)

        self.assertEqual(len(files), 0)
        print(f"linking:  tempFile={tempFile}, destFile1={self.destFile1}")
        os.link(tempFile, self.destFile1)
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        await asyncio.sleep(3)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)
        await ingester.disable()
        return 0

    async def no_test_task(self):
        # result = loop.run_until_complete(self.ingest_task())
        task = asyncio.create_task(self.ingest_task())
        await task

    async def bad_ingest(self, parent):

        with open(self.destFile1, "w") as f:
            f.write("bad ingest")
        scanner = DirectoryScanner(self.config["ingester"])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(parent, self.config["ingester"])

        await ingester.ingest_file(self.destFile1)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)
        return 0

    def test_bad_ingest(self):
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(self.bad_ingest(None))
        self.assertEqual(result, 0)

    def test_bad_ingest_fake_parent(self):
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(self.bad_ingest(FakeParent()))
        self.assertEqual(result, 0)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass
