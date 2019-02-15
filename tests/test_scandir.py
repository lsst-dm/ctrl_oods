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
from lsst.ctrl.oods.directoryScanner import DirectoryScanner
import lsst.utils.tests
import tempfile
import unittest


class ScanDirTestCase(lsst.utils.tests.TestCase):
    """Test Scanning directory"""

    def testScanDir(self):

        dirPath = tempfile.mkdtemp()

        config = {}
        config["directories"] = [dirPath]

        scanner = DirectoryScanner(config)
        files = scanner.getAllFiles()

        self.assertEqual(len(files), 0)

        (fh1, filename1) = tempfile.mkstemp(dir=dirPath)
        (fh2, filename2) = tempfile.mkstemp(dir=dirPath)
        (fh3, filename3) = tempfile.mkstemp(dir=dirPath)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 3)

        os.close(fh1)
        os.remove(filename1)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 2)

        os.close(fh2)
        os.remove(filename2)
        os.close(fh3)
        os.remove(filename3)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        os.rmdir(dirPath)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
