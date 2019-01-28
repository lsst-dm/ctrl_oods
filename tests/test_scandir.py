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
import lsst.utils.tests
import tempfile


def setup_module(module):
    lsst.utils.tests.init()


class ScanDirTestCase(lsst.utils.tests.TestCase):
    """Test Scanning directory"""

    def testScanDir(self):

        dirPath = tempfile.mkdtemp()

        config = {}
        config["directories"] = [dirPath]

        scanner = ds.DirectoryScanner(config)
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
