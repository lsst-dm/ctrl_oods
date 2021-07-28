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

import lsst.utils.tests
from lsst.ctrl.oods.imageFile import ImageFile


class ImageFileTestCase(lsst.utils.tests.TestCase):
    """Test ImageFile object"""

    def testImageFileAT(self):
        FILENAME = "AT_O_20210709_000012_R02S01.fits"
        image = ImageFile(FILENAME)

        self.assertEqual(image.archiver, "ATArchiver")
        self.assertEqual(image.camera, "LATISS")
        self.assertEqual(image.filename, FILENAME)
        self.assertEqual(image.obsid, "AT_O_20210709_000012")
        self.assertEqual(image.raft, "02")
        self.assertEqual(image.sensor, "01")

    def testImageFileCC(self):
        FILENAME = "CC_O_20210709_000012_R22S21.fits"
        image = ImageFile(FILENAME)

        self.assertEqual(image.archiver, "CCArchiver")
        self.assertEqual(image.camera, "COMCAM")
        self.assertEqual(image.filename, FILENAME)
        self.assertEqual(image.obsid, "CC_O_20210709_000012")
        self.assertEqual(image.raft, "22")
        self.assertEqual(image.sensor, "21")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
