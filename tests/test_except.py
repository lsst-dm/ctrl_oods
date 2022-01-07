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

import asynctest
import lsst.utils.tests
from lsst.ctrl.oods.butlerIngester import ButlerIngester


class DummyIngester(ButlerIngester):

    def ingest(self, filename):
        print(filename)

    def getName(self):
        return "Dummy"


class Gen3ComCamIngesterTestCase(asynctest.TestCase):
    """Test Gen3 Butler Ingest"""

    async def testException(self):
        """test exception extraction
        """
        dummy = DummyIngester()
        try:
            print(3.14/0)
        except ZeroDivisionError as e:
            s = dummy.extract_cause(e)
            self.assertEqual(s, "float division by zero")

    async def testException2(self):
        """test exception extraction
        """
        dummy = DummyIngester()
        try:
            print(3.14/0)
        except ZeroDivisionError as e:
            try:
                raise ZeroDivisionError("divide by zero") from e
            except ZeroDivisionError as e2:
                s = dummy.extract_cause(e2)
                self.assertEqual(s, "float division by zero")

    async def testException3(self):
        """test exception extraction
        """
        dummy = DummyIngester()
        try:
            try:
                print(3.14/0)
            except ZeroDivisionError:
                raise TypeError("TypeError")
        except TypeError as e:
            s = dummy.extract_cause(e)
            self.assertEqual(s, "TypeError")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
