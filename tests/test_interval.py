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

from lsst.ctrl.oods.timeInterval import TimeInterval
import lsst.utils.tests


def setup_module(module):
    lsst.utils.tests.init()


class IntervalTestCase(lsst.utils.tests.TestCase):
    """Test cache cleaning"""

    def testFileCleaner(self):

        config = {}
        config["days"] = 1
        config["hours"] = 1
        config["minutes"] = 1
        config["seconds"] = 1

        interval = TimeInterval(config)

        seconds = interval.calculateTotalSeconds()
        self.assertTrue(seconds, 86400+3600+60+1)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass
