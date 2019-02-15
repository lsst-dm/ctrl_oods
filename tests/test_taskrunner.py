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
from lsst.ctrl.oods.taskRunner import TaskRunner
import lsst.utils.tests
from time import sleep
import unittest


class SimpleTask(object):
    def __init__(self):
        self.value = -1

    def run_task(self):
        self.value = 1
        return 1


class TaskRunnerTestCase(lsst.utils.tests.TestCase):
    def testRunner(self):
        st = SimpleTask()

        scanInterval = {"days": 0, "hours": 0, "minutes": 0, "seconds": 2}

        runner = TaskRunner(interval=scanInterval, task=st.run_task)
        runner.start()

        sleep(5)
        runner.stop()
        self.assertEqual(st.value, 1)
        runner.join()


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
