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
from threading import Thread
import time
from lsst.ctrl.oods.timeInterval import TimeInterval


class TaskRunner(Thread):
    """Continously: run a task and then sleep.
    """
    def __init__(self, interval, task, *args, **kwargs):
        super(TaskRunner, self).__init__()
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.isRunning = True

        self.pause = TimeInterval.calculateTotalSeconds(interval)

    def run(self):
        """Execute the task repeatedly, pausing between execution steps.
        This runs the task first, for however long it takes, and then
        sleeps for the previously specified interval.  This is to
        perform the initial task immediately, rather than waiting for
        a potentially long time before executing anything.
        """
        while self.isRunning:
            self.task(*self.args, **self.kwargs)
            time.sleep(self.pause)

    def stop(self):
        """Stop executing after last pause
        """
        self.isRunning = False
