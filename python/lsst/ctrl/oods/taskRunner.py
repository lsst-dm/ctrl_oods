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

        ti = TimeInterval(interval)
        self.pause = ti.calculateTotalSeconds()

    def run(self):
        """ execute the task repeatedly, pausing between
        execution steps
        """
        while self.isRunning:
            self.task(*self.args, **self.kwargs)
            time.sleep(self.pause)

    def stop(self):
        """Stop executing after last pause
        """
        self.isRunning = False
