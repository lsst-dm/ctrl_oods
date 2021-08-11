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

import asyncio
import logging
from lsst.ctrl.oods.directoryScanner import DirectoryScanner

LOGGER = logging.getLogger(__name__)


class FileQueue(object):
    """Report on files that exist or appear in an existing directory.

    Parameters
    ----------
    dir_path: `str`
        A file directory to watch
    scanInterval: `int`
        The number of seconds to wait between directory scans
    """

    def __init__(self, dir_path, scanInterval=1):
        self.dir_path = dir_path
        self.scanInterval = scanInterval

        self.queue = asyncio.Queue()

    async def queue_files(self):
        """Queue all files that currently exist, and that are put
        into this directory
        """
        # scan for all files currently in this directory
        scanner = DirectoryScanner([self.dir_path])

        # now, add all the currently known files to the queue
        while True:
            file_list = scanner.getAllFiles()
            if len(file_list) != 0:
                await self.queue.put(file_list)
            await asyncio.sleep(self.scanInterval)

    async def dequeue_files(self):
        file_list = await self.queue.get()
        self.queue.task_done()
        return file_list
