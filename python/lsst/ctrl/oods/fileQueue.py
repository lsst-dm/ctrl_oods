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
import concurrent
import logging

from lsst.ctrl.oods.directoryScanner import DirectoryScanner

LOGGER = logging.getLogger(__name__)


class FileQueue(object):
    """Report on files that exist or appear in an existing directory.

    Parameters
    ----------
    dir_path: `str`
        A file directory to watch
    scanInterval: `int`, optional.
        The number of seconds to wait between directory scans. Defaults to 1.
    """

    def __init__(self, dir_path, scanInterval=1):
        self.dir_path = dir_path
        self.scanInterval = scanInterval

        self.fileSet = set()
        self.lock = asyncio.Lock()

    async def queue_files(self):
        """Queue all files that currently exist, and that are put
        into this directory
        """
        # scan for all files currently in this directory
        LOGGER.info("Scanning files in %s", self.dir_path)
        scanner = DirectoryScanner([self.dir_path])

        loop = asyncio.get_running_loop()
        # now, add all the currently known files to the queue
        while True:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                file_list = await loop.run_in_executor(pool, scanner.getAllFiles)

            async with self.lock:
                self.fileSet.update(file_list)
            await asyncio.sleep(self.scanInterval)

    async def dequeue_files(self):
        """Return all of the files retrieved so far"""
        # get a list of files, sort it, and clear the fileSet
        async with self.lock:
            file_list = list(self.fileSet)
            file_list.sort()
            self.fileSet.clear()
        return file_list
