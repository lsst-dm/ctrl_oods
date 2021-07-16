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
import lsst.ctrl.notify.notify as notify
import lsst.ctrl.notify.inotifyEvent as inotifyEvent

LOGGER = logging.getLogger(__name__)


class FileHandler(object):
    """Report on files that exist or appear in an existing directory.


    Parameters
    ----------
    dir_path: `str`
        A file directory to watch
    """

    def __init__(self, dir_path):
        self.note = notify.Notify()
        self.dir_path = dir_path

        self.note.addWatch(self.dir_path, inotifyEvent.IN_CLOSE_WRITE)
        self.queue = asyncio.Queue()

    async def queue_files(self):
        """Queue all files that currently exist, and that are put
        into this directory
        """
        # scan for all files currently in this directory
        scanner = DirectoryScanner([self.dir_path])
        all_files = scanner.getAllFiles()

        # There's a race condition, in which a file
        # (or files) could be added to the directory,
        # be picked up by DirectoryScanner before
        # before notify object could be read, resulting
        # in a duplicate file. To prevent that, the Notify
        # object is read until all current events are depleted
        # and added to the list of files to queue.
        #
        # Attempt to read an event.  If the file does not
        # exist in the list, add it. Do this until we
        # run out of events. Note that it's done this way
        # because there's no method call to check to see
        # if an entry already exists in the queue.
        #
        while True:
            event = await self.note.readEvent(0)
            if event is None:
                break
            if event.name not in all_files:
                all_files.append(event.name)

        # now, add all the currently known files to the queue
        for file in all_files:
            await self.queue.put(file)

        # continously read events, blocking until we
        # get one, and add the file to the queue.
        while True:
            event = await self.note.readEvent()
            if event is not None:
                await self.queue.put(event.name)

    async def dequeue_file(self):
        file = await self.queue.get()
        self.queue.task_done()
        return file
