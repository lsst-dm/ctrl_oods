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
import shutil
import os
import os.path
from importlib import import_module
import lsst.ctrl.notify.notify as notify
import lsst.ctrl.notify.inotifyEvent as inotifyEvent

LOGGER = logging.getLogger(__name__)


class FileIngester(object):
    """Ingest files into the butler specified in the configuration.
    Files must be removed from the directory as part of the ingest
    or there will be an attempt to ingest them again later.
    """

    def __init__(self, parent, config):
        self.parent = parent
        self.config = config

        self.directories = config["directories"]
        self.bad_file_dir = config["badFileDirectory"]

        butlerConfig = config["butler"]

        classConfig = butlerConfig["class"]

        # create the butler
        importFile = classConfig["import"]
        name = classConfig["name"]

        mod = import_module(importFile)
        butlerClass = getattr(mod, name)

        self.repo = butlerConfig["repoDirectory"]
        self.butler = butlerClass(self.repo)

        self.enabled_task = None

    async def read_event(self):
        loop = asyncio.get_event_loop()
        while True:
            if self.enabled_task is None:
                return
            event = await loop.run_in_executor(None, self.note.readEvent)
            if event is None:
                return
            asyncio.create_task(self.ingest_file(event.name))

    async def enable(self):
        self.note = notify.Notify()
        for directory in self.directories:
            self.note.addWatch(directory, inotifyEvent.IN_CREATE)

        self.enabled_task = asyncio.create_task(self.read_event())

    async def disable(self):
        self.note.cancelReadEvent()
        self.enabled_task.cancel()
        await self.enabled_task
        for directory in self.directories:
            self.note.rmWatch(directory)
        self.note.close()
        self.note = None

    def extract_cause(self, e):
        if e.__cause__ is None:
            return None
        cause = self.extract_cause(e.__cause__)
        if cause is None:
            return f"{str(e.__cause__)}"
        else:
            return f"{str(e.__cause__)};  {cause}"

    def create_bad_dirname(self, original):
        for dirname in self.directories:
            if original.startswith(dirname):
                # strip the original directory location, except for the date
                newfile = original.lstrip(dirname)
                # split into date and filename
                head, tail = os.path.split(newfile)
                # create subdirectory path name for self.bad_file_dir with date
                newdir = os.path.join(self.bad_file_dir, head)
                # create the directory, and hand the name back
                os.makedirs(newdir, exist_ok=True)
                return newdir
        return None

    async def ingest_file(self, filename):
        print(f"ingest_file called: trying to ingest {filename}")
        try:
            print(f"trying to ingest {filename}")
            self.butler.ingest(filename)
            LOGGER.info(f"{filename} ingested")
            obsid = "Fill me in properly"
            msg = f"OBSID {obsid}: File {filename} ingested into OODS"
            print(msg)
            if self.parent is not None:
                self.parent.send_imageInOODS(filename, msg, 0)
        except Exception as e:
            LOGGER.exception(e)
            bad_file_dir = self.create_bad_dirname(filename)
            try:
                msg = f"{filename} could not be ingested.  Moving to {bad_file_dir}: {self.extract_cause(e)}"
                shutil.move(filename, bad_file_dir)
            except Exception as fmException:
                LOGGER.info(f"Failed to move {filename} to {bad_file_dir} {fmException}")

            if self.parent is not None:
                self.parent.send_imageInOODS(filename, msg, 1)
            return

    async def run_task(self):
        # wait, to keep the object alive
        while True:
            await asyncio.sleep(60)
