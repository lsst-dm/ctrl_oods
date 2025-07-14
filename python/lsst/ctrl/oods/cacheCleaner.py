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
import os
import time

from lsst.ctrl.oods.scanner import Scanner
from lsst.ctrl.oods.oods_config import TimeInterval

LOGGER = logging.getLogger(__name__)


class CacheCleaner(object):
    """Removes files and empty subdirectories older than a certain interval.

    Parameters
    ----------
    config : `dict`
        details on which directories to clean, and how often
    """

    def __init__(self, config, csc=None):
        self.config = config
        self.csc = csc
        self.files_and_directories = self.config.clear_empty_directories_and_old_files
        self.only_empty_directories = []
        # XXX
        #if "clearEmptyDirectories" in self.config:
        #    self.only_empty_directories = self.config["clearEmptyDirectories"]
        self.fileInterval = self.config.files_older_than
        self.emptyDirsInterval = self.config.directories_empty_for_more_than
        scanInterval = self.config.cleaning_interval
        self.seconds = TimeInterval.calculateTotalSeconds(scanInterval)
        self.terminate = False

    async def run_tasks(self):
        """Check and clean directories at regular intervals"""
        self.terminate = False
        while True:
            if self.csc:
                self.csc.log.info("Cleaning %s", self.files_and_directories)

            await self.clean()

            if self.csc:
                self.csc.log.info("done cleaning; waiting %d seconds", self.seconds)
            await asyncio.sleep(self.seconds)
            if self.terminate:
                return

    def stop_tasks(self):
        """Set flag to stop coroutines"""
        self.terminate = True

    async def clean(self):
        """Remove files older than a given interval, and directories
        that have been empty for a given interval.
        """

        # The removal of files and directories have different
        # intervals.  Files are removed based on how long it has been
        # since the file was last modified.  Directories are removed
        # based on how long it's been since they've been empty.

        now = time.time()

        # remove old files
        seconds = TimeInterval.calculateTotalSeconds(self.fileInterval)
        seconds = now - seconds

        files = await self.getAllFilesOlderThan(seconds, self.files_and_directories)
        for name in files:
            await asyncio.sleep(0)
            LOGGER.info("removing %s", name)
            try:
                os.unlink(name)
            except Exception as e:
                LOGGER.info("Couldn't remove %s: %s", name, e)

        # remove empty directories
        seconds = TimeInterval.calculateTotalSeconds(self.emptyDirsInterval)
        seconds = now - seconds

        await self.clearEmptyDirectories(seconds, self.files_and_directories)
        await self.clearEmptyDirectories(seconds, self.only_empty_directories)

    async def getAllFilesOlderThan(self, seconds, directories):
        """Get files in directories older than 'seconds'.

        Parameters
        ----------
        seconds: `int`
            age to match files against
        directories: `list`
            directories to observe

        Returns
        -------
        allFiles: `list`
            all files that haven't been modified in 'seconds'
        """
        allFiles = []
        for name in directories:
            files = await self.getFilesOlderThan(seconds, name)
            allFiles.extend(files)
        return allFiles

    async def getFilesOlderThan(self, seconds, directory):
        """Get files in one directory older than 'seconds'.

        Parameters
        ----------
        seconds: `int`
            age to match files against
        directory: `str`
            directory to observe

        Returns
        -------
        files: `list`
            All files that haven't been modified in 'seconds'
        """
        files = []

        scanner = Scanner()
        async for entry in scanner.scan(directory):
            if entry.is_dir():
                continue
            full_name = entry.path
            stat_info = os.stat(full_name)
            modification_time = stat_info.st_mtime
            if modification_time < seconds:
                files.append(full_name)
        return files

    async def clearEmptyDirectories(self, seconds, directories):
        """Get files in one directory older than 'seconds'.

        Parameters
        ----------
        seconds : `int`
            age to match files against
        directories : `list`
            directories to observe
        """
        for directory in directories:
            dirs = await self.getDirectoriesOlderThan(seconds=seconds, directory=directory)
            if len(dirs) != 0:
                await self.removeEmptyDirectories(dirs)

    async def getDirectoriesOlderThan(self, seconds, directory):
        """Get subdirectories older than "seconds"; Note that
        we get the list of all these directories now, and delete
        later. If we delete in place here, the parent directory modification
        time gets changed, and we end up keeping parent directories around
        a full cycle longer than they should be.

        Parameters
        ----------
        seconds: `int`
            age to match files against
        directory: `str`
            single directory to observe

        Returns
        -------
        directories: `list`
            all subdirectories empty for more than 'seconds'
        """
        directories = []

        scanner = Scanner()
        async for entry in scanner.scan(directory):
            if entry.is_file():
                continue
            full_name = entry.path
            stat_info = os.stat(full_name)
            modification_time = stat_info.st_mtime
            if modification_time < seconds:
                directories.append(full_name)
        return directories

    async def removeEmptyDirectories(self, directories):
        """Remove these directories, if they're empty.

        Parameters
        ----------
        directories: `list`
            directories to remove, if they're empty
        """
        for directory in sorted(directories, reverse=True):
            await asyncio.sleep(0)
            if self._isEmpty(directory):
                LOGGER.info("removing %s", directory)
                try:
                    os.rmdir(directory)
                except Exception as e:
                    LOGGER.info("Couldn't remove %s: %s", directory, e)

    def _isEmpty(self, directory):
        with os.scandir(directory) as entries:
            for entry in entries:
                return False
            return True
