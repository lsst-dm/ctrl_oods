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
from lsst.ctrl.oods.timeInterval import TimeInterval

LOGGER = logging.getLogger(__name__)


class CacheCleaner(object):
    """Removes files and empty subdirectories older than a certain interval.

    Parameters
    ----------
    config : `dict`
        details on which directories to clean, and how often
    """

    def __init__(self, config):
        self.config = config
        self.files_and_directories = self.config["clearEmptyDirectoriesAndOldFiles"]
        self.only_empty_directories = []
        if "clearEmptyDirectories" in self.config:
            self.only_empty_directories = self.config["clearEmptyDirectories"]
        self.fileInterval = self.config["filesOlderThan"]
        self.emptyDirsInterval = self.config["directoriesEmptyForMoreThan"]
        scanInterval = self.config["scanInterval"]
        self.seconds = TimeInterval.calculateTotalSeconds(scanInterval)
        self.terminate = False

    async def run_tasks(self):
        """Check and clean directories at regular intervals"""
        self.terminate = False
        while True:
            self.clean()
            await asyncio.sleep(self.seconds)
            if self.terminate:
                return

    def stop_tasks(self):
        """Set flag to stop coroutines"""
        self.terminate = True

    def clean(self):
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

        files = self.getAllFilesOlderThan(seconds, self.files_and_directories)
        for name in files:
            LOGGER.info(f"removing file {name}")
            os.unlink(name)

        # remove empty directories
        seconds = TimeInterval.calculateTotalSeconds(self.emptyDirsInterval)
        seconds = now - seconds

        self.clearEmptyDirectories(seconds, self.files_and_directories)
        self.clearEmptyDirectories(seconds, self.only_empty_directories)

    def getAllFilesOlderThan(self, seconds, directories):
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
            files = self.getFilesOlderThan(seconds, name)
            allFiles.extend(files)
        return allFiles

    def getFilesOlderThan(self, seconds, directory):
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

        for dirName, subdirs, fileList in os.walk(directory):
            for fname in fileList:
                fullName = os.path.join(dirName, fname)
                stat_info = os.stat(fullName)
                modification_time = stat_info.st_mtime
                if modification_time < seconds:
                    files.append(fullName)
        return files

    def clearEmptyDirectories(self, seconds, directories):
        """Get files in one directory older than 'seconds'.

        Parameters
        ----------
        seconds : `int`
            age to match files against
        directories : `list`
            directories to observe
        """
        for directory in directories:
            dirs = self.getDirectoriesOlderThan(seconds=seconds, directory=directory)
            if len(dirs) != 0:
                self.removeEmptyDirectories(dirs)

    def getDirectoriesOlderThan(self, seconds, directory):
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

        for root, dirs, files in os.walk(directory, topdown=False):
            for name in dirs:
                full_name = os.path.join(root, name)
                stat_info = os.stat(full_name)
                mtime = stat_info.st_mtime
                if mtime < seconds:
                    directories.append(full_name)
        return directories

    def removeEmptyDirectories(self, directories):
        """Remove these directories, if they're empty.

        Parameters
        ----------
        directories: `list`
            directories to remove, if they're empty
        """
        for directory in sorted(directories, reverse=True):
            if os.listdir(directory) == []:
                LOGGER.info(f"removing {directory}")
                os.rmdir(directory)
