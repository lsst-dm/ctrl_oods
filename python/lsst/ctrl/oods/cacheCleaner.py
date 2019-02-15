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
import os
import time
from lsst.ctrl.oods.timeInterval import TimeInterval


class CacheCleaner(object):
    """Removes files and subdirectories older than a certain interval."""

    def __init__(self, logger, config):
        self.logger = logger
        self.config = config
        self.directories = self.config["directories"]
        self.fileInterval = self.config["filesOlderThan"]
        self.emptyDirsInterval = self.config["directoriesEmptyForMoreThan"]

    def run_task(self):
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

        files = self.getAllFilesOlderThan(seconds, self.directories)
        for name in files:
            self.logger.info("removing", name)
            os.unlink(name)

        # remove empty directories
        seconds = TimeInterval.calculateTotalSeconds(self.emptyDirsInterval)
        seconds = now - seconds

        dirs = self.getAllEmptyDirectoriesOlderThan(seconds, self.directories)
        for name in dirs:
            self.logger.info("removing", name)
            os.rmdir(name)

    def getAllFilesOlderThan(self, seconds, directories):
        """Get files in directories older than 'seconds'.
        @param seconds: age to match files against
        @param directories: directories to observe
        @return: all files that haven't been modified in 'seconds'
        """
        allFiles = []
        for name in directories:
            files = self.getFilesOlderThan(seconds, name)
            allFiles.extend(files)
        return allFiles

    def getFilesOlderThan(self, seconds, directory):
        """Get files in one directory older than 'seconds'.
        @param seconds: age to match files against
        @param directory: directory to observe
        @return: all files that haven't been modified in 'seconds'
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

    def getAllEmptyDirectoriesOlderThan(self, seconds, directories):
        """Get subdirectories empty more than 'seconds' in all directories.
        @param seconds: age to match files against
        @param directories: directories to observe
        @return: all subdirectories empty for more  than 'seconds'
        """
        allDirs = []
        for name in directories:
            dirs = self.getEmptyDirectoriesOlderThan(seconds, name)
            allDirs.extend(dirs)
        return allDirs

    def getEmptyDirectoriesOlderThan(self, seconds, directory):
        """Get subdirectories empty more than 'seconds' in a directory.
        All subdirectories are checked to see if they're empty and are marked
        as older than 'seconds" if the modification time for that directory
        is at least that old.
        @param seconds: age to match files against
        @param directory: single directory to observe
        @return: all subdirectories empty for more than 'seconds'
        """
        directories = []

        for root, dirs, files in os.walk(directory, topdown=False):
            for name in dirs:
                fullName = os.path.join(root, name)
                if os.listdir(fullName) == []:
                    stat_info = os.stat(fullName)
                    modification_time = stat_info.st_mtime
                    if modification_time < seconds:
                        directories.append(fullName)
        return directories
