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

import os
import time
from lsst.ctrl.oods.timeInterval import TimeInterval


class CacheCleaner(object):
    """Removes files and subdirectories older than a certain interval"""

    def __init__(self, config, verbose=False):
        self.config = config
        self.verbose = verbose

    def runTask(self):
        """Remove files older than a given, and directories
        that have been empty for a given interval
        """

        # The removal of files and directories have different
        # intervals.  Files are removed based on how long it has been
        # since the file was last modified.  Directories are removed
        # based on how long it's been since they've been empty.

        directories = self.config["directories"]

        # remove old files
        fileInterval = self.config["filesOlderThan"]
        interval = TimeInterval(fileInterval)
        seconds = interval.calculateTotalSeconds()

        files = self.getAllFilesOlderThan(seconds, directories)
        for name in files:
            if self.verbose:
                print("removing", name)
            os.unlink(name)

        # remove empty directories
        emptyDirsInterval = self.config["directoriesEmptyForMoreThan"]
        interval = TimeInterval(emptyDirsInterval)
        seconds = interval.calculateTotalSeconds()

        dirs = self.getAllEmptyDirectoriesOlderThan(seconds, directories)
        for name in dirs:
            if self.verbose:
                print("removing", name)
            os.rmdir(name)

    def getAllFilesOlderThan(self, seconds, directories):
        """Get files in directories older than a certain time
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
        """Get files in one directory older than a certain time
        @param seconds: age to match files against
        @param directory: directory to observe
        @return: all files that haven't been modified in 'seconds'
        """
        files = []
        now = time.time()
        olderThan = now - seconds

        for dirName, subdirs, fileList in os.walk(directory):
            for fname in fileList:
                fullName = os.path.join(dirName, fname)
                stat_info = os.stat(fullName)
                modification_time = stat_info.st_mtime
                if modification_time < olderThan:
                    files.append(fullName)
        return files

    def getAllEmptyDirectoriesOlderThan(self, seconds, directories):
        """Get subdirectories empty more than 'seconds' in all directories
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
        """Get subdirectories empty more than 'seconds' in a directory
        @param seconds: age to match files against
        @param directory: single directory to observe
        @return: all subdirectories empty for more than 'seconds'
        """
        directories = []
        now = time.time()
        olderThan = now - seconds
        for root, dirs, files in os.walk(directory, topdown=False):
            for name in dirs:
                fullName = os.path.join(root, name)
                if os.listdir(fullName) == []:
                    stat_info = os.stat(fullName)
                    modification_time = stat_info.st_mtime
                    if modification_time < olderThan:
                        directories.append(fullName)
        return directories
