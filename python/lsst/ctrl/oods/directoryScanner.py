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


class DirectoryScanner(object):
    """Scan directories for files
    """
    def __init__(self, config):
        self.config = config

    def getAllFiles(self):
        """Retrieve all files from a set of directories
        @param directories: list directories to scan
        @return list of files in the given directories
        """
        directories = self.config["directories"]
        allFiles = []
        for directory in directories:
            files = self.getFiles(directory)
            allFiles.extend(files)
        return allFiles

    def getFiles(self, directory):
        """Retrieve all files from a directory
        @param directory: directory to scan
        @return list of files in the given directory
        """
        files = []
        for dirName, subdirs, fileList in os.walk(directory):
            for fname in fileList:
                fullName = os.path.join(dirName, fname)
                files.append(fullName)
        return files
