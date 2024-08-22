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
import os


class DirectoryScanner(object):
    """Scan directories for files

    Parameters
    ----------
    directories: `list`
        A list of directories to scan
    """

    def __init__(self, directories):
        self.directories = directories

    async def getAllFiles(self):
        """Retrieve all files from a set of directories

        Parameters
        ----------
        directories: `list`
            directories to scan

        Returns
        -------
        allFiles: `list`
            list of all files in the given directories
        """
        allFiles = []
        for directory in self.directories:
            files = await self.getFiles(directory)
            allFiles.extend(files)
        return allFiles

    async def getFiles(self, directory):
        """Retrieve all files from a directory

        Parameters
        ----------
        directory: `str`
            directory to scan

        Returns
        -------
        files: `list`
            list of all files in the given directory
        """
        files = []
        for entry in self._scanner(directory):
            await asyncio.sleep(0)
            files.append(entry)
        return files

    def _scanner(self, directory):
        """Retrieve all files from a directory, yielding on
        each file that is found

        Parameters
        ----------
        directory: `str`
            directory to scan
        """
        for e in os.scandir(directory):
            if e.is_dir(follow_symlinks=False):
                yield from self._scanner(e.path)
            else:
                yield e.path
