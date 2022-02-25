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
import os.path
from lsst.ctrl.oods.utils import Utils
from abc import ABC, abstractmethod


class ButlerIngester(ABC):
    """Interface class for processing files for a butler."""

    SUCCESS = 0
    FAILURE = 1

    @abstractmethod
    def ingest(self, file_list):
        """Placeholder to ingest a file

        Parameters
        ----------
        file_list : `list`
            list of files to ingest
        """
        raise NotImplementedError()

    @abstractmethod
    def getName(self):
        """Get the name of this ingester

        Returns
        -------
        ret : `str`
            the name of this ingester
        """
        raise NotImplementedError()

    async def cleaner_task(self):
        """Run task that require periodical attention"""
        await asyncio.sleep(60)

    def clean(self):
        """Perform a cleaning pass for this ingester; override if necessary"""
        pass

    def create_bad_dirname(self, bad_dir_root, staging_dir_root, original):
        """Create a full path to a directory contained in the
        'bad directory' hierarchy; this retains the subdirectory structure
        created where the file was staged, where the uningestable file will
        be placed.

        Parameters
        ----------
        bad_dir_root : `str`
            Root of the bad directory hierarchy
        staging_dir_root : `str`
            Root of the bad directory hierarchy
        original : `str`
            Original directory location

        Returns
        -------
        newdir : `str`
            new directory name
        """
        # strip the original directory location, except for the date
        newfile = Utils.strip_prefix(original, staging_dir_root)

        # split into subdir and filename
        head, tail = os.path.split(newfile)

        # create subdirectory path name for directory with date
        newdir = os.path.join(bad_dir_root, head)

        # create the directory, and hand the name back
        os.makedirs(newdir, exist_ok=True)

        return newdir

    def extract_cause(self, e):
        """extract the cause of an exception

        Returns
        -------
        s : `str`
            A string containing the cause of an exception
        """
        if e.__cause__ is None:
            return None
        cause = self.extract_cause(e.__cause__)
        if cause is None:
            return f"{str(e.__cause__)}"
        else:
            return f"{str(e.__cause__)};  {cause}"
