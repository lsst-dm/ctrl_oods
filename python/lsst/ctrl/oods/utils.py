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
from pathlib import PurePath


class Utils:
    """statis utility functions
    """

    @staticmethod
    def strip_prefix(pathname, prefix):
        """Strip the prefix of the path

        Parameters
        ----------
        pathname: `str`
            Path name
        prefix: `str`
            Prefix to strip from pathname

        Returns
        -------
        ret: `str`
            The remaining path
        """
        p = PurePath(pathname)
        ret = str(p.relative_to(prefix))
        return ret

    @staticmethod
    def create_bad_dirname(bad_dir_root, staging_dir_root, original):
        """Create a full path to a directory contained in the
        'bad directory' heirarchy; this retains the subdirectory structure
        created where the file was staged, where the uningestable file will
        be placed.

        Parameters
        ----------
        bad_dir_root: `str`
            Root of the bad directory heirarchy
        staging_dir_root: `str`
            Root of the bad directory heirarchy
        original: `str`
            Original directory location

        Returns
        -------
        newdir: `str`
            new directory name

        Raises
        ------
        PermissionError
            Raised if the directory could not be created
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
