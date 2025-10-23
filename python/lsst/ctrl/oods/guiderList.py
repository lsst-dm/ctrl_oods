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


import logging
import time

from lsst.ctrl.oods.guiderEntry import GuiderEntry
from lsst.resources import ResourcePath

LOGGER = logging.getLogger(__name__)


class GuiderList(list[GuiderEntry]):
    """A list of GuiderEntry objects"""

    def remove_by_name(self, guider_file: str) -> None:
        """Remove the matching guider_file name

        Parameters
        ----------
        guider_file: `str`
            name of the guider file to remove from the list
        """
        for i, entry in enumerate(self):
            if entry.guider_resource_path == guider_file:
                del self[i]
                return

    def purge_old_entries(self, max_age_seconds: int) -> list[GuiderEntry]:
        """Remove entries older than the specified number of seconds

        Parameters
        ----------
        max_age_seconds: `int`
            maximum number of seconds the file should be

        Returns
        -------
        removed: `list[GuiderEntry]`
            entries removed from the list
        """
        current_time = time.time()
        cutoff_time = current_time - max_age_seconds
        kept = GuiderList()
        removed = GuiderList()

        for entry in self:
            if entry.timestamp >= cutoff_time:
                kept.append(entry)
            else:
                removed.append(entry)

        # replace in place
        self[:] = kept

        return removed

    def get_guider_resource_paths(self) -> list[ResourcePath]:
        """Get all file names in this list

        Returns
        -------
        names: `list[ResourcePath]`
            a list of ResourcePaths
        """
        return [e.guider_resource_path for e in self]
