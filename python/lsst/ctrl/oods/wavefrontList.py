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

LOGGER = logging.getLogger(__name__)


class WavefrontList:
    """A list of WavefrontEntry objects"""

    def __init__(self, max_age_seconds=3.0):
        self.reset()
        self._max_age_seconds = max_age_seconds

    def extend(self, entries) -> None:
        """append an entry to the end of the list"""

        # first entry added has a timestamp, and a cutoff time
        if self._start_time is None:
            self._start_time = time.time()
            self._cutoff_time = self._start_time + self._max_age_seconds

        self._list.extend(entries)

    def should_ingest(self) -> bool:
        if len(self._list) == 8:
            LOGGER.info("8 files, ingesting")
            return True
        if self._cutoff_time is None:
            LOGGER.info("no cutoff time yet, returning false")
            return False
        LOGGER.info("here")
        return time.time() >= self._cutoff_time

    def get_list(self) -> list:
        return self._list

    def reset(self) -> None:
        self._list = []
        self._start_time = None
        self._cutoff_time = None
