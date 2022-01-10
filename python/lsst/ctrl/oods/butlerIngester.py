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
from abc import ABC, abstractmethod


class ButlerIngester(ABC):
    """Interface class for processing files for ingestion into a butler.
    """

    @abstractmethod
    def ingest(self, filename):
        """Placeholder to ingest a file

        Parameters
        ----------
        filename: `str`
            file name to ingest
        """
        raise NotImplementedError()

    @abstractmethod
    def getName(self):
        """Get the name of this ingester

        Returns
        -------
        ret: `str`
            the name of this ingester
        """
        raise NotImplementedError()

    def extract_cause(self, e):
        """extract the cause of an exception

        Returns
        -------
        s: `str`
            A string containing the cause of an exception
        """
        if e.__cause__ is None:
            return str(e)
        else:
            return str(e.__cause__)

    async def run_task(self):
        """Run task that require periodical attention
        """
        await asyncio.sleep(60)

    def clean(self):
        """Perform a cleaning pass for this ingester; override if necessary
        """
        pass
