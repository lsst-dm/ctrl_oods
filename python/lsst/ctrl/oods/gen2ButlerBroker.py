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
from lsst.ctrl.oods.butlerBroker import ButlerBroker
from lsst.pipe.tasks.ingest import IngestTask


class Gen2ButlerBroker(ButlerBroker):
    """Processes files on behalf of a Gen2 Butler.

    Parameters
    ----------
    butlerConfig: `dict`
        dictionary containing butler configuration information
    """
    def __init__(self, butlerConfig):
        repo = butlerConfig["repoDirectory"]
        self.task = IngestTask.prepareTask(repo)

    def ingest(self, filename):
        """Ingest a file into a butler

        Parameters
        ----------
        filename: `str`
            filename to ingest
        """
        self.task.ingestFiles(filename)

    def getName(self):
        """Get the name of this ingester

        Returns
        -------
        ret: `str`
            the name of this ingester
        """
        return "gen2"

    async def cleaner_task(self):
        """Run task that require periodical attention
        """
        while True:
            await asyncio.sleep(60)
