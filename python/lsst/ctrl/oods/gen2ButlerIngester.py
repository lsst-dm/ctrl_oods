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
from lsst.ctrl.oods.butlerIngester import ButlerIngester
from lsst.pipe.tasks.ingest import IngestTask


class Gen2ButlerIngester(ButlerIngester):
    """Processes files for ingestion into a Gen2 Butler.
    """
    def __init__(self, butlerConfig):
        repo = butlerConfig["repoDirectory"]
        self.task = IngestTask.prepareTask(repo)

    def ingest(self, filename):
        self.task.ingestFiles(filename)

    def getName(self):
        return "gen2"

    async def run_task(self):
        while True:
            await asyncio.sleep(60)
