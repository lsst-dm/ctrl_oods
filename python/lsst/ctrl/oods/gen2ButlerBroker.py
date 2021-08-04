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
import logging
import os
import shutil
from lsst.ctrl.oods.butlerBroker import ButlerBroker
from lsst.pipe.tasks.ingest import IngestTask

LOGGER = logging.getLogger(__name__)


class Gen2ButlerBroker(ButlerBroker):
    """Processes files on behalf of a Gen2 Butler.

    Parameters
    ----------
    butlerConfig: `dict`
        dictionary containing butler configuration information
    """
    def __init__(self, butlerConfig):
        self.repo_dir = butlerConfig["repoDirectory"]
        self.task = IngestTask.prepareTask(self.repo_dir)

        self.staging_dir = butlerConfig["stagingDirectory"]
        self.bad_file_dir = butlerConfig["badFileDirectory"]

    def ingest(self, file_list):
        """Ingest a list of files into a butler

        Parameters
        ----------
        file_list: `list`
            file names to ingest
        """
        try:
            self.task.ingestFiles(file_list)
        except Exception as e:
            status_code = self.FAILURE
            status_message = f"gen2: error ingesting: {self.extract_cause(e)}"
            for filename in file_list:
                self.move_to_bad_dir(filename)
            return (status_code, status_message)
        return(self.SUCCESS, "gen2: files ingested")

    def move_to_bad_dir(self, filename):
        if os.path.exists(filename) is False:
            return
        bad_dir = self.create_bad_dirname(self.bad_file_dir, self.staging_dir, filename)
        try:
            shutil.move(filename, bad_dir)
        except Exception as fmException:
            LOGGER.info(f"Failed to move {filename} to {self.bad_file_dir} {fmException}")

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
