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
from lsst.ctrl.oods.fileAttendant import FileAttendant
from lsst.ctrl.oods.messageAttendant import MessageAttendant

LOGGER = logging.getLogger(__name__)


class ButlerProxy(object):
    """proxy interface to the gen2 or gen3 butler

    Parameters
    ----------
    butlerConfig : `dict`
        details on how to construct and configure the butler
    csc : `OodsCsc`
        OODS CSC
    """

    def __init__(self, main_config, csc=None):
        self.staging_dir = None

        # create the butler
        if main_config.file_ingester is not None:
            self.butlerInstance = FileAttendant(main_config, csc)
            self.staging_dir = main_config.file_ingester.staging_directory
        else:
            self.butlerInstance = MessageAttendant(main_config, csc)


    def getStagingDirectory(self):
        """Return the path of the staging directory
        Returns
        -------
        staging_dir : `str`
            the staging directory
        """
        return self.staging_dir

    async def ingest(self, file_list):
        """Bulk ingest of a list of files

        Parameters
        ----------
        file_list : `list`
            A list of file names
        """
        await self.butlerInstance.ingest(file_list)

    async def clean_task(self):
        """Call the butler's clean_task method"""
        try:
            await self.butlerInstance.clean_task()
        except asyncio.exceptions.CancelledError:
            LOGGER.info("cleaning task cancelled")

    async def send_status_task(self):
        await self.butlerInstance.send_status_task()

    async def clean(self):
        """Call the butler's clean method"""
        await self.butlerInstance.clean()
