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

from importlib import import_module


class ButlerProxy(object):
    """proxy interface to the gen2 or gen3 butler

    Parameters
    ----------
    butlerConfig : `dict`
        details on how to construct and configure the butler
    csc : `OodsCsc`
        OODS CSC
    """

    def __init__(self, butlerConfig, csc=None):
        # create the butler
        classConfig = butlerConfig["class"]

        importFile = classConfig["import"]
        name = classConfig["name"]

        mod = import_module(importFile)
        butlerClass = getattr(mod, name)

        self.butlerInstance = butlerClass(butlerConfig, csc)

        self.staging_dir = butlerConfig.get("stagingDirectory")

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
        await self.butlerInstance.clean_task()

    async def clean(self):
        """Call the butler's clean method"""
        await self.butlerInstance.clean()
