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
    butlerConfig: `dict`
        details on how to construct and configure the butler
    """
    def __init__(self, butlerConfig):
        # create the butler
        classConfig = butlerConfig["class"]

        importFile = classConfig["import"]
        name = classConfig["name"]

        mod = import_module(importFile)
        butlerClass = getattr(mod, name)

        self.butlerInstance = butlerClass(butlerConfig)

        # load configuration info for the repository, staging,
        # and bad file areas
        self.repo_dir = butlerConfig["repoDirectory"]
        self.staging_dir = butlerConfig["stagingDirectory"]
        self.bad_file_dir = butlerConfig["badFileDirectory"]

    def getButler(self):
        """Return the butler being proxied

        Returns
        -------
        butler: `lsst.daf.butler.Butler`
            this butler instance
        """
        return self.butlerInstance

    def getStagingDirectory(self):
        """Return the path of the staging directory

        Returns
        -------
        staging_dir: `str`
            the staging directory
        """
        return self.staging_dir

    def getBadFileDirectory(self):
        """Return the path of the "bad file" directory

        Returns
        -------
        bad_file_dir: `str`
            the bad file directory
        """
        return self.bad_file_dir

    async def clean_task(self):
        """Run and await the async task code for this butler
        """
        await self.butlerInstance.clean_task()
