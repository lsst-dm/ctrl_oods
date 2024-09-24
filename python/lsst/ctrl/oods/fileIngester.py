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
import os.path

from lsst.ctrl.oods.butlerProxy import ButlerProxy
from lsst.ctrl.oods.cacheCleaner import CacheCleaner
from lsst.ctrl.oods.fileQueue import FileQueue
from lsst.ctrl.oods.timeInterval import TimeInterval
from lsst.ctrl.oods.utils import Utils

LOGGER = logging.getLogger(__name__)


class FileIngester(object):
    """Ingest files into the butler specified in the configuration.
    Files must be removed from the directory as part of the ingest
    or there will be an attempt to ingest them again later.

    Parameters
    ----------
    config: `dict`
        A butler configuration dictionary
    """

    def __init__(self, mainConfig, csc=None):
        self.SUCCESS = 0
        self.FAILURE = 1
        self.config = mainConfig["ingester"]

        self.image_staging_dir = self.config["imageStagingDirectory"]
        scanInterval = self.config["scanInterval"]
        seconds = TimeInterval.calculateTotalSeconds(scanInterval)

        self.fileQueue = FileQueue(self.image_staging_dir, seconds, csc)

        butlerConfigs = self.config["butlers"]
        if len(butlerConfigs) == 0:
            raise Exception("No Butlers configured; check configuration file")

        self.butlers = []
        for butlerConfig in butlerConfigs:
            butler = ButlerProxy(butlerConfig["butler"], csc)
            self.butlers.append(butler)

        cache_config = mainConfig["cacheCleaner"]
        self.cache_cleaner = CacheCleaner(cache_config, csc)

        self.tasks = []
        self.dequeue_task = None

    def getStagingDirectory(self):
        """Return the directory where the external service stages files"""
        return self.image_staging_dir

    def getButlerCleanMethods(self):
        methods = []
        for butler in self.butlers:
            methods.append(butler.clean)
        return methods

    def getButlerCleanTasks(self):
        """Get a list of all butler run_task methods

        Returns
        -------
        tasks: `list`
            A list containing each butler run_task method
        """
        tasks = []
        for butler in self.butlers:
            tasks.append(butler.clean_task)
        return tasks

    def move_staged_file(self, filename, dirname):
        """Move from filename to a new file in directory dirname

        Parameters
        ----------
        filename : `str`
            Existing file to move
        dirname : `str`
            Directory where new file will be located
        """
        # remove the staging area portion from the filepath; note that
        # we don't use os.path.basename here because the file might be
        # in a subdirectory of the staging directory.  We want to retain
        #  that subdirectory name

        basefile = Utils.strip_prefix(filename, self.image_staging_dir)

        # create a new full path to where the file will be moved for the OODS
        new_file = os.path.join(dirname, basefile)

        # create the directory path where the file will be moved for the OODS
        new_dir = os.path.dirname(new_file)
        os.makedirs(new_dir, exist_ok=True)
        # hard move the file in the staging area
        os.rename(filename, new_file)
        LOGGER.debug(f"moved {filename} to {new_file}")

        return new_file

    def stageFiles(self, file_list):
        """Stage all files from the initial directory to directories
        specific to each butler.
        """
        files = {}
        for butlerProxy in self.butlers:
            files[butlerProxy] = []
        for filename in file_list:
            try:
                for butlerProxy in self.butlers:
                    local_staging_dir = butlerProxy.getStagingDirectory()
                    newfile = self.move_staged_file(filename, local_staging_dir)
                    files[butlerProxy].append(newfile)
            except Exception as e:
                LOGGER.info("error staging files butler for %s, %s", filename, e)
                continue
        return files

    async def ingest(self, butler_file_list):
        """Attempt to perform butler ingest for all butlers

        Parameters
        ----------
        butler_file_list: `list`
            files to ingest
        """

        # for each butler, attempt to ingest the requested file,
        # Success or failure is noted in a message description which
        # will send out via a CSC logevent.
        try:
            for butler in self.butlers:
                await butler.ingest(butler_file_list[butler])
        except Exception as e:
            LOGGER.warning("Exception: %s", e)

    def run_tasks(self):
        """run tasks to queue files and ingest them"""

        # this is split into two tasks so they can run at slightly different
        # cadences.  We want to gather as many files as we can before we
        # do the ingest
        task = asyncio.create_task(self.fileQueue.queue_files())
        self.tasks.append(task)

        task = asyncio.create_task(self.dequeue_and_ingest_files())
        self.tasks.append(task)

        cleanTasks = self.getButlerCleanTasks()
        for cleanTask in cleanTasks:
            task = asyncio.create_task(cleanTask())
            self.tasks.append(task)

        self.tasks.append(asyncio.create_task(self.cache_cleaner.run_tasks()))

        return self.tasks

    def stop_tasks(self):
        LOGGER.info("stopping file scanning and file cleanup")
        self.cache_cleaner.stop_tasks()  # XXX - this might be redundant
        for task in self.tasks:
            task.cancel()
        self.tasks = []

    async def dequeue_and_ingest_files(self):
        while True:
            file_list = await self.fileQueue.dequeue_files()
            # First move the files from the image staging area
            # to the area where they're staged for the OODS.
            # Files staged here so the scanning asyncio routine doesn't
            # queue them twice.
            butler_file_list = self.stageFiles(file_list)
            await self.ingest(butler_file_list)
