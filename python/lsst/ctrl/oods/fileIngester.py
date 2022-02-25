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
from lsst.ctrl.oods.fileQueue import FileQueue
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

    def __init__(self, config, csc=None):
        self.SUCCESS = 0
        self.FAILURE = 1
        self.config = config

        self.image_staging_dir = self.config["imageStagingDirectory"]

        self.fileQueue = FileQueue(self.image_staging_dir)

        butlerConfigs = self.config["butlers"]
        if len(butlerConfigs) == 0:
            raise Exception("No Butlers configured; check configuration file")

        self.butlers = []
        for butlerConfig in butlerConfigs:
            butler = ButlerProxy(butlerConfig["butler"], csc)
            self.butlers.append(butler)

        self.tasks = []
        self.dequeue_task = None

    def getStagingDirectory(self):
        """Return the directory where the external service stages files"""
        return self.image_staging_dir

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

    def create_link_to_file(self, filename, dirname):
        """Create a link from filename to a new file in directory dirname

        Parameters
        ----------
        filename : `str`
            Existing file to link to
        dirname : `str`
            Directory where new link will be located
        """
        # remove the staging area portion from the filepath; note that
        # we don't use os.path.basename here because the file might be
        # in a subdirectory of the staging directory.  We want to retain
        #  that subdirectory name

        basefile = Utils.strip_prefix(filename, self.image_staging_dir)

        # create a new full path to where the file will be linked for the OODS
        new_file = os.path.join(dirname, basefile)

        # hard link the file in the staging area
        # create the directory path where the file will be linked for the OODS
        new_dir = os.path.dirname(new_file)
        os.makedirs(new_dir, exist_ok=True)
        # hard link the file in the staging area
        os.link(filename, new_file)
        LOGGER.debug("created link to %s", new_file)

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
                    newfile = self.create_link_to_file(filename, local_staging_dir)
                    files[butlerProxy].append(newfile)
            except Exception as e:
                LOGGER.info("error staging files butler for %s, %s", filename, e)
                continue
            os.unlink(filename)
        return files

    async def ingest(self, file_list):
        """Attempt to perform butler ingest for all butlers

        Parameters
        ----------
        filename: `str`
            file to ingest
        """

        # first move the files from the image staging area
        # to the area where they're staged for the OODS.
        butler_file_lists = self.stageFiles(file_list)

        # for each butler, attempt to ingest the requested file,
        # Success or failure is noted in a message description which
        # is sent via RabbitMQ message back to Archiver, which will
        # send it out via a CSC logevent.
        try:
            for butler in self.butlers:
                butler.ingest(butler_file_lists[butler])
        except Exception as e:
            LOGGER.warn("Exception: %s", e)

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

        return self.tasks

    def stop_tasks(self):
        for task in self.tasks:
            task.cancel()
        self.tasks = []

    async def dequeue_and_ingest_files(self):
        while True:
            file_list = await self.fileQueue.dequeue_files()
            await self.ingest(file_list)
