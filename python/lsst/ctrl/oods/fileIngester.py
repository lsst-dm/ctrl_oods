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
import shutil
import os
import os.path
from pathlib import PurePath
from lsst.ctrl.oods.butlerProxy import ButlerProxy
from lsst.ctrl.oods.imageFile import ImageFile
from lsst.ctrl.oods.fileQueue import FileQueue
from lsst.dm.csc.base.publisher import Publisher

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

    def __init__(self, config):
        self.SUCCESS = 0
        self.FAILURE = 1
        self.config = config

        self.forwarder_staging_dir = config["forwarderStagingDirectory"]

        self.fileQueue = FileQueue(self.forwarder_staging_dir)

        if 'baseBrokerAddr' in config:
            self.base_broker_url = config["baseBrokerAddr"]
        else:
            self.base_broker_url = None

        butlerConfigs = config["butlers"]
        if len(butlerConfigs) == 0:
            raise Exception("No Butlers configured; check configuration file")

        self.butlers = []
        for butlerConfig in butlerConfigs:
            butler = ButlerProxy(butlerConfig["butler"])
            self.butlers.append(butler)

        self.tasks = []

        self.PUBLISH_QUEUE = config['PUBLISH_QUEUE']

        if self.base_broker_url is not None:
            asyncio.create_task(self.start_comm())

    def getStagingDirectory(self):
        return self.forwarder_staging_dir

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

    async def start_comm(self):
        """Start communication services
        """

        self.publisher = Publisher(self.base_broker_url)
        await self.publisher.start()

    def extract_cause(self, e):
        """extract the cause of an exception

        Returns
        -------
        s: `str`
            A string containing the cause of an exception
        """
        if e.__cause__ is None:
            return None
        cause = self.extract_cause(e.__cause__)
        if cause is None:
            return f"{str(e.__cause__)}"
        else:
            return f"{str(e.__cause__)};  {cause}"

    def create_bad_dirname(self, bad_dir_root, staging_dir_root, original):
        """Create a full path to a directory contained in the
        'bad directory' heirarchy; this retains the subdirectory structure
        created where the file was staged, where the uningestable file will
        be placed.

        Parameters
        ----------
        bad_dir_root: `str`
            Root of the bad directory heirarchy
        staging_dir_root: `str`
            Root of the bad directory heirarchy
        original: `str`
            Original directory location

        Returns
        -------
        newdir: `str`
            new directory name
        """
        # strip the original directory location, except for the date
        newfile = self.strip_prefix(original, staging_dir_root)

        # split into subdir and filename
        head, tail = os.path.split(newfile)

        # create subdirectory path name for directory with date
        newdir = os.path.join(bad_dir_root, head)

        # create the directory, and hand the name back
        os.makedirs(newdir, exist_ok=True)

        return newdir

    def strip_prefix(self, pathname, prefix):
        """Strip the prefix of the path

        Parameters
        ----------
        pathname: `str`
            Path name
        prefix: `str`
            Prefix to strip from pathname

        Returns
        -------
        ret: `str`
            The remaining path
        """
        p = PurePath(pathname)
        ret = str(p.relative_to(prefix))
        return ret

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
        basefile = self.strip_prefix(filename, self.forwarder_staging_dir)

        # create a new full path to where the file will be linked for the OODS
        new_file = os.path.join(dirname, basefile)

        # hard link the file in the staging area
        # create the directory path where the file will be linked for the OODS
        new_dir = os.path.dirname(new_file)
        os.makedirs(new_dir, exist_ok=True)
        # hard link the file in the staging area
        os.link(filename, new_file)
        LOGGER.info(f"created link to {new_file}")

        return new_file

    def stageFiles(self, filename):
        """ stage the files to their butler staging areas
        and remove the original file

        Parameters
        ----------
        filename: `str`
            file to ingest
        """

        try:
            for butlerProxy in self.butlers:
                local_staging_dir = butlerProxy.getStagingDirectory()
                self.create_link_to_file(filename, local_staging_dir)
        except Exception:
            LOGGER.info(f"error staging files butler for {filename}")
            return
        # file has been linked to all staging areas;
        # now we unlink the original file.
        os.unlink(filename)

    async def ingest(self, filename):
        """Attempt to perform butler ingest for all butlers

        Parameters
        ----------
        filename: `str`
            file to ingest
        """

        # create object containing file information
        image_file = ImageFile(filename)

        # first move the files from the Forwarder staging area
        # to the area where they're staged for the OODS.
        self.stageFiles(filename)

        # for each butler, attempt to ingest the requested file,
        # Success or failure is noted in a message description which
        # is sent via RabbitMQ message back to Archiver, which will
        # send it out via a CSC logevent.
        code = self.SUCCESS
        description = None
        for butler in self.butlers:
            (status_code, status_msg) = self.ingest_file(butler, image_file)
            if status_code == self.FAILURE:
                code = self.FAILURE
            if description is None:
                description = status_msg
            else:
                description = f"{description}; {status_msg}"

        if code == self.SUCCESS and description is None:
            LOGGER.info("Error in processing, no success message was created")
            return

        if self.base_broker_url is not None:
            msg = dict()
            msg['ARCHIVER'] = image_file.archiver
            msg['CAMERA'] = image_file.camera
            msg['FILENAME'] = image_file.filename
            msg['OBSID'] = image_file.obsid
            msg['RAFT'] = image_file.raft
            msg['SENSOR'] = image_file.sensor
            msg['MSG_TYPE'] = 'IMAGE_IN_OODS'
            msg['STATUS_CODE'] = code
            msg['DESCRIPTION'] = description

            LOGGER.info(f"Sending message: {msg}")
            await self.publisher.publish_message(self.PUBLISH_QUEUE, msg)

    def get_locally_staged_filename(self, butlerProxy, full_filename):
        """Construct the full path to the staging area unique to a butler Proxy

        Parameters
        ----------
        butlerProxy: `ButlerProxy`
            A Butler Proxy
        full_filename: `str`
            The full name of forwarder-staged file

        Returns
        -------
        locally_staged_filename: `str`
            The full pathname of to the file in this butler's staging area
        """
        basefile = self.strip_prefix(full_filename, self.forwarder_staging_dir)
        local_staging_dir = butlerProxy.getStagingDirectory()
        locally_staged_filename = os.path.join(local_staging_dir, basefile)
        return locally_staged_filename

    def ingest_file(self, butlerProxy, image):
        """Ingest the file the incoming message requests

        Parameters
        ----------
        butlerProxy: `ButlerProxy`
            proxy for the butler
        image: `ImageFile`
            object that containers information about the image file

        Returns
        -------
        (status_code, status_msg): `int`, `str`
            status code and message to send about what happened
        """

        # get the locally staged file name

        filename = self.get_locally_staged_filename(butlerProxy, os.path.realpath(image.filename))

        # attempt to ingest the file;  if ingests, log that
        # if it does not ingest, move it to a "bad file" directory
        # and log that.
        try:
            butler = butlerProxy.getButler()
            butler.ingest(filename)
            LOGGER.info(f"{butler.getName()}: {image.obsid} {filename} \
                        ingested from {image.camera} by {image.archiver}")
        except Exception as e:
            status_code = self.FAILURE
            status_msg = f"{butler.getName()}: {filename} could not be ingested: {self.extract_cause(e)}"
            LOGGER.exception(status_msg)
            bad_dir = butlerProxy.getBadFileDirectory()
            staging_dir = butlerProxy.getStagingDirectory()

            bad_file_dir = self.create_bad_dirname(bad_dir, staging_dir, filename)
            try:
                LOGGER.info(f"Moving {filename} to {bad_file_dir}")
                shutil.move(filename, bad_file_dir)
            except Exception as fmException:
                LOGGER.info(f"Failed to move {filename} to {bad_file_dir} {fmException}")

            return (status_code, status_msg)

        status_code = self.SUCCESS
        status_msg = f"{butler.getName()}: OBSID {image.obsid} - File {filename} ingested into OODS"

        return (status_code, status_msg)

    async def run_task(self):
        """Keep this object alive
        """
        task_list = []

        task = asyncio.create_task(self.fileQueue.queue_files())
        task_list.append(task)

        task = asyncio.create_task(self.dequeue_and_ingest_files())
        task_list.append(task)

        cleanTasks = self.getButlerCleanTasks()
        for cleanTask in cleanTasks:
            task = asyncio.create_task(cleanTask())
            task_list.append(task)

        return task_list

    async def dequeue_and_ingest_files(self):
        while True:
            filename = await self.fileQueue.dequeue_file()
            await self.ingest(filename)
