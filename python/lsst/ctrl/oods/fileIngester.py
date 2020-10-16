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
from lsst.ctrl.oods.butlerProxy import ButlerProxy
from lsst.ctrl.oods.directoryScanner import DirectoryScanner
from lsst.dm.csc.base.consumer import Consumer
from lsst.dm.csc.base.publisher import Publisher

LOGGER = logging.getLogger(__name__)


class FileIngester(object):
    """Ingest files into the butler specified in the configuration.
    Files must be removed from the directory as part of the ingest
    or there will be an attempt to ingest them again later.
    """

    def __init__(self, config):
        self.SUCCESS = 0
        self.FAILURE = 1
        self.config = config

        self.forwarder_staging_dir = config["forwarderStagingDirectory"]
        FILE_INGEST_REQUEST = config["FILE_INGEST_REQUEST"]
        self._msg_actions = {FILE_INGEST_REQUEST: self.ingest}

        self.scanner = DirectoryScanner([self.forwarder_staging_dir])

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

        self.CONSUME_QUEUE = config['CONSUME_QUEUE']
        self.PUBLISH_QUEUE = config['PUBLISH_QUEUE']

        if self.base_broker_url is not None:
            asyncio.create_task(self.start_comm())

    async def start_comm(self):
        self.consumer = Consumer(self.base_broker_url, None, self.CONSUME_QUEUE, self.on_message)
        self.consumer.start()

        self.publisher = Publisher(self.base_broker_url)
        await self.publisher.start()

    def on_message(self, ch, method, properties, body):
        """ Route the message to the proper handler
        """
        msg_type = body['MSG_TYPE']
        ch.basic_ack(method.delivery_tag)
        if msg_type in self._msg_actions:
            handler = self._msg_actions.get(msg_type)
            asyncio.create_task(handler(body))
        else:
            LOGGER.info(f"unknown message type: {msg_type}")

    def extract_cause(self, e):
        if e.__cause__ is None:
            return None
        cause = self.extract_cause(e.__cause__)
        if cause is None:
            return f"{str(e.__cause__)}"
        else:
            return f"{str(e.__cause__)};  {cause}"

    def create_bad_dirname(self, bad_dir_root, staging_dir_root, original):
        # strip the original directory location, except for the date
        newfile = self.strip_prefix(original, staging_dir_root)

        # split into subdir and filename
        head, tail = os.path.split(newfile)

        # create subdirectory path name for directory with date
        newdir = os.path.join(bad_dir_root, head)

        # create the directory, and hand the name back
        os.makedirs(newdir, exist_ok=True)

        return newdir

    def strip_prefix(self, name, prefix):
        ret = name
        if name.startswith(prefix):
            ret = name[len(prefix):].lstrip('/')
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

    def stageFiles(self, msg):
        """ stage the files to their butler staging areas
        and remove the original file
        """
        filename = os.path.realpath(msg['FILENAME'])

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

    async def ingest(self, msg):

        self.stageFiles(msg)

        code = self.SUCCESS
        description = None
        for butler in self.butlers:
            (status_code, status_msg) = self.ingest_file(butler, msg)
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
            LOGGER.info(f"Sending message: {msg}")
            d = dict(msg)
            d['MSG_TYPE'] = 'IMAGE_IN_OODS'
            d['STATUS_CODE'] = code
            d['DESCRIPTION'] = description
            await self.publisher.publish_message(self.PUBLISH_QUEUE, d)

    def get_locally_staged_filename(self, butlerProxy, full_filename):
        basefile = self.strip_prefix(full_filename, self.forwarder_staging_dir)
        local_staging_dir = butlerProxy.getStagingDirectory()
        locally_staged_filename = os.path.join(local_staging_dir, basefile)
        return locally_staged_filename

    def ingest_file(self, butlerProxy, msg):
        camera = msg['CAMERA']
        obsid = msg['OBSID']
        filename = self.get_locally_staged_filename(butlerProxy, os.path.realpath(msg['FILENAME']))
        archiver = msg['ARCHIVER']

        try:
            butler = butlerProxy.getButler()
            butler.ingest(filename)
            LOGGER.info(f"{butler.getName()}: {obsid} {filename} ingested from {camera} by {archiver}")
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
        status_msg = f"{butler.getName()}: OBSID {obsid} - File {filename} ingested into OODS"

        return (status_code, status_msg)

    async def run_task(self):
        # wait, to keep the object alive
        while True:
            await asyncio.sleep(60)
