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
from lsst.ctrl.oods.directoryScanner import DirectoryScanner
from lsst.dm.csc.base.consumer import Consumer
from lsst.dm.csc.base.publisher import Publisher
from importlib import import_module

LOGGER = logging.getLogger(__name__)


class FileIngester(object):
    """Ingest files into the butler specified in the configuration.
    Files must be removed from the directory as part of the ingest
    or there will be an attempt to ingest them again later.
    """

    def __init__(self, config):
        self.config = config

        FILE_INGEST_REQUEST = config["FILE_INGEST_REQUEST"]
        self._msg_actions = {FILE_INGEST_REQUEST: self.ingest_file}

        self.scanner = DirectoryScanner(config)

        if 'baseBrokerAddr' in config:
            self.base_broker_url = config["baseBrokerAddr"]
        else:
            self.base_broker_url = None

        self.directories = config["directories"]
        self.bad_file_dir = config["badFileDirectory"]

        butlerConfig = config["butler"]

        classConfig = butlerConfig["class"]

        # create the butler
        importFile = classConfig["import"]
        name = classConfig["name"]

        mod = import_module(importFile)
        butlerClass = getattr(mod, name)

        self.repo = butlerConfig["repoDirectory"]
        self.butler = butlerClass(self.repo)
        if self.base_broker_url is not None:
            asyncio.create_task(self.start_comm())

        self.CONSUME_QUEUE = config['CONSUME_QUEUE']
        self.PUBLISH_QUEUE = config['PUBLISH_QUEUE']

    async def start_comm(self):
        self.consumer = Consumer(self.base_broker_url, None, self.CONSUME_QUEUE, self.on_message)
        self.consumer.start()

        self.publisher = Publisher(self.base_broker_url)
        await self.publisher.start()

    def on_message(self, ch, method, properties, body):
        """ Route the message to the proper handler
        """
        msg_type = body['MSG_TYPE']
        LOGGER.info("TEMP: received message {body}")
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

    def create_bad_dirname(self, original):
        for dirname in self.directories:
            if original.startswith(dirname):
                # strip the original directory location, except for the date
                newfile = original.lstrip(dirname)
                # split into date and filename
                head, tail = os.path.split(newfile)
                # create subdirectory path name for self.bad_file_dir with date
                newdir = os.path.join(self.bad_file_dir, head)
                # create the directory, and hand the name back
                os.makedirs(newdir, exist_ok=True)
                return newdir
        return None

    async def ingest_file(self, msg):
        camera = msg['CAMERA']
        obsid = msg['OBSID']
        filename = os.path.realpath(msg['FILENAME'])
        archiver = msg['ARCHIVER']

        try:
            self.butler.ingest(filename)
            LOGGER.info(f"{obsid} {filename} ingested from {camera} by {archiver}")
        except Exception as e:
            err = f"{filename} could not be ingested.  Moving to {self.bad_file_dir}: {self.extract_cause(e)}"
            LOGGER.exception(err)
            bad_file_dir = self.create_bad_dirname(filename)
            try:
                shutil.move(filename, bad_file_dir)
            except Exception as fmException:
                LOGGER.info(f"Failed to move {filename} to {bad_file_dir} {fmException}")

            if self.base_broker_url is not None:
                d = dict(msg)
                d['MSG_TYPE'] = 'IMAGE_IN_OODS'
                d['STATUS_CODE'] = 1
                d['DESCRIPTION'] = err
                await self.publisher.publish_message(self.PUBLISH_QUEUE, d)
            return

        if self.base_broker_url is not None:
            d = dict(msg)
            d['MSG_TYPE'] = 'IMAGE_IN_OODS'
            d['STATUS_CODE'] = 0
            d['DESCRIPTION'] = f"OBSID {obsid}: File {filename} ingested into OODS"
            await self.publisher.publish_message(self.PUBLISH_QUEUE, d)

    async def run_task(self):
        # wait, to keep the object alive
        while True:
            await asyncio.sleep(60)
