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

        self._msg_actions = {'AT_FILE_INGEST_REQUEST': self.ingest_file}

        self.scanner = DirectoryScanner(config)

        if 'baseBrokerAddr' in config:
            self.base_broker_url = config["baseBrokerAddr"]
        else:
            self.base_broker_url = None

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

    async def start_comm(self):
        self.consumer = Consumer(self.base_broker_url, None, "at_publish_to_oods", self.on_message)
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

    async def ingest_file(self, msg):
        camera = msg['CAMERA']
        obsid = msg['OBSID']
        filename = msg['FILENAME']
        archiver = msg['ARCHIVER']

        try:
            self.butler.ingest(filename)
            LOGGER.info(f"{obsid} {filename} ingested from {camera} by {archiver}")
        except Exception as e:
            err = f"{filename} could not be ingested.  Moving to {self.bad_file_dir}: {e}"
            LOGGER.info(err)
            shutil.move(filename, self.bad_file_dir)
            if self.base_broker_url is not None:
                d = dict(msg)
                d['MSG_TYPE'] = 'IMAGE_IN_OODS'
                d['STATUS_CODE'] = 1
                d['DESCRIPTION'] = err
                await self.publisher.publish_message("oods_publish_to_at", d)
            return

        if self.base_broker_url is not None:
            d = dict(msg)
            d['MSG_TYPE'] = 'IMAGE_IN_OODS'
            d['STATUS_CODE'] = 0
            d['DESCRIPTION'] = f"OBSID {obsid}: File {filename} ingested into OODS"
            await self.publisher.publish_message("oods_publish_to_at", d)

        os.remove(filename)

    async def run_task(self):
        # wait, to keep the object alive
        while True:
            await asyncio.sleep(60)
