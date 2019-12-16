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
from lsst.ctrl.oods.directoryScanner import DirectoryScanner
from importlib import import_module


class FileIngester(object):
    """Ingest files into the butler specified in the configuration.
    Files must be removed from the directory as part of the ingest
    or there will be an attempt to ingest them again later.
    """

    def __init__(self, logger, config):
        self.logger = logger
        self.config = config

        self._msg_actions = {'AT_FILE_INGEST_REQUEST', self.ingest_file}

        self.scanner = DirectoryScanner(config)

        butlerConfig = config["butler"]

        classConfig = butlerConfig["class"]

        # create the butler
        importFile = classConfig["import"]
        name = classConfig["name"]

        mod = import_module(importFile)
        butlerClass = getattr(mod, name)

        self.repo = butlerConfig["repoDirectory"]
        self.bad_file_dir  = butlerConfig["badFileDirectory"])
        self.butler = butlerClass(logger, self.repo)
        self.base_broker_url = butlerConfig["baseBrokerAddr"] 

        self.consumer = Consumer(self.base_broker_url, None, "at_publish_to_oods", self.on_message)
        await self.consumer.start()

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
            task = asyncio.create_task(handler(body))
        else:
            self.logger.info(f"unknown message type: {msg_type}")

    async def ingest_file(self, msg):
        camera = msg['CAMERA']
        obsid = msg['OBSID']
        filename = msg['FILENAME']
        archiver = msg['ARCHIVER']

        try:
            self.butler.ingest(filename)
            self.logger.info(f"{osbid} {filename} ingested from {camera} by {archiver}")
        except Exception:
            self.logger.info(f"{filename} could not be ingested.  Moving to {self.bad_file_dir}")
            shutil.move(filename, self.bad_file_dir)
            return

        d = dict(msg)
        d['MSG_TYPE'] = 'FILE_INGESTED_BY_OODS'
        self.publisher.publish_message("oods_publish_to_at", d)
        

    async def run_task(self):
        seconds = TimeInterval.calculateTotalSeconds(interval)
        while True:
            self.scan_ingest()
            await asyncio.sleep(seconds)

    def scan_ingest(self):
        """Scan to get the files, and ingest them in batches.
        """
        files = self.scanner.getAllFiles()
        for filename in files:
            try:
                self.butler.ingest(filename)
            except Exception:
                self.logger.info(f"{filename} could not be ingested.  Moving to {self.bad_file_dir}")
                shutil.move(filename, self.bad_file_dir)
                return
            self.publisher.publish_message(
