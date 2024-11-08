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

from lsst.ctrl.oods.bucketMessage import BucketMessage
from lsst.ctrl.oods.butlerProxy import ButlerProxy
from lsst.ctrl.oods.msgQueue import MsgQueue
from lsst.resources import ResourcePath

LOGGER = logging.getLogger(__name__)


class MsgIngester(object):
    """Ingest files into the butler specified in the configuration.

    Parameters
    ----------
    config: `dict`
        A butler configuration dictionary
    """

    def __init__(self, mainConfig, csc=None):
        self.SUCCESS = 0
        self.FAILURE = 1
        self.config = mainConfig["ingester"]
        self.max_messages = 1

        kafka_settings = self.config.get("kafka")
        if kafka_settings is None:
            raise ValueError("section 'kafka' not configured; check configuration file")

        brokers = kafka_settings.get("brokers")
        if brokers is None:
            raise ValueError("No brokers configured; check configuration file")

        group_id = kafka_settings.get("group_id")
        if group_id is None:
            raise ValueError("No group_id configured; check configuration file")

        topics = kafka_settings.get("topics")
        if topics is None:
            raise ValueError("No topics configured; check configuration file")

        max_messages = kafka_settings.get("max_messages")
        if max_messages is None:
            LOGGER.warn(f"max_messages not set; using default of {self.max_messages}")
        else:
            self.max_messages = max_messages
            LOGGER.info(f"max_messages set to {self.max_messages}")

        LOGGER.info("listening to brokers %s", brokers)
        LOGGER.info("listening on topics %s", topics)
        self.msgQueue = MsgQueue(brokers, group_id, topics, self.max_messages)

        butler_configs = self.config["butlers"]
        if len(butler_configs) == 0:
            raise Exception("No Butlers configured; check configuration file")

        self.butlers = []
        for butler_config in butler_configs:
            butler = ButlerProxy(butler_config["butler"], csc)
            self.butlers.append(butler)

        self.tasks = []
        self.dequeue_task = None

    def get_butler_clean_tasks(self):
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
                await butler.ingest(butler_file_list)
        except Exception as e:
            LOGGER.warning("Exception: %s", e)

    def run_tasks(self):
        """run tasks to queue files and ingest them"""

        # this is split into two tasks so they can run at slightly different
        # cadences.  We want to gather as many files as we can before we
        # do the ingest

        task = asyncio.create_task(self.dequeue_and_ingest_files())
        self.tasks.append(task)

        clean_tasks = self.get_butler_clean_tasks()
        for clean_task in clean_tasks:
            task = asyncio.create_task(clean_task())
            self.tasks.append(task)

        return self.tasks

    def stop_tasks(self):
        self.running = False
        self.msgQueue.stop()
        for task in self.tasks:
            task.cancel()
        self.tasks = []

    async def dequeue_and_ingest_files(self):
        self.running = True
        while self.running:
            message_list = await self.msgQueue.dequeue_messages()
            resources = []
            for m in message_list:
                rps = self._gather_all_resource_paths(m)
                resources.extend(rps)
            await self.ingest(resources)

            # XXX - commit on success, failure, or metadata_failure
            self.msgQueue.commit(message=message_list[-1])

    def _gather_all_resource_paths(self, m):
        # extract all urls within this message
        msg = BucketMessage(m.value())

        rp_list = [ResourcePath(url) for url in msg.extract_urls()]

        return rp_list
