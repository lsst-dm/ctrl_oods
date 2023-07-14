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
from lsst.resources import ResourcePath

from lsst.ctrl.oods.butlerProxy import ButlerProxy
from lsst.ctrl.oods.msgQueue import MsgQueue
from lsst.ctrl.oods.bucketMessage import BucketMessage

LOGGER = logging.getLogger(__name__)


class MsgIngester(object):
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
        self.max_messages = 100

        kafka_settings = self.config.get("kafka")
        if kafka_settings is None:
            raise Exception("section 'kafka' not configured; check configuration file")

        brokers = kafka_settings.get("brokers")
        if brokers is None:
            raise Exception("No brokers configured; check configuration file")

        group_id = kafka_settings.get("group_id")
        if group_id is None:
            raise Exception("No group_id configured; check configuration file")

        topics = kafka_settings.get("topics")
        if topics is None:
            raise Exception("No topics configured; check configuration file")

        max_messages = kafka_settings.get("max_messages")
        if max_messages is None:
            LOGGER.warn(f"max_messages not set; using default of {self.max_messages}")
        else:
            self.max_messages = max_messages

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
                await butler.ingest(butler_file_list[butler])
        except Exception as e:
            LOGGER.warn("Exception: %s", e)

    def run_tasks(self):
        """run tasks to queue files and ingest them"""

        # this is split into two tasks so they can run at slightly different
        # cadences.  We want to gather as many files as we can before we
        # do the ingest
        task = asyncio.create_task(self.msgQueue.queue_files())
        self.tasks.append(task)

        task = asyncio.create_task(self.dequeue_and_ingest_files())
        self.tasks.append(task)

        clean_tasks = self.get_butler_clean_tasks()
        for clean_task in clean_tasks:
            task = asyncio.create_task(clean_task())
            self.tasks.append(task)

        return self.tasks

    def stop_tasks(self):
        LOGGER.info("stopping file scanning and file cleanup")
        for task in self.tasks:
            task.cancel()
        self.tasks = []

    async def dequeue_and_ingest_files(self):
        while True:
            message_list = await self.msgQueue.dequeue_messages()
            # First move the files from the image staging area
            # to the area where they're staged for the OODS.
            # Files staged here so the scanning asyncio routine doesn't
            # queue them twice.
            for message in message_list:
                rps = self._gather_all_resource_paths(message)
                await self.ingest(rps)
                self.msgQueue.commit(message=message)

    def _gather_all_resource_paths(self, m):
        # extract all urls within this message
        msg = BucketMessage(m)

        rp_list = list()
        for url in msg.extract_all_urls():
            rp = ResourcePath(url)
            rp_list.append(rp)
        return rp_list
