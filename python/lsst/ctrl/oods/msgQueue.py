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
import concurrent
import logging
from confluent_kafka import Consumer

LOGGER = logging.getLogger(__name__)


class MsgQueue(object):
    """Report on new messages

    Parameters
    ----------
    config: `dict`
        configuration dictionary for a consumer
    topics: `list`
        The topics to listen on
    """

    def __init__(self, brokers, group_id, topics, max_messages):
        self.brokers = brokers
        self.group_id = group_id
        self.topics = topics
        self.max_messages = max_messages

        self.msgList = list()
        self.condition = asyncio.Condition()

        config = {'bootstrap.servers': ",".join(self.brokers),
                  'group.id': self.group_id,
                  'auto.offset.reset': 'earliest'}
        self.consumer = Consumer(config)
        self.consumer.subscribe(topics)

    async def queue_files(self):
        """Queue all files in messages on the subscribed topics
        """
        loop = asyncio.get_running_loop()
        # now, add all the currently known files to the queue
        while True:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                message_list = await loop.run_in_executor(pool, self.get_messages)

            if message_list:
                async with self.condition:
                    self.msgList.extend(message_list)
                    self.condition.notify_all()

    def get_messages(self):
        """Return up to max_messages at a time from Kafka
        """
        while True:
            m_list = self.consumer.consume(num_messages=self.max_messages, timeout=0.5)

            if len(m_list) == 0:
                continue
            return m_list

    async def dequeue_messages(self):
        """Return all of the messages retrieved so far"""
        # get a list of messages, clear the msgList
        async with self.condition:
            await self.condition.wait()
            message_list = list(self.msgList)
            self.msgList.clear()
        return message_list

    def commit(self, message):
        self.consumer.commit(message=message)

    def stop(self):
        self.consumer.close()
