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
import socket
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
                  'client.id': socket.gethostname,
                  'group.id': self.group_id,
                  'auto.offset.reset': 'earliest',
                  'enable.auto.commit': True}
        self.consumer = Consumer(config)
        self.consumer.subscribe(topics)

    async def queue_files(self):
        """Queue all files in messages on the subscribed topics
        """
        loop = asyncio.get_running_loop()
        try:
            # now, add all the currently known files to the queue
            while True:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    message_list = await loop.run_in_executor(pool, self.get_messages())

                if message_list:
                    async with self.condition:
                        self.msgList.extend(message_list)
                        self.condition.notify_all()
        finally:
            LOGGER.info("consumer unsubscribing")
            self.consumer.unsubscribe()

    def get_messages(self):
        """Return up to max_messages at a time from Kafka

        Parameters
        ----------
        max_messages: `int`
            maximum number of messages to retrieve at a time
        """
        # idea here is to not busy loop.  Wait for an initial
        # message, and after we get one, try and get the rest.
        # If there other messages, retrieve up to 'max_messages'.
        # If not, read as many as you can before the timeout,
        # and then return with what we could get.
        #
        m = self.consumer.consume(num_messages=1)
        return_list = self._extract_all_urls(m)

        if self.max_messages == 1:
            return return_list

        # we we'd like to get more messages, so grab as many as we can
        # before timing out.
        mlist = self.consumer(num_messages=self.max_messages-1, timeout=0.5)

        # if we didn't get any additional messages, just return
        if len(mlist) == 0:
            return return_list

        return_list.extend(mlist)
        return return_list

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