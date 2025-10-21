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
import os

from confluent_kafka import Consumer

LOGGER = logging.getLogger(__name__)

SECURITY_PROTOCOL = "SASL_PLAINTEXT"
SASL_MECHANISM = "SCRAM-SHA-512"

USERNAME_KEY = "LSST_KAFKA_SECURITY_USERNAME"
PASSWORD_KEY = "LSST_KAFKA_SECURITY_PASSWORD"
PROTOCOL_KEY = "LSST_KAFKA_SECURITY_PROTOCOL"
MECHANISM_KEY = "LSST_KAFKA_SECURITY_MECHANISM"


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

        username = os.environ.get(USERNAME_KEY, None)
        password = os.environ.get(PASSWORD_KEY, None)
        mechanism = os.environ.get(MECHANISM_KEY, SASL_MECHANISM)
        protocol = os.environ.get(PROTOCOL_KEY, SECURITY_PROTOCOL)

        use_auth = True
        if username is None:
            LOGGER.info(f"{USERNAME_KEY} has not been set.")
            use_auth = False
        if password is None:
            LOGGER.info(f"{PASSWORD_KEY} has not been set.")
            use_auth = False

        if use_auth:
            LOGGER.info(f"{MECHANISM_KEY} set to {mechanism}")
            LOGGER.info(f"{PROTOCOL_KEY} set to {protocol}")
            config = {
                "bootstrap.servers": ",".join(self.brokers),
                "client.id": "oods message dequeue",
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "security.protocol": protocol,
                "sasl.mechanism": mechanism,
                "sasl.username": username,
                "sasl.password": password,
            }
        else:
            LOGGER.info("Defaulting to no authentication to Kafka")
            config = {
                "bootstrap.servers": ",".join(self.brokers),
                "client.id": "oods message dequeue",
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
            }

        # note: this is done because mocking a cimpl is...tricky
        self.createConsumer(config, topics)
        self.running = True

    def createConsumer(self, config, topics):
        """Create a Kafka Consumer"""
        self.consumer = Consumer(config)
        self.consumer.subscribe(topics)
        LOGGER.info("subscribed")

    def _get_messages(self):
        """Return up to max_messages at a time from Kafka"""
        LOGGER.debug("getting more messages")
        while self.running:
            try:
                m_list = self.consumer.consume(num_messages=self.max_messages, timeout=1.0)
            except Exception as e:
                LOGGER.exception(e)
                raise e
            if len(m_list) == 0:
                continue
            LOGGER.debug("message(s) received")
            return m_list

    async def dequeue_messages(self):
        """Retrieve messages"""
        try:
            loop = asyncio.get_running_loop()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                message_list = await loop.run_in_executor(pool, self._get_messages)
            return message_list
        except asyncio.exceptions.CancelledError:
            LOGGER.info("get messages task cancelled")

    def commit(self, message):
        """Perform Kafka commit a message

        Parameters
        ----------
        message: Kafka message
            message to commit
        """
        self.consumer.commit(message=message)

    def stop(self):
        """shut down"""
        self.running = False
        self.consumer.close()
