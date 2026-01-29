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
import concurrent.futures
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
    """Read messages from Kafka

    Parameters
    ----------
    brokers : `list[str]`
        A list of Kafka brokers
    group_id : `str`
        Kafka group to join
    topics : `list[str]`
        Kafka topics to listen on
    max_messages : `int`
        Maximum number of messages to grab at once from Kafka
    max_wait_time : `float`
        Maximum amount of time in seconds to wait for consumer read
    """

    def __init__(
        self,
        brokers,
        group_id,
        topics,
        max_messages,
        max_wait_time,
    ):
        self.brokers = brokers
        self.group_id = group_id
        self.topics = topics
        self.max_messages = max_messages
        self.max_wait_time = max_wait_time

        self._queue = asyncio.Queue()
        self._reader_task = None
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

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
        self.start()
        self.running = True

    def createConsumer(self, config, topics):
        """Create a Kafka Consumer

        Parameters
        ----------
        config : `dict`
            Kafka configuration for consumer
        topics : `list[str]`
            List of Kafka topics to subscribe to
        """
        self.consumer = Consumer(config)
        self.consumer.subscribe(topics)
        LOGGER.info("subscribed")

    def _get_messages(self):
        """Return up to max_messages at a time from Kafka (blocking call)."""
        LOGGER.debug("getting more messages")
        while self.running:
            try:
                m_list = self.consumer.consume(num_messages=self.max_messages, timeout=self.max_wait_time)
            except Exception as e:
                LOGGER.exception(e)
                raise e

            if len(m_list) == 0:
                continue

            LOGGER.debug(f"message(s) received: {len(m_list)}")
            return m_list
        return None

    async def _reader_loop(self):
        """Task that reads from Kafka and puts messages in the queue."""
        loop = asyncio.get_running_loop()
        LOGGER.info("Starting Kafka reader loop")
        try:
            while self.running:
                try:
                    LOGGER.debug("calling _get_messages")
                    message_list = await loop.run_in_executor(self._executor, self._get_messages)
                    if message_list is None:
                        # running was set to False
                        break
                    LOGGER.info(f"adding {len(message_list)} messages to queue")
                    for msg in message_list:
                        await self._queue.put(msg)
                        LOGGER.debug("Message added to internal queue")
                except asyncio.CancelledError:
                    LOGGER.info("Reader loop cancelled")
                    raise
                except Exception as e:
                    LOGGER.exception(f"Error in reader loop: {e}")
                    # Continue reading despite errors
        finally:
            LOGGER.info("Kafka reader loop stopped")

    def start(self):
        """Start the background reader task.

        This must be called from within an async context (event loop running).
        """
        if self._reader_task is None or self._reader_task.done():
            self._reader_task = asyncio.create_task(self._reader_loop())
            LOGGER.info("Background reader task started")

    async def dequeue_messages(self) -> list:
        """Retrieve messages from the internal queue.

        Returns
        -------
        ret : `list`
            List of messages retrieved from the internal queue.
        """
        messages = []
        try:
            # Wait indefinitely for at least one message
            msg = await self._queue.get()
            messages.append(msg)

            # get the rest
            while True:
                try:
                    msg = self._queue.get_nowait()
                    messages.append(msg)
                except asyncio.QueueEmpty:
                    break

            LOGGER.debug(f"Dequeued {len(messages)} message(s)")
            return messages

        except asyncio.CancelledError:
            LOGGER.info("dequeue_messages task cancelled")
            return messages

    def commit(self, message):
        """Perform Kafka commit on a message

        Parameters
        ----------
        message: Kafka message
            message to commit
        """
        self.consumer.commit(message=message)

    async def stop(self):
        """Shut down the message queue gracefully."""
        LOGGER.info("Stopping MsgQueue")
        self.running = False

        # Cancel the reader task if it's running
        if self._reader_task is not None and not self._reader_task.done():
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass

        # Shutdown the executor
        self._executor.shutdown(wait=False)

        # Close the consumer
        self.consumer.close()
        LOGGER.info("MsgQueue stopped")
