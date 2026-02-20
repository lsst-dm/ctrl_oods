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
import os
from unittest.mock import MagicMock, patch

import lsst.utils.tests
from heartbeat_base import HeartbeatBase
from lsst.ctrl.oods.msgQueue import MsgQueue


def consumer_generator():
    testdir = os.path.abspath(os.path.dirname(__file__))

    dataFile = os.path.join(testdir, "data", "kafka_msg.json")

    with open(dataFile, "r") as f:
        message = f.read()

    yield [message]

    while True:
        yield []


class MsgQueueTestCase(HeartbeatBase):

    @patch.object(MsgQueue, "createConsumer", return_value=None)
    async def testMsgQueue(self, MockClass1):

        brokers = ["test_broker"]
        group_id = "test_group"
        topics = "test_topic"
        max_messages = 4

        self.mq = MsgQueue(
            brokers=brokers,
            group_id=group_id,
            topics=topics,
            max_messages=max_messages,
            max_wait_time=1.0,
            group_wait_time=0.1,
            time_to_wait_without_data=0.2,
        )
        self.mq.consumer = MagicMock()
        self.mq.consumer.consume.side_effect = consumer_generator()
        self.mq.consumer.commit = MagicMock()
        self.mq.consumer.close = MagicMock()

        self.mq.start()

        task_list = []
        task_list.append(asyncio.create_task(self.interrupt_me()))
        msg = await self.mq.dequeue_messages()
        self.assertEqual(len(msg), 1)

        try:
            await asyncio.gather(*task_list)
        except Exception:
            for task in task_list:
                task.cancel()

    async def interrupt_me(self):
        await asyncio.sleep(5)
        await self.mq.stop()
        raise RuntimeError("I'm interrupting")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
