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
import shutil
import tempfile
import time
from unittest.mock import MagicMock

import lsst.utils.tests
from heartbeat_base import HeartbeatBase
from lsst.ctrl.oods.bucketMessage import BucketMessage
from lsst.ctrl.oods.msgIngester import MsgIngester
from lsst.ctrl.oods.msgQueue import MsgQueue
from lsst.ctrl.oods.oods_config import OODSConfig
from lsst.daf.butler import Butler


class S3AuxtelIngesterTestCase(HeartbeatBase):
    """Test S3 Butler Ingest"""

    def createConfig(self, config_name):
        """create a standard configuration file, using temporary directories

        Parameters
        ----------
        config_name: `str`
            name of the OODS configuration file
        fits_name: `str`
            name of the test FITS file

        Returns
        -------
        config: `dict`
            An OODS configuration to use for testing
        """

        # create a path to the configuration file

        testdir = os.path.abspath(os.path.dirname(__file__))
        config_file = os.path.join(testdir, "etc", config_name)

        # load the YAML configuration

        config = OODSConfig.load(config_file)

        ingester_config = config.message_ingester
        butler_config = ingester_config.butler

        self.repo_dir = tempfile.mkdtemp()
        Butler.makeRepo(self.repo_dir)
        butler_config.repo_directory = self.repo_dir

        return config

    def tearDown(self):
        shutil.rmtree(self.repo_dir, ignore_errors=True)

    def returnVal(self, num_messages, timeout):
        if self.attempts == 0:
            self.attempts += 1
            return [self.fakeKafkaMessage]
        time.sleep(1)
        return []

    async def testAuxTelIngest(self):
        """test ingesting an auxtel file"""
        test_dir = os.path.abspath(os.path.dirname(__file__))
        msg_file = os.path.join(test_dir, "data", "kafka_msg.json")
        with open(msg_file, "r") as f:
            message = f.read()

        fits_name = "2020032700020-det000.fits.fz"
        file_url = f'file://{os.path.join(test_dir, "data", fits_name)}'

        fits_name = "bad.fits.fz"
        bad_file_url = f'file://{os.path.join(test_dir, "data", fits_name)}'

        self.attempts = 0
        self.fakeKafkaMessage = MagicMock()
        self.fakeKafkaMessage.value = MagicMock(return_value=message)

        MsgQueue.createConsumer = MagicMock()
        MsgQueue.consumer = MagicMock()
        MsgQueue.consumer.consume = MagicMock(side_effect=self.returnVal)

        BucketMessage.extract_urls = MagicMock(return_value=[file_url, bad_file_url])

        config = self.createConfig("ingest_auxtel_s3.yaml")

        # create a MsgIngester
        ingester = MsgIngester(config, None)

        task_list = ingester.run_tasks()
        # add one more task, whose sole purpose is to interrupt the others by
        # throwing an acception
        task_list.append(asyncio.create_task(self.interrupt_me()))

        # gather all the tasks, until one (the "interrupt_me" task)
        # throws an exception
        try:
            await asyncio.gather(*task_list)
        except Exception:
            await ingester.stop_tasks()
            for task in task_list:
                task.cancel()

    async def interrupt_me(self):
        await asyncio.sleep(5)
        raise Exception("I'm interrupting")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
