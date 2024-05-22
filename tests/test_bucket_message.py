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

import os
import os.path
import lsst.utils.tests
from lsst.ctrl.oods.bucketMessage import BucketMessage


class BucketMessageTestCase(lsst.utils.tests.TestCase):
    """Test Bucket Message"""

    def createBucketMessage(self, msg_file):

        # create a path to the test directory

        testdir = os.path.abspath(os.path.dirname(__file__))

        # path to the data file

        dataFile = os.path.join(testdir, "data", msg_file)

        # load the YAML configuration

        with open(dataFile, "r") as f:
            message = f.read()

        bucket_message = BucketMessage(message)
        return bucket_message

    def testBucketMessage(self):
        bucket_message = self.createBucketMessage("kafka_msg.json")
        url_list = list()
        for url in bucket_message.extract_urls():
            url_list.append(url)

        self.assertEqual(len(url_list), 1)
        self.assertEqual(url_list[0], 
            "s3://rubin-pp/HSC/73/2023061400090/0/6140090/HSC-Z/HSC-2023061400090-0-6140090-HSC-Z-73.fz")

    def testBadBucketMessage(self):
        bucket_message = self.createBucketMessage("bad_kafka_msg.json")

        with self.assertRaises(Exception):
            assert next(bucket_message.extract_urls(), None) is None


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
