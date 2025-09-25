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
import shutil
import tempfile

import lsst.utils.tests
from heartbeat_base import HeartbeatBase
from lsst.ctrl.oods.oods_config import OODSConfig
from lsst.daf.butler import Butler, CollectionType
from lsst.daf.butler.tests import MetricsExample, addDataIdValue, addDatasetType, registerMetricsExample


class CleanCollectionsTestCase(HeartbeatBase):

    def setUp(self):
        """set up test info, including config dict, and populate
        Butler repo with test data
        """

        config_name = "clean_collections.yaml"

        # create a path to the configuration file

        testdir = os.path.abspath(os.path.dirname(__file__))
        config_file = os.path.join(testdir, "etc", config_name)

        # load the YAML configuration

        self.config = OODSConfig.load(config_file)

        # extract parts of the ingester configuration
        # and alter the image staging directory to point
        # at the temporary directories created for his test

        ingester_config = self.config.file_ingester
        self.image_dir = tempfile.mkdtemp()
        ingester_config.image_staging_directory = self.image_dir

        self.bad_dir = tempfile.mkdtemp()
        butler_config = ingester_config.butler
        ingester_config.bad_file_directory = self.bad_dir
        self.stagingDir = tempfile.mkdtemp()
        ingester_config.staging_directory = self.stagingDir

        self.repo_dir = tempfile.mkdtemp()
        Butler.makeRepo(self.repo_dir)

        butler_config.repo_directory = self.repo_dir

        self.clean_collections = butler_config.collection_cleaner.collections_to_clean
        print(f"{self.clean_collections=}")

        # Define the run collection
        run_a = "collection_a"
        run_b = "collection_b"
        self.collections = [run_a, run_b]

        # Initialize the Butler
        opts = dict(writeable=True)
        self.butler = Butler(self.repo_dir, **opts)

        self.butler.registry.registerCollection(run_a, CollectionType.RUN)
        self.butler.registry.registerCollection(run_b, CollectionType.RUN)

        registerMetricsExample(self.butler)
        image_data = MetricsExample(summary={"answer": 42, "question": "unknown"})
        image_data2 = MetricsExample(summary={"answer": 42, "question": "unknown"})

        # Define dataset type and data ID
        data_id_1 = {"instrument": "notACam", "visit": 12345, "detector": 1}
        data_id_2 = {"instrument": "notACam", "visit": 12346, "detector": 1}

        addDataIdValue(self.butler, "instrument", "notACam")
        addDatasetType(self.butler, "DataType1", {"instrument"}, "StructuredDataNoComponents")
        addDatasetType(self.butler, "DataType2", {"instrument"}, "StructuredDataNoComponents")

        # Write the image to the Butler under the run collection
        self.butler.put(image_data, "DataType1", dataId=data_id_1, run=run_a)
        self.butler.put(image_data2, "DataType2", dataId=data_id_2, run=run_b)

    def tearDown(self):
        """Remove butler repo directory"""
        shutil.rmtree(self.repo_dir, ignore_errors=True)

    def number_of_datasets(self):
        """count the number of files in the butler

        Returns
        -------
        ret : `int`
            Number of files in all collections
        """
        ref = list(self.butler.registry.queryDatasets(datasetType=..., collections=self.collections))
        return len(ref)

    def modify_collection_time(self, name, seconds):
        """Modify the time interval in the config dict
        to change the time at which files are reaped

        Parameters
        ----------
        name : `str`
            Collection name
        seconds : `int`
            number of seconds after which files will reaped
        """
        for entry in self.clean_collections:
            collection = entry.collection
            if collection == name:
                entry.files_older_than.days = 0
                entry.files_older_than.hours = 0
                entry.files_older_than.minutes = 0
                entry.files_older_than.seconds = seconds
                return

    async def testCleanCask(self):
        """test clean collections operations"""

        # ensure two files are registered on setUp

        self.assertEqual(self.number_of_datasets(), 2)

        # set expiration time to 120 seconds
        self.modify_collection_time("collection_a", 120)
        self.modify_collection_time("collection_b", 120)

        # attempt to clean the collections
        await self.perform_clean(self.config)

        # both should still be there
        self.assertEqual(self.number_of_datasets(), 2)

        # set expiration time to 3 seconds in collection_a
        self.modify_collection_time("collection_a", 1)
        await self.perform_clean(self.config)

        # should have removed one in collection_a, and
        # kept one in collection_b
        self.assertEqual(self.number_of_datasets(), 1)

        # set expiration time to 3 seconds in collection_b
        self.modify_collection_time("collection_b", 1)
        await self.perform_clean(self.config)

        # should be no more left
        self.assertEqual(self.number_of_datasets(), 0)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
