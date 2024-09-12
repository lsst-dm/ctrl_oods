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
import unittest

import lsst.utils.tests
import yaml
from lsst.ctrl.oods.fileIngester import FileIngester

from lsst.daf.butler import Butler, CollectionType
from lsst.daf.butler.tests import MetricsExample, addDatasetType, registerMetricsExample, addDataIdValue


class CleanCollectionsTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        """set up test info, including config dict, and populate
        Butler repo with test data
        """

        config_name = "clean_collections.yaml"

        # create a path to the configuration file

        testdir = os.path.abspath(os.path.dirname(__file__))
        configFile = os.path.join(testdir, "etc", config_name)

        # load the YAML configuration

        with open(configFile, "r") as f:
            self.config = yaml.safe_load(f)

        # extract parts of the ingester configuration
        # and alter the image staging directory to point
        # at the temporary directories created for his test

        ingesterConfig = self.config["ingester"]
        self.imageDir = tempfile.mkdtemp()
        ingesterConfig["imageStagingDirectory"] = self.imageDir

        self.badDir = tempfile.mkdtemp()
        butlerConfig = ingesterConfig["butlers"][0]["butler"]
        butlerConfig["badFileDirectory"] = self.badDir
        self.stagingDir = tempfile.mkdtemp()
        butlerConfig["stagingDirectory"] = self.stagingDir

        self.repoDir = tempfile.mkdtemp()
        Butler.makeRepo(self.repoDir)

        butlerConfig["repoDirectory"] = self.repoDir

        self.clean_collections = butlerConfig["cleanCollections"]

        # Define the run collection
        run_a = "collection_a"
        run_b = "collection_b"
        self.collections = [run_a, run_b]

        # Initialize the Butler
        opts = dict(writeable=True)
        self.butler = Butler(self.repoDir, **opts)

        self.butler.registry.registerCollection("collection_a", CollectionType.RUN)
        self.butler.registry.registerCollection("collection_b", CollectionType.RUN)

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
        shutil.rmtree(self.repoDir, ignore_errors=True)

    def number_of_files(self):
        """count the number of files in the butler

        Returns
        -------
        ret : `int`
            Number of files in all collections
        """
        ref = list(self.butler.registry.queryDatasets(datasetType=..., collections=self.collections))
        return len(ref)

    async def perform_clean(self):
        """Perform butler clean operation to remove old files"""

        ingester = FileIngester(self.config)

        clean_tasks = ingester.getButlerCleanTasks()

        task_list = []
        for clean_task in clean_tasks:
            task = asyncio.create_task(clean_task())
            task_list.append(task)

        # add one more task, whose sole purpose is to interrupt the others by
        # throwing an acception
        task_list.append(asyncio.create_task(self.interrupt_me()))

        # gather all the tasks, until one (the "interrupt_me" task)
        # throws an exception
        try:
            await asyncio.gather(*task_list)
        except Exception:
            for task in task_list:
                task.cancel()

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
            collection = entry["collection"]
            if collection == name:
                entry["filesOlderThan"]["days"] = 0
                entry["filesOlderThan"]["hours"] = 0
                entry["filesOlderThan"]["minutes"] = 0
                entry["filesOlderThan"]["seconds"] = seconds
                return

    async def testCleanTask(self):
        """test clean collections operations"""

        # ensure two files are registered on setUp

        self.assertEqual(self.number_of_files(), 2)

        # set expiration time to 120 seconds
        self.modify_collection_time("collection_a", 120)
        self.modify_collection_time("collection_b", 120)

        # attempt to clean the collections
        await self.perform_clean()

        # both should still be there
        self.assertEqual(self.number_of_files(), 2)

        # set expiration time to 3 seconds in collection_a
        self.modify_collection_time("collection_a", 3)
        await self.perform_clean()

        # should have removed one in collection_a, and
        # kept one in collection_b
        self.assertEqual(self.number_of_files(), 1)

        # set expiration time to 3 seconds in collection_b
        self.modify_collection_time("collection_b", 3)
        await self.perform_clean()

        # should be no more left
        self.assertEqual(self.number_of_files(), 0)

    async def interrupt_me(self):
        await asyncio.sleep(5)
        raise RuntimeError("I'm interrupting")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
