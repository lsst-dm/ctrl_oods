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
import os
import tempfile
from pathlib import PurePath
from shutil import copyfile

import lsst.utils.tests
import yaml
from heartbeat_base import HeartbeatBase
from lsst.ctrl.oods.directoryScanner import DirectoryScanner
from lsst.ctrl.oods.fileIngester import FileIngester
from lsst.daf.butler import Butler
from lsst.daf.butler.registry import CollectionType


class TaggingTestCase(HeartbeatBase):
    """Test TAGGED deletion

    This test simulates the OODS asyncio cleanup and a secondary associate
    (tagging) and disassociate (untagging) of files, to see be sure the OODS
    cleanup behaves properly:

    1) When a dataset is TAGGED, will not be deleted, and the OODS cleaup
       routine bypasses it.
    2) When a dataset is not TAGGED, it can be deleted, and the OODS cleaup
       routine removes it.

    The test simulates this by gathering all ingester asyncio tasks, plus unit
    test tasks to associate, disassociate and check for a dataset's existance
    (or non-existance), and an interrupt task. Because the ingester cleanup
    tasks run at predetermined intervals, the various checks in the unit tests
    are also set up as asyncio tasks, waiting an appropriate amount of time
    for the ingest cleanup routines to run. The code below might be a little
    hard to follow, so here's a description of how the tasks run in this unit
    test.

    1) File ingest runs
    2) Ingester cleanup runs, and nothing happens because the file hasn't
       expired yet, and goes to sleep
    3) Task to associate the dataset runs, and tags the file, and completes
    4) Ingester cleanup task runs, finds an expired file, but doesn't deleted
       it because it's TAGGED, and goes back to sleep
    5) Task disassociate the dataset runs, and removes the TAGGED designation,
        and completes
    6) Task to check that the file runs, affirming it's still on disk, and
       completes
    7) Ingester cleanup task runs, finds an expired file, and deletes it,
       since it's not TAGGED anymore and goes back to sleep
    8) Task to check that the file runs, affirming it is not longer on disk,
       and completes
    7) Ingester cleanup task runs, finds nothing to do, and goes back to sleep
    9) Task to interrupt all tasks runs, causes an exception on purpose, which
       interrupts that gather() causing all tasks to stop.  End of unit test
    """

    async def stage(self):
        """stage test data and set up ingester tasks

        Returns
        -------
        staged_file : `str`
            Path to file to after it has been staged for ingestion
        task_list : `list`
            A list of tasks to run for this butler ingester (ingest, cleanup)
        """
        # fits file to ingest
        fits_name = "3019053000001-R22-S00-det000.fits.fz"

        # configuration file to load
        config_name = "ingest_tag_test.yaml"

        # create a path to the configuration file

        testdir = os.path.abspath(os.path.dirname(__file__))
        config_file = os.path.join(testdir, "etc", config_name)

        # path to the FITS file to ingest

        fitsFile = os.path.join(testdir, "data", fits_name)

        # load the YAML configuration

        with open(config_file, "r") as f:
            self.config = yaml.safe_load(f)

        # extract parts of the ingester configuration
        # and alter the image staging directory to point
        # at the temporary directories created for his test

        ingesterConfig = self.config["ingester"]
        self.imageStagingDir = tempfile.mkdtemp()
        ingesterConfig["imageStagingDirectory"] = self.imageStagingDir

        self.badDir = tempfile.mkdtemp()
        butlerConfig = ingesterConfig["butlers"][0]["butler"]
        butlerConfig["badFileDirectory"] = self.badDir
        self.stagingDirectory = tempfile.mkdtemp()
        butlerConfig["stagingDirectory"] = self.stagingDirectory

        self.repoDir = tempfile.mkdtemp()
        Butler.makeRepo(self.repoDir)
        butlerConfig["repoDirectory"] = self.repoDir

        self.collections = butlerConfig["collections"]

        # copy the FITS file to it's test location

        subDir = tempfile.mkdtemp(dir=self.imageStagingDir)
        self.destFile = os.path.join(subDir, fits_name)
        copyfile(fitsFile, self.destFile)

        # setup directory to scan for files in the image staging directory
        # and ensure one file is there
        ingesterConfig = self.config["ingester"]
        image_staging_dir = ingesterConfig["imageStagingDirectory"]
        scanner = DirectoryScanner([image_staging_dir])
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        # check to see that the file is there before ingestion
        self.assertTrue(os.path.exists(self.destFile))

        # stage the files
        ingester = FileIngester(self.config)
        staged_files = ingester.stageFiles([self.destFile])
        await ingester.ingest(staged_files)

        # make sure staging area is now empty
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        # Check to see that the file was ingested.
        # Recall that files start in the image staging area, and are
        # moved to the OODS staging area before ingestion. On "direct"
        # ingestion, this is where the file is located.  This is a check
        # to be sure that happened.
        name = self.strip_prefix(self.destFile, self.imageStagingDir)
        staged_file = os.path.join(self.stagingDirectory, name)
        self.assertTrue(os.path.exists(staged_file))

        # this file should now not exist
        self.assertFalse(os.path.exists(self.destFile))

        return staged_file

    async def testTaggedFileTestCase(self):
        """Test associating and disassociating of datasets

        This test creates async tasks to simulate the OODS in operation
        when outside actors associate and disassociate datasets.  Associated
        files are not deleted, even if expired. When these files are
        later disassociated, they are cleaned up.
        """
        exposure = "3019053000001"
        file_to_ingest = await self.stage()

        await self.perform_clean(self.config)
        await self.associate_file(exposure)
        self.assertTrue(os.path.exists(file_to_ingest))

        await self.disassociate_file(exposure)
        await self.perform_clean(self.config)
        self.assertFalse(os.path.exists(file_to_ingest))

    async def associate_file(self, exposure):
        """add exposure from TAGGED collection

        Parameters
        ----------
        exposure : `str`
            the name of the exposure to add
        """
        # wait for the file to be ingested
        logging.info("about to associate file")

        # now that the file has been ingested, create a butler
        # and tag the file
        butler = Butler(self.repoDir, writeable=True)

        # register the new collection
        butler.registry.registerCollection("test_collection", CollectionType.TAGGED)

        # get the dataset
        results = set(
            butler.registry.queryDatasets(
                datasetType=...,
                collections=self.collections,
                where=f"exposure={exposure} and instrument='LSSTComCam'",
            )
        )

        # should just be one...
        self.assertEqual(len(results), 1)

        # associate the dataset
        butler.registry.associate("test_collection", results)
        logging.info("done associating file")

    async def disassociate_file(self, exposure):
        """remove exposure from TAGGED collection

        Parameters
        ----------
        exposure : `str`
            the name of the exposure to remove
        """

        logging.info("waiting to disassociate file")
        await asyncio.sleep(5)
        logging.info("about to disassociate file")
        # create a butler and remove the file from the TAGGED collecdtion
        butler = Butler(self.repoDir, writeable=True)

        results = set(
            butler.registry.queryDatasets(
                datasetType=...,
                collections=self.collections,
                where=f"exposure={exposure} and instrument='LSSTComCam'",
            )
        )

        # should just be one...
        self.assertEqual(len(results), 1)

        # disassociate the dataset
        butler.registry.disassociate("test_collection", results)
        logging.info("done disassociating file")

    def strip_prefix(self, name, prefix):
        """strip prefix from name

        Parameters
        ----------
        name : `str`
           path of a file
        prefix : `str`
           prefix to strip

        Returns
        -------
        ret : `str`
            remainder of string
        """
        p = PurePath(name)
        ret = str(p.relative_to(prefix))
        return ret


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
