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
import tempfile
from pathlib import PurePath
from shutil import copyfile
import yaml
from lsst.ctrl.oods.directoryScanner import DirectoryScanner
from lsst.ctrl.oods.fileIngester import FileIngester
import lsst.utils.tests
import asynctest
import utils


class Gen3ComCamIngesterTestCase(asynctest.TestCase):
    """Test Gen3 Butler Ingest"""

    def createConfig(self, config_name, fits_name):
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
        configFile = os.path.join(testdir, "etc", config_name)

        # path to the FITS file to ingest

        fitsFile = os.path.join(testdir, "data", fits_name)

        # load the YAML configuration

        with open(configFile, "r") as f:
            config = yaml.safe_load(f)

        # extract parts of the ingester configuration
        # and alter the forwarder staging directory to point
        # at the temporary directories created for his test

        ingesterConfig = config["ingester"]
        self.forwarderStagingDir = tempfile.mkdtemp()
        ingesterConfig["forwarderStagingDirectory"] = self.forwarderStagingDir
        print(f"forwarderStagingDirectory = {self.forwarderStagingDir}")

        self.badDir = tempfile.mkdtemp()
        butlerConfig = ingesterConfig["butlers"][0]["butler"]
        butlerConfig["badFileDirectory"] = self.badDir
        self.stagingDirectory = tempfile.mkdtemp()
        butlerConfig["stagingDirectory"] = self.stagingDirectory
        print(f"stagingDirectory = {self.stagingDirectory}")

        self.repoDir = tempfile.mkdtemp()
        butlerConfig["repoDirectory"] = self.repoDir

        # copy the FITS file to it's test location

        self.subDir = tempfile.mkdtemp(dir=self.forwarderStagingDir)
        self.destFile = os.path.join(self.subDir, fits_name)
        copyfile(fitsFile, self.destFile)

        return config

    def removeEntries(self, directory):
        if os.path.exists(directory) is False:
            return
        if os.path.isdir(directory) is False:
            os.unlink(directory)
            return
        with os.scandir(directory) as entries:
            for ent in entries:
                if os.path.isdir(ent):
                    self.removeEntries(ent)
                else:
                    os.remove(ent)
        os.rmdir(directory)

    def tearDown(self):
        utils.removeEntries(self.destFile)
        utils.removeEntries(self.forwarderStagingDir)
        utils.removeEntries(self.badDir)
        utils.removeEntries(self.stagingDirectory)
        utils.removeEntries(self.repoDir)
        utils.removeEntries(self.subDir)

    def strip_prefix(self, name, prefix):
        """strip prefix from name

        Parameters
        ----------
        name: `str`
           path of a file
        prefix: `str`
           prefix to strip

        Returns
        -------
        ret: `str`
            remainder of string
        """
        p = PurePath(name)
        ret = str(p.relative_to(prefix))
        return ret

    async def testAuxTelIngest(self):
        """test ingesting an auxtel file
        """
        fits_name = "2020032700020-det000.fits.fz"
        config = self.createConfig("ingest_auxtel_gen3.yaml", fits_name)

        # setup directory to scan for files in the forwarder staging directory
        # and ensure one file is there
        ingesterConfig = config["ingester"]
        forwarder_staging_dir = ingesterConfig["forwarderStagingDirectory"]
        scanner = DirectoryScanner([forwarder_staging_dir])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        # create a FileIngester
        ingester = FileIngester(ingesterConfig)

        await ingester.ingest(self.destFile)

        # check to make sure the file was moved from the staging directory
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        # check to be sure the file didn't land in the "bad file" directory
        bad_path = os.path.join(self.badDir, fits_name)
        self.assertFalse(os.path.exists(bad_path))

    async def testComCamIngest(self):
        fits_name = "3019053000001-R22-S00-det000.fits.fz"
        config = self.createConfig("ingest_comcam_gen3.yaml", fits_name)

        # setup directory to scan for files in the forwarder staging directory
        # and ensure one file is there
        ingesterConfig = config["ingester"]
        forwarder_staging_dir = ingesterConfig["forwarderStagingDirectory"]
        scanner = DirectoryScanner([forwarder_staging_dir])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        # create the file ingester, get all tasks associated with it, and
        # create the tasks
        ingester = FileIngester(ingesterConfig)
        clean_tasks = ingester.getButlerCleanTasks()

        task_list = []
        for clean_task in clean_tasks:
            task = asyncio.create_task(clean_task())
            task_list.append(task)

        # check to see that the file is there before ingestion
        self.assertTrue(os.path.exists(self.destFile))
        print(f"destFile = {self.destFile}")

        await ingester.ingest(self.destFile)

        # make sure staging area is now empty
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        # Check to see that the file was ingested.
        # Recall that files start in teh forwarder staging area, and are
        # moved to the OODS staging area before ingestion. On "direct"
        # ingestion, this is where the file is located.  This is a check
        # to be sure that happened.
        name = self.strip_prefix(self.destFile, self.forwarderStagingDir)
        file_to_ingest = os.path.join(self.stagingDirectory, name)
        self.assertTrue(os.path.exists(file_to_ingest))

        # this file should now not exist
        self.assertFalse(os.path.exists(self.destFile))

        # add one more task, whose sole purpose is to interrupt the others by
        # throwing an acception
        task_list.append(asyncio.create_task(self.interrupt_me()))

        # kick off all the tasks, until one (the "interrupt_me" task)
        # throws an exception
        try:
            await asyncio.gather(*task_list)
        except Exception:
            for task in task_list:
                task.cancel()

        # that should have been enough time to run the "real" tasks,
        # which performed the ingestion, and the clean up task, which
        # was set to clean it up right away.  (That "clean up" time
        # is set in the config file loaded for this FileIngester).
        # And, when "cleaned up", the file that was originally there
        # is now gone.  Check for that.
        self.assertFalse(os.path.exists(file_to_ingest))

        # check to be sure that the file wasn't "bad" (and
        # therefore, not ingested)
        bad_path = os.path.join(self.badDir, fits_name)
        self.assertFalse(os.path.exists(bad_path))

    async def testBadIngest(self):
        fits_name = "bad.fits.fz"
        config = self.createConfig("ingest_comcam_gen3.yaml", fits_name)

        # setup directory to scan for files in the forwarder staging directory
        ingesterConfig = config["ingester"]
        forwarder_staging_dir = ingesterConfig["forwarderStagingDirectory"]
        scanner = DirectoryScanner([forwarder_staging_dir])
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        ingester = FileIngester(config["ingester"])

        await ingester.ingest(self.destFile)

        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        name = self.strip_prefix(self.destFile, forwarder_staging_dir)
        bad_path = os.path.join(self.badDir, name)
        self.assertTrue(os.path.exists(bad_path))

    async def testRepoExists(self):
        fits_name = "bad.fits.fz"
        config = self.createConfig("ingest_comcam_gen3.yaml", fits_name)

        FileIngester(config["ingester"])
        # tests the path that the previously created repo (above) exists
        FileIngester(config["ingester"])

    async def interrupt_me(self):
        await asyncio.sleep(20)
        raise RuntimeError("I'm interrupting")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
