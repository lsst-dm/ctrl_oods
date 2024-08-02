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
from lsst.ctrl.oods.directoryScanner import DirectoryScanner
from lsst.ctrl.oods.fileIngester import FileIngester
from lsst.ctrl.oods.utils import Utils


class AsyncIngestTestCase(unittest.IsolatedAsyncioTestCase):
    """Test full async ingest"""

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
        # and alter the image staging directory to point
        # at the temporary directories created for his test

        ingesterConfig = config["ingester"]
        self.imageStagingDir = tempfile.mkdtemp()
        ingesterConfig["imageStagingDirectory"] = self.imageStagingDir

        self.badDir = tempfile.mkdtemp()
        butlerConfig = ingesterConfig["butlers"][0]["butler"]
        butlerConfig["badFileDirectory"] = self.badDir
        self.stagingDirectory = tempfile.mkdtemp()
        butlerConfig["stagingDirectory"] = self.stagingDirectory

        self.repoDir = tempfile.mkdtemp()
        butlerConfig["repoDirectory"] = self.repoDir

        # copy the FITS file to it's test location

        self.subDir = tempfile.mkdtemp(dir=self.imageStagingDir)
        self.destFile = os.path.join(self.subDir, fits_name)
        shutil.copyfile(fitsFile, self.destFile)

        return config

    def tearDown(self):
        """clean up after each test"""
        shutil.rmtree(self.destFile, ignore_errors=True)
        shutil.rmtree(self.imageStagingDir, ignore_errors=True)
        shutil.rmtree(self.badDir, ignore_errors=True)
        shutil.rmtree(self.stagingDirectory, ignore_errors=True)
        shutil.rmtree(self.repoDir, ignore_errors=True)
        shutil.rmtree(self.subDir, ignore_errors=True)

    async def testAsyncIngest(self):
        """test ingesting an auxtel file using all the async tasks"""
        fits_name = "2020032700020-det000.fits.fz"
        config = self.createConfig("ingest_auxtel_gen3.yaml", fits_name)

        # setup directory to scan for files in the image staging directory
        # and ensure one file is there
        ingesterConfig = config["ingester"]
        image_staging_dir = ingesterConfig["imageStagingDirectory"]
        scanner = DirectoryScanner([image_staging_dir])
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 1)

        # create a FileIngester
        ingester = FileIngester(config)

        # start all the ingester tasks
        task_list = ingester.run_tasks()

        # allow the other async tasks to run
        await asyncio.sleep(2)

        # check to make sure file was moved from image staging directory
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        # check to be sure the file didn't land in the "bad file" directory
        bad_path = os.path.join(self.badDir, fits_name)
        self.assertFalse(os.path.exists(bad_path))

        # check to be sure file ended up landing in butler "staging" directory
        staging_sub_dir = Utils.strip_prefix(self.subDir, image_staging_dir)
        stage_path = os.path.join(self.stagingDirectory, staging_sub_dir, fits_name)

        self.assertTrue(os.path.exists(stage_path))

        # add one more task, whose sole purpose is to interrupt the others by
        # throwing an acception
        task_list.append(asyncio.create_task(self.interrupt_me()))

        # gather all the tasks, until one (the "interrupt_me" task)
        # throws an exception, at which point, we can clean up the rest
        # of the running tasks.
        try:
            await asyncio.gather(*task_list)
        except Exception:
            for task in task_list:
                task.cancel()

    async def interrupt_me(self):
        """This method throws an exception after 10 seconds"""
        await asyncio.sleep(10)
        raise RuntimeError("I'm interrupting")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
