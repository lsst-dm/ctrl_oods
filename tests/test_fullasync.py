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


class AsyncIngestTestCase(asynctest.TestCase):
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
        # and alter the forwarder staging directory to point
        # at the temporary directories created for his test

        ingesterConfig = config["ingester"]
        self.forwarderStagingDir = tempfile.mkdtemp()
        ingesterConfig["forwarderStagingDirectory"] = self.forwarderStagingDir

        self.badDir = tempfile.mkdtemp()
        butlerConfig = ingesterConfig["butlers"][0]["butler"]
        butlerConfig["badFileDirectory"] = self.badDir
        self.stagingDirectory = tempfile.mkdtemp()
        butlerConfig["stagingDirectory"] = self.stagingDirectory

        self.repoDir = tempfile.mkdtemp()
        butlerConfig["repoDirectory"] = self.repoDir

        # copy the FITS file to it's test location

        self.subDir = tempfile.mkdtemp(dir=self.forwarderStagingDir)
        self.destFile = os.path.join(self.subDir, fits_name)
        copyfile(fitsFile, self.destFile)

        return config

    def tearDown(self):
        """ clean up after each test """
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

    async def testAsyncIngest(self):
        """test ingesting an auxtel file using all the async tasks
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

        # start all the ingester tasks
        task_list = await ingester.run_task()

        # allow the other async tasks to run
        await asyncio.sleep(2)

        # check to make sure file was moved from forwarder staging directory
        files = scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        # check to be sure the file didn't land in the "bad file" directory
        bad_path = os.path.join(self.badDir, fits_name)
        self.assertFalse(os.path.exists(bad_path))

        # check to be sure file ended up landing in butler "staging" directory
        staging_sub_dir = self.strip_prefix(self.subDir, forwarder_staging_dir)
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
        """ This method throws an exception after 10 seconds"""
        await asyncio.sleep(10)
        raise RuntimeError("I'm interrupting")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
