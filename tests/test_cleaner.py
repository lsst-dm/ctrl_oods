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
import datetime
import os
import tempfile
import time

import lsst.utils.tests
from heartbeat_base import HeartbeatBase
from lsst.ctrl.oods.cacheCleaner import CacheCleaner
from lsst.ctrl.oods.directoryScanner import DirectoryScanner
from lsst.ctrl.oods.oods_config import OODSConfig, TimeInterval


class CleanerTestCase(HeartbeatBase):
    """Test cache cleaning"""

    async def testFileCleaner(self):
        # create a temporary directory that looks like the cache
        dirPath = tempfile.mkdtemp()
        testdir = os.path.abspath(os.path.dirname(__file__))
        config = OODSConfig.load(os.path.join(testdir, "etc", "cleaner.yaml"))

        cc_config = config.file_ingester.cache_cleaner
        cc_config.clear_empty_directories_and_old_files = [dirPath]

        interval = TimeInterval()
        interval.days = 1
        interval.minutes = 0
        interval.hours = 0
        interval.seconds = 0

        scanInterval = TimeInterval()
        scanInterval.days = 0
        scanInterval.minutes = 0
        scanInterval.hours = 0
        scanInterval.seconds = 3

        cc_config.files_older_than = interval
        cc_config.directories_empty_for_more_than = interval
        cc_config.cleaning_interval = scanInterval

        # put some files into it
        (fh1, filename1) = tempfile.mkstemp(dir=dirPath)
        (fh2, filename2) = tempfile.mkstemp(dir=dirPath)
        (fh3, filename3) = tempfile.mkstemp(dir=dirPath)

        # create a DirectoryScanner so we can keep tabs on the files
        # we put into the temp directory
        scanner = DirectoryScanner([dirPath])
        files = await scanner.getAllFiles()

        # check to make sure we have the files in there that we
        # think we do
        self.assertEqual(len(files), 3)

        # run the cleaner once
        cleaner = CacheCleaner(cc_config)
        await cleaner.clean()

        # get the list of files
        files = await scanner.getAllFiles()

        # all the files should still be there
        self.assertEqual(len(files), 3)

        # change the modifiction date to the past
        self.changeModificationDate(filename1)

        # clean up that "old" file
        await cleaner.clean()

        # check to see that we now have the number of files we expect
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 2)

        # change the modifiction date to the past
        self.changeModificationDate(filename2)
        self.changeModificationDate(filename3)

        # clean up that "old" file
        await cleaner.clean()

        # check to see that we now have the number of files we expect
        files = await scanner.getAllFiles()
        self.assertEqual(len(files), 0)

        # remove the temporary directory we created
        os.rmdir(dirPath)

    async def testDirectoryCleaner(self):
        # create a temporary directory that looks like the cache
        dirPath = tempfile.mkdtemp()

        testdir = os.path.abspath(os.path.dirname(__file__))
        config = OODSConfig.load(os.path.join(testdir, "etc", "cleaner.yaml"))

        cc_config = config.file_ingester.cache_cleaner
        cc_config.clear_empty_directories_and_old_files = [dirPath]

        interval = TimeInterval()
        interval.days = 1
        interval.hours = 0
        interval.minutes = 0
        interval.seconds = 0

        scanInterval = TimeInterval()
        scanInterval.days = 0
        scanInterval.minutes = 0
        scanInterval.hours = 0
        scanInterval.seconds = 3

        cc_config.files_older_than = interval
        cc_config.directories_empty_for_more_than = interval
        cc_config.cleaning_interval = scanInterval

        # put some directories into it
        dirname1 = tempfile.mkdtemp(dir=dirPath)
        dirname2 = tempfile.mkdtemp(dir=dirPath)
        dirname3 = tempfile.mkdtemp(dir=dirPath)

        # ... and one file into dirname1, and one in the main directory
        (fh1, filename1) = tempfile.mkstemp(dir=dirname1)
        (fh2, filename2) = tempfile.mkstemp(dir=dirPath)

        # run the cleaner once
        cleaner = CacheCleaner(cc_config)
        await cleaner.clean()

        # check to see if all the directories are still there
        self.assertTrue(os.path.exists(dirname1))
        self.assertTrue(os.path.exists(dirname2))
        self.assertTrue(os.path.exists(dirname3))

        testFile = os.path.join(dirname1, filename1)
        self.assertTrue(os.path.exists(testFile))
        self.assertTrue(os.path.exists(filename2))

        # change the modifiction date to the past
        self.changeModificationDate(dirname2)

        # turn off verbosity and clean up that "old" subdirectory
        await cleaner.clean()

        # check to see that we now have the number of subdirectories we expect
        self.assertTrue(os.path.exists(dirname1))
        self.assertFalse(os.path.exists(dirname2))
        self.assertTrue(os.path.exists(dirname3))
        self.assertTrue(os.path.exists(testFile))

        # change the modification date of the subdirectory with a file in it
        self.changeModificationDate(dirname1)
        await cleaner.clean()

        # the subdirectory should still be there because the file is newer
        self.assertTrue(os.path.exists(dirname1))
        # ..and the file should be there too.
        self.assertTrue(os.path.exists(testFile))

        # change the modifiction date of the file to the past
        self.changeModificationDate(testFile)
        await cleaner.clean()

        # make sure the file was removed...and the subdirectory NOT.
        self.assertFalse(os.path.exists(testFile))
        self.assertTrue(os.path.exists(dirname1))

        # this directory should also remain untouched
        self.assertTrue(os.path.exists(dirname3))

        # change the modifiction date to the past
        self.changeModificationDate(dirname1)

        # clean up the "old" directory
        await cleaner.clean()

        # check to see that we now have one subdirectory left
        self.assertFalse(os.path.exists(dirname1))
        self.assertTrue(os.path.exists(dirname3))

        # change the date of the last subdirectory and remove it.
        self.changeModificationDate(dirname3)
        await cleaner.clean()
        self.assertFalse(os.path.exists(dirname3))

        # change the date of the last file in the main directory
        self.assertTrue(os.path.exists(filename2))
        self.changeModificationDate(filename2)
        await cleaner.clean()
        self.assertFalse(os.path.exists(filename2))

        # close handles to old files
        os.close(fh1)
        os.close(fh2)
        # remove the temporary directory we created
        os.rmdir(dirPath)

    def changeModificationDate(self, filename):
        # change the modification time of the file/dir to Jan 2, 2018 03:04:05
        date = datetime.datetime(year=2018, month=1, day=2, hour=3, minute=4, second=5)
        modTime = time.mktime(date.timetuple())
        os.utime(filename, (modTime, modTime))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
