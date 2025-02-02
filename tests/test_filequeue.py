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

from heartbeat_base import HeartbeatBase
from lsst.ctrl.oods.fileQueue import FileQueue


class FileQueueTestCase(HeartbeatBase):
    """Test FileQueue object"""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        fd, self.tmp_file = tempfile.mkstemp()
        with open(self.tmp_file, "w") as f:
            f.write("filequeue test")
        os.close(fd)

    def tearDown(self):
        os.unlink(self.tmp_file)
        os.rmdir(self.tmp_dir)

    async def testFileQueue(self):
        fileq = FileQueue(self.tmp_dir)

        queue_task = asyncio.create_task(fileq.queue_files())

        await asyncio.sleep(1)

        os.link(self.tmp_file, os.path.join(self.tmp_dir, os.path.basename(self.tmp_file)))

        await asyncio.sleep(1)
        file_list = await fileq.dequeue_files()
        self.assertEqual(len(file_list), 1)
        ret_file = file_list[0]

        self.assertEqual(os.path.basename(self.tmp_file), os.path.basename(ret_file))
        os.unlink(ret_file)

        queue_task.cancel()

    async def testNoSleepFileQueue(self):
        fileq = FileQueue(self.tmp_dir)

        queue_task = asyncio.create_task(fileq.queue_files())

        os.link(self.tmp_file, os.path.join(self.tmp_dir, os.path.basename(self.tmp_file)))

        file_list = await fileq.dequeue_files()
        self.assertEqual(len(file_list), 1)
        ret_file = file_list[0]

        self.assertEqual(os.path.basename(self.tmp_file), os.path.basename(ret_file))

        os.unlink(ret_file)
        queue_task.cancel()

    async def testDoubleScan(self):
        fileq = FileQueue(self.tmp_dir)

        queue_task = asyncio.create_task(fileq.queue_files())

        os.link(self.tmp_file, os.path.join(self.tmp_dir, os.path.basename(self.tmp_file)))

        await asyncio.sleep(1)
        # waited, now there should be 1 file that we can dequeue
        fd, tmp_file2 = tempfile.mkstemp()
        with open(tmp_file2, "w") as f:
            f.write("filequeue test")
        os.close(fd)

        # create another file, but when we dequeue, there should still only
        # be one, since we didn't wait.
        os.link(tmp_file2, os.path.join(self.tmp_dir, os.path.basename(tmp_file2)))
        file_list = await fileq.dequeue_files()
        self.assertEqual(len(file_list), 2)
        ret_file1 = file_list[0]
        ret_file2 = file_list[1]
        os.unlink(ret_file1)
        os.unlink(ret_file2)

        with self.assertRaises(Exception):
            await asyncio.wait_for(fileq.dequeue_files(), timeout=3.0)

        queue_task.cancel()
