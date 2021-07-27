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
from lsst.ctrl.oods.fileQueue import FileQueue

LOGGER = logging.getLogger(__name__)


class FileLinker():
    def __init__(self, config):

        staging_dir = config["stagingDirectory"]
        self.link_dirs = config["linkToDirectories"]
        file_queue = FileQueue(staging_dir)

        asyncio.create_task(file_queue.queue_files())

        asyncio.create_task(self._linker(file_queue.dequeue_file))

    async def _linker(self, method):
        while True:
            filename = await method()
            dir_name = self.create_landing_directory(filename)
            for link_dir in self.link_dirs:
                os.link(filename, os.path.join(link_dir, dir_name, os.path.basename(filename)))

    def create_landing_directory(self, root_dir, filename):
        # creates a directory that looks like
        #
        # rootdir/date/image_name

        # this presumes file names that look like:
        #
        # CC_O_20210708_000020_R22_S22.fits
        #
        # or
        #
        # CC_O_20210708_000020-R22S22.fits
        #
        # Given the file name above, the landing
        # directory would be:
        #
        # rootdir/20210708/000020

        if filename.indexOf('-') != 0:
            s = filename.split('-')[0]
            s = s[0].split('_')
        else:
            s = filename.split('_')

        date_name = s[2]  # the date in the image
        image_name = s[3]  # the image number
        new_dir_name = os.path.join(root_dir, date_name, image_name)
        os.makedirs(new_dir_name)
        return new_dir_name
