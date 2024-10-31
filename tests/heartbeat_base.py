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
import unittest

from lsst.ctrl.oods.fileIngester import FileIngester

LOGGER = logging.getLogger(__name__)


class HeartbeatBase(unittest.IsolatedAsyncioTestCase):
    """Base class for asyncio tests"""

    async def heartbeat(self):
        """Produce a log message every second"""
        x = 0
        while True:
            LOGGER.info(f"heartbeat {x}")
            x = x + 1
            await asyncio.sleep(1)

    async def asyncSetUp(self):
        """Create a heartbeat task on startup"""
        self.heartbeat_task = asyncio.create_task(self.heartbeat())

    async def asyncTearDown(self):
        """Cancel the heartbeat task"""
        self.heartbeat_task.cancel()

    async def perform_clean(self, config):
        """Perform butler clean operation to remove old files"""

        ingester = FileIngester(config)

        clean_methods = ingester.getButlerCleanMethods()

        for clean in clean_methods:
            await asyncio.create_task(clean())
