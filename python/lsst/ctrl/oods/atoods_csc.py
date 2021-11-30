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
import pathlib
from lsst.dm.csc.base.archiver_csc import ArchiverCSC
from lsst.dm.ATArchiver.atdirector import ATDirector
from lsst.ts import salobj
from lsst.ts.salobj import State

LOGGER = logging.getLogger(__name__)


class ATOodsCSC(ArchiverCSC):

    def __init__(self):
        super().__init__("ATOODS", initial_state=salobj.State.STANDBY)

        self.transitioning_to_fault_evt = asyncio.Event()
        self.transitioning_to_fault_evt.clear()

        self.current_state = None
        LOGGER.info("************************ Starting ATOODS ************************")
