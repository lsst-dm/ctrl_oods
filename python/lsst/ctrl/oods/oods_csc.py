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

import logging
from lsst.ctrl_oods.dm_csc import DmCSC
from lsst.ts import salobj

LOGGER = logging.getLogger(__name__)


class OodsCSC(DmCSC):
    """Base OODS class used for OODS Commandable SAL Components (CSC)

    Parameters
    ----------
    name : `str`
        Name of SAL component.
    index : `int` or `None`
        SAL component index, or 0 or None if the component is not indexed.
    initial_state : `State` or `int`, optional
        The initial state of the CSC. This is provided for unit testing,
        as real CSCs should start up in `State.STANDBY`, the default.

    """

    def __init__(self, name, initial_state=salobj.State.STANDBY):
        super().__init__(name, initial_state=initial_state)
        self.config = None
        # import YAML file here
        if "CTRL_OODS_CONFIG_FILE" in os.environ:
            self.config = os.environ["CTRL_OODS_CONFIG_FILE"]
        else:
            raise FileNotFoundError("CTRL_OODS_CONFIG_FILE is not set")

    async def send_imageInOODS(self, info):
        """Send SAL message that the images has been ingested into the OODS

        Parameters
        ----------
        info : `dict`
            information about the image
        """
        camera = info['CAMERA']
        archiverName = info['ARCHIVER']
        obsid = info['OBSID']
        raft = "undef"
        if 'RAFT' in info:
            raft = info['RAFT']
        sensor = "undef"
        if 'SENSOR' in info:
            sensor = info['SENSOR']
        statusCode = info['STATUS_CODE']
        description = info['DESCRIPTION']

        s = f'sending camera={camera} obsid={obsid} raft={raft} sensor={sensor} '
        s = s + f'archiverName={archiverName}, statusCode={statusCode}, description={description}'
        LOGGER.info(s)
        self.evt_imageInOODS.set_put(camera=camera,
                                     obsid=obsid,
                                     raft=raft,
                                     sensor=sensor,
                                     archiverName=archiverName,
                                     statusCode=statusCode,
                                     description=description)

    async def start_services(self):
        """Start all cleanup and archiving services
        """
        ingester_config = self.config["ingester"]
        ingester = FileIngester(ingester_config)

        cache_config = self.config["cacheCleaner"]
        cache_cleaner = CacheCleaner(cache_config)

        r = [ingester.run_task(), cache_cleaner.run_task()]

        LOGGER.info("gathering tasks")
        res = await asyncio.gather(*r, return_exceptions=True)
        LOGGER.info("tasks gathered")
        pass

    async def stop_services(self):
        """Stop all cleanup and archiving services
        """
        pass

    async def do_resetFromFault(self, data):
        """resetFromFault. Required by ts_salobj csc
        """
        print(f"do_resetFromFault called: {data}")
