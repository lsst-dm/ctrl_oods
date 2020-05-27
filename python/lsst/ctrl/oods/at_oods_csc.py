# This file is part of dm_ctrl_oods
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
#
import asyncio
import logging
import pathlib
from lsst.dm.csc.base.dm_csc import dm_csc
from lsst.ts import salobj
from lsst.ts.salobj import State

LOGGER = logging.getLogger(__name__)


class AT_OODS_CSC(dm_csc):

    def __init__(self, schema_file, index, config_dir=None, initial_state=salobj.State.STANDBY,
                 initial_simulation_mode=0):
        schema_path = pathlib.Path(__file__).resolve().parents[4].joinpath("schema", schema_file)
        super().__init__("ATOODS", index=index, schema_path=schema_path,
                         config_dir=config_dir, initial_state=initial_state,
                         initial_simulation_mode=initial_simulation_mode)

        domain = salobj.Domain()

        salinfo = salobj.SalInfo(domain=domain, name="ATOODS", index=0)

        #self.director = ATDirector(self, "ATArchiver", "atarchiver_config.yaml", "ATArchiverCSC.log")
        #self.director.configure()

        self.transitioning_to_fault_evt = asyncio.Event()
        self.transitioning_to_fault_evt.clear()

        self.current_state = None

        self.ingester_task = None
        self.cleaner_task = None

        LOGGER.info("************************ Starting ATOODS ************************")

    async def start_services(self):
        ingester_config = self.config["ingester"]
        self.ingester = FileIngester(ingester_config)
        self.ingester.enable()

        cache_config = self.config["cacheCleaner"]
        cache_cleaner = CacheCleaner(cache_config)

        self.cleaner_task = asyncio.create_task(cache_cleaner.run_task())
        
    async def stop_services(self):
        self.ingester.disable()
        self.cleaner_task.cancel()

    async def do_resetFromFault(self, data):
        pass

    #async def send_imageInOODS(self, camera, obsid, archiverName, statusCode, description):
    async def send_imageInOODS(self, filename, statusCode, description):
        LOGGER.info(f"sending {camera} {obsid} {archiverName} {statusCode}: {description}")

        self.evt_imageInOODS.set_put(camera=camera,
                                     obsid=obsid,
                                     archiverName=archiverName,
                                     statusCode=statusCode,
                                     description=description)

    @staticmethod
    def get_config_pkg():
        return "dm_config_at_oods"
