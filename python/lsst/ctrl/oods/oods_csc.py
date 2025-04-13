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
import os
from importlib import import_module

import yaml
from lsst.ctrl.oods.dm_csc import DmCsc
from lsst.ctrl.oods.msgIngester import MsgIngester
from lsst.ts import salobj

LOGGER = logging.getLogger(__name__)


class OodsCsc(DmCsc):
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
            filename = os.environ["CTRL_OODS_CONFIG_FILE"]
            LOGGER.info("using configuration %s", filename)
            with open(filename, "r") as f:
                self.config = yaml.safe_load(f)
        else:
            raise FileNotFoundError("CTRL_OODS_CONFIG_FILE is not set")

        self.task_list = None

        self.ingester_config = self.config["ingester"]

    async def send_imageInOODS(self, info):
        """Send SAL message that the images has been ingested into the OODS

        Parameters
        ----------
        info : `dict`
            information about the image
        """
        camera = info["CAMERA"]
        obsid = info["OBSID"]
        raft = "undef"
        if "RAFT" in info:
            raft = info["RAFT"]
        sensor = "undef"
        if "SENSOR" in info:
            sensor = info["SENSOR"]
        status_code = info["STATUS_CODE"]
        description = info["DESCRIPTION"]

        s = f"sending camera={camera} obsid={obsid} raft={raft} sensor={sensor} "
        s = s + f"statusCode={status_code}, description={description}"
        LOGGER.debug(s)
        await self.evt_imageInOODS.set_write(
            camera=camera,
            obsid=obsid,
            raft=raft,
            sensor=sensor,
            statusCode=status_code,
            description=description,
        )

    async def start_services(self):
        """Start all cleanup and archiving services"""

        # self added here, and by the time it's utilized by Ingester
        # the CSC will be up and running
        self.ingester = self.createIngester()

        self.task_list = self.ingester.run_tasks()

    def createIngester(self):
        if "ingesterClass" not in self.config:
            ingester = MsgIngester(self.config, self)
            return ingester

        # this is a fall back, in case we want to use another
        # ingestion type (like FileIngester)
        classConfig = self.config["ingesterClass"]
        importFile = classConfig["ingesterType"]
        name = classConfig["ingesterName"]

        mod = import_module(importFile)
        ingesterClass = getattr(mod, name)
        ingester = ingesterClass(self.config, self)
        return ingester

    async def stop_services(self):
        """Stop all cleanup and archiving services"""
        self.ingester.stop_tasks()
