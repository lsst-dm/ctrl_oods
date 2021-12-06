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
from lsst.ts.salobj import State
from lsst.ts.salobj import BaseCsc
from . import __version__

LOGGER = logging.getLogger(__name__)


class DmCSC(BaseCsc):
    """This object implements configuration and the state
    transition model for the BaseCSC

    Parameters
    ----------
    name : `str`
        name of this csc
    index : `int`
        csc index value
    initial_state : `int`
        initial state of the CSC
    """

    valid_simulation_modes = [0]
    version = __version__

    def __init__(self, name, initial_state):
        super().__init__(name, initial_state=initial_state)

        self.state_to_str = {
            State.DISABLED: "disabled",
            State.ENABLED: "enabled",
            State.FAULT: "fault",
            State.OFFLINE: "offline",
            State.STANDBY: "standby"}

    async def configure(self, config):
        """Configure this CSC and output the ``settingsApplied`` event.

        Parameters
        ----------
        config : `types.SimpleNamespace`
            Configuration, as described by ``schema/ATDomeTrajectory.yaml``
        """
        self.config = config
        LOGGER.info("configuring")
        self.evt_settingsApplied.set_put(settingsVersion=self.config.settingsVersion)
        await self.evt_softwareVersions.set_put(cscVersion=self.version, subsystemVersions="")

    def report_summary_state(self):
        """State transition model for the ArchiverCSC
        """
        super().report_summary_state()

        s_cur = None
        if self.current_state is not None:
            s_cur = self.state_to_str[self.current_state]
        s_sum = self.state_to_str[self.summary_state]

        LOGGER.info(f"current state is: {s_cur}; transition to {s_sum}")

        # Services are started when transitioning from STANDBY to DISABLED.
        # Services are stopped when transitioning from DISABLED to STANDBY.
        # Services are always stopped when moving from any state to FAULT.
        # The internal state machine in the base class only allows transition
        # from FAULT to STANDBY

        # NOTE: The following can be optimized, but hasn't been to give a
        # clearer picture about which state transitions occur, and what
        # happens when they do occur.

        # if current_state hasn't been set, and the summary_state is STANDBY,
        # we're just starting up, so don't do anything but set the current
        # state to STANBY
        if (self.current_state is None) and (self.summary_state == State.STANDBY):
            self.current_state = State.STANDBY
            self.transitioning_to_fault_evt.clear()
            return

        # if going from STANDBY to DISABLED, start external services
        if (self.current_state == State.STANDBY) and (self.summary_state == State.DISABLED):
            asyncio.ensure_future(self.start_services())
            self.current_state = State.DISABLED
            return

        # if going from DISABLED to STANDBY, stop external services
        if (self.current_state == State.DISABLED) and (self.summary_state == State.STANDBY):
            asyncio.ensure_future(self.stop_services())
            self.current_state = State.STANDBY
            return

        # if going from STANDBY to FAULT, kill any external services that
        # started
        if (self.current_state == State.STANDBY) and (self.summary_state == State.FAULT):
            asyncio.ensure_future(self.stop_services())
            self.current_state = State.FAULT
            return

        # if going from ENABLED to FAULT, don't do anything, so we can debug
        if (self.current_state == State.ENABLED) and (self.summary_state == State.FAULT):
            asyncio.ensure_future(self.stop_services())
            self.current_state = State.FAULT
            return

        # if going from DISABLED to FAULT, don't do anything, so we can debug
        if (self.current_state == State.DISABLED) and (self.summary_state == State.FAULT):
            asyncio.ensure_future(self.stop_services())
            self.current_state = State.FAULT
            return

        # if going from FAULT to STANDBY, services have already been stopped
        if (self.current_state == State.FAULT) and (self.summary_state == State.STANDBY):
            self.current_state = State.STANDBY
            self.transitioning_to_fault_evt.clear()
            return

        # These are all place holders and only update state

        # if going from DISABLED to ENABLED leave external services alone,
        # but accept control commands
        if (self.current_state == State.DISABLED) and (self.summary_state == State.ENABLED):
            self.current_state = State.ENABLED
            return

        # if going from ENABLED to DISABLED, leave external services alone,
        # but reject control commands
        if (self.current_state == State.ENABLED) and (self.summary_state == State.DISABLED):
            self.current_state = State.DISABLED
            return

    def call_fault(self, code, report):
        """Called when a fault in the CSC is detected

        This is called by lower level methods when anything happens that
        the CSC detects as a fault, and reports it via the ts_salobj
        reporting code.

        Parameters
        ----------
        code : `int`
            error code
        report : `str`
            description of the fault to report
        """
        # this safeguard is in place so that if multiple faults occur, fault
        # is only reported one time.
        if self.transitioning_to_fault_evt.is_set():
            return
        self.transitioning_to_fault_evt.set()
        LOGGER.info(report)
        self.fault(code, report)
