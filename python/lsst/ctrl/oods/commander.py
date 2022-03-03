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

from lsst.ts import salobj


class Commander:
    """Issues commands to a CSC device

    Parameters
    ----------
    device_name : `str`
        name of CSC device to commnad
    command : `str`
        command to issues
    timeout : `int`
        command timeout value, in seconds
    """

    def __init__(self, device_name, command, timeout):
        self.device_name = device_name
        self.command = command
        self.timeout = timeout

    async def run_command(self):
        """send a command to a CSC device"""
        async with salobj.Domain() as domain:
            arc = salobj.Remote(domain=domain, name=self.device_name, index=0)
            await arc.start_task

            try:
                cmd = getattr(arc, f"cmd_{self.command}")
                await cmd.set_start(timeout=self.timeout)
            except Exception as e:
                print(e)
