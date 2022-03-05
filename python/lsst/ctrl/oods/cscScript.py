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

import argparse
import asyncio

from lsst.ctrl.oods.commander import Commander


def build_argparser():

    parser = argparse.ArgumentParser(description="Send SAL commands to devices")
    parser.add_argument(
        "-D",
        "--device",
        type=str,
        dest="device",
        required=True,
        help="component to which the command will be sent",
    )
    parser.add_argument("-t", "--timeout", type=int, dest="timeout", default=5, help="command timeout")

    subparsers = parser.add_subparsers(dest="command")

    cmds = ["start", "enable", "disable", "enterControl", "exitControl", "standby", "abort", "resetFromFault"]
    for x in cmds:
        subparsers.add_parser(x)

    return parser


def main():

    args = build_argparser().parse_args()

    cmdr = Commander(args.device, args.command, args.timeout)
    asyncio.run(cmdr.run_command())
