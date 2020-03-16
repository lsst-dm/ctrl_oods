#!/usr/bin/env python

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
import logging
import os
import sys
import yaml
import lsst.log as lsstlog
import lsst.utils
from lsst.ctrl.oods.fileIngester import FileIngester
from lsst.ctrl.oods.cacheCleaner import CacheCleaner
from lsst.ctrl.oods.validator import Validator


async def wait_for_ingest(config):
    ingester_config = config["ingester"]
    ingester = FileIngester(ingester_config)

    res = await asyncio.create_task(ingester.run_task())
    return res


async def wait_for_cleaner(config):
    cache_config = config["cacheCleaner"]
    cache_cleaner = CacheCleaner(cache_config)
    res = await asyncio.create_task(cache_cleaner.run_task())
    return res


async def gather_tasks(interval):
    r = [wait_for_ingest(oods_config), wait_for_cleaner(oods_config)]
    res = await asyncio.gather(*r, return_exceptions=True)
    return res


if __name__ == "__main__":
    lsstlog.usePythonLogging()

    LOGGER = logging.getLogger(__name__)
    FORMAT = ('%(levelname) -10s %(asctime)s.%(msecs)03dZ %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")

    name = os.path.basename(sys.argv[0])

    parser = argparse.ArgumentParser(prog=name,
                                     description='''Ingests new files into a Butler''')
    parser.add_argument("config", default=None, nargs='?',
                        help="use specified OODS YAML configuration file")

    parser.add_argument("-y", "--yaml-validate", action="store_true",
                        dest="validate", default=False,
                        help="validate YAML configuration file")
    args = parser.parse_args()

    if args.config is None:
        package = lsst.utils.getPackageDir("ctrl_oods")
        yaml_path = os.path.join(package, "etc", "oods.yaml")
    else:
        yaml_path = args.config

    with open(yaml_path, 'r') as f:
        oods_config = yaml.safe_load(f)

    if args.validate:
        v = Validator(oods_config)
        v.verify()
        if v.isValid:
            print("valid OODS YAML configuration file")
            sys.exit(0)
        print("invalid OODS YAML configuration file")
        sys.exit(10)

    LOGGER.info("***** OODS starting...")

    res1, res2 = asyncio.get_event_loop().run_until_complete(gather_tasks(oods_config))
