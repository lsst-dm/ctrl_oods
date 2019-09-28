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
import logging
import os
import sys
import yaml
import lsst.utils
from lsst.ctrl.oods.taskRunner import TaskRunner
from lsst.ctrl.oods.fileIngester import FileIngester
from lsst.ctrl.oods.cacheCleaner import CacheCleaner
from lsst.ctrl.oods.validator import Validator

logger = logging.getLogger("ctrl_oods")

name = os.path.basename(sys.argv[0])

parser = argparse.ArgumentParser(prog=name,
                                 description='''Ingests new files into a Butler''')
parser.add_argument("config", default=None, nargs='?',
                    help="use specified OODS YAML configuration file")

parser.add_argument("-y", "--yaml-validate", action="store_true",
                    dest="validate", default=False,
                    help="validate YAML configuration file")
parser.add_argument("-l", "--loglevel", nargs='?',
                    choices=('DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL'),
                    default='warn', help="print logging statements")

args = parser.parse_args()
lvls = {'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARN': logging.WARNING,
        'ERROR': logging.ERROR,
        'FATAL': logging.CRITICAL}

logger.setLevel(lvls[args.loglevel.upper()])

if args.config is None:
    package = lsst.utils.getPackageDir("ctrl_oods")
    yaml_path = os.path.join(package, "etc", "oods.yaml")
else:
    yaml_path = args.config

with open(yaml_path, 'r') as f:
    oods_config = yaml.safe_load(f)

if args.validate:
    v = Validator(logger, oods_config)
    v.verify()
    if v.isValid:
        print("valid OODS YAML configuration file")
        sys.exit(0)
    print("invalid OODS YAML configuration file")
    sys.exit(10)


logger.info("starting...")


ingester_config = oods_config["ingester"]
ingester = FileIngester(logger, ingester_config)
ingest = TaskRunner(interval=ingester_config["scanInterval"],
                    task=ingester.run_task)

cache_config = oods_config["cacheCleaner"]
cache_cleaner = CacheCleaner(logger, cache_config)
cleaner = TaskRunner(interval=cache_config["scanInterval"],
                     task=cache_cleaner.run_task)

ingest.start()
cleaner.start()

ingest.join()
cleaner.join()
