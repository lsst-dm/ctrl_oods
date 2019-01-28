#!/usr/bin/env python

#
# LSST Data Management System
#
# Copyright 2008-2019  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#

import optparse
import os
import sys
import yaml
import lsst.utils
from lsst.ctrl.oods.taskRunner import TaskRunner
from lsst.ctrl.oods.fileIngester import FileIngester
from lsst.ctrl.oods.cacheCleaner import CacheCleaner
from lsst.ctrl.oods.validator import Validator

usage = """usage: %prog [[-c config]|[-y config]]"""

parser = optparse.OptionParser("usage")
parser.add_option("-c", "--config", action="store", dest="configFile",
                  default=None, help="OODS configuration file")
parser.add_option("-y", "--yaml-validate", action="store_true", dest="validate",
                  default=False, help="validate YAML file")
parser.add_option("-v", "--verbose", action="store_true",
                  dest="verbose", default=False, help="verbose output")

parser.opts = {}
parser.args = []

(parser.opts, parser.args) = parser.parse_args()

if parser.opts.validate is True:
    with open(parser.args[0], 'r') as f:
        config = yaml.load(f)
        v = Validator(config, parser.opts.verbose)
        v.verify()
        if not v.isValid:
            print("invalid OODS YAML configuration file")
        else:
            print("valid OODS YAML configuration file")
    sys.exit(0)

package = lsst.utils.getPackageDir("ctrl_oods")
yamlPath = os.path.join(package, "etc", "oods.yaml")
if parser.opts.configFile is not None:
    yamlPath = os.path.join(parser.opts.configFile)

oodsConfig = None
with open(yamlPath, 'r') as f:
    oodsConfig = yaml.load(f)


print("starting...")


ingesterConfig = oodsConfig["ingester"]
ingester = FileIngester(ingesterConfig, parser.args.verbose)
ingest = TaskRunner(interval=ingesterConfig["scanInterval"],
                    task=ingester.runTask)

cacheConfig = oodsConfig["cacheCleaner"]
cacheCleaner = CacheCleaner(cacheConfig, parser.args.verbose)
cleaner = TaskRunner(interval=cacheConfig["scanInterval"],
                     task=cacheCleaner.runTask)

ingest.start()
cleaner.start()

ingest.join()
cleaner.join()
