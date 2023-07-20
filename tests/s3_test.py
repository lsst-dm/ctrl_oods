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
import collections
import concurrent
import logging
import os

from lsst.daf.butler import Butler
from lsst.daf.butler.registry import CollectionType
from lsst.obs.base.ingest import RawIngestConfig, RawIngestTask
from lsst.pipe.base import Instrument
from lsst.resources import ResourcePath

LOGGER = logging.getLogger(__name__)


repo = "/tmp/repo"
instrument = "lsst.obs.lsst.Latiss"
collections = ["LATISS/raw/all"]

butlerConfig = Butler.makeRepo(repo)

instr = Instrument.from_string(instrument)
run = instr.makeDefaultRawIngestRunName()
opts = dict(run=run, writeable=True, collections=collections)
butler = Butler(butlerConfig, **opts)

# Register an instrument.
instr.register(butler.registry)

cfg = RawIngestConfig()
cfg.transfer = "direct"
task = RawIngestTask(
    config=cfg,
    butler=butler
)

file_list=[ResourcePath("s3://arn:aws:s3::rubin:rubin-pp/HSC/73/2023061400090/0/6140090/HSC-Z/HSC-2023061400090-0-6140090-HSC-Z-73.fz")]
        
task.run(file_list)
