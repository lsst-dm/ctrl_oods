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

def on_metadata_failure(filename, exc):
    """Callback used on metadata extraction failure. Used to transmit
    unsuccessful data ingestion status

    Parameters
    ----------
    filename: `ButlerURI`
        ButlerURI that failed in ingest
    exc: `Exception`
        Exception which explains what happened
    """
    print("on_metadata_failure")
    real_file = filename.ospath
    cause = self.extract_cause(exc)
    info = self.undef_metadata(real_file)
    self.transmit_status(info, code=2, description=f"metadata failure: {cause}")


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
    butler=butler,
    on_metadata_failure=on_metadata_failure
)

file_list=[ResourcePath("file:///tmp/2020032700020-det000.fits.fz")]
        
task.run(file_list)
