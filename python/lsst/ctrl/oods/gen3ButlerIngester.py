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

from lsst.daf.butler import Butler
from lsst.obs.base.ingest import RawIngestTask, RawIngestConfig
from lsst.obs.base.utils import getInstrument


class Gen3ButlerIngester(object):
    """Processes files for ingestion into a Gen3 Butler.
    """
    def __init__(self, butlerConfig):
        repo = butlerConfig["repoDirectory"]
        instrument = butlerConfig["instrument"]

        register = False
        try:
            butlerConfig = Butler.makeRepo(repo)
            register = True
        except FileExistsError:
            butlerConfig = repo

        instr = getInstrument(instrument)
        run = instr.makeDefaultRawIngestRunName()
        opts = dict(run=run, writeable=True)
        self.btl = Butler(butlerConfig, **opts)

        if register:
            # Register an instrument.
            instr.register(self.btl.registry)

    def ingest(self, filename):

        # Ingest image.
        cfg = RawIngestConfig()
        cfg.transfer = "direct"
        task = RawIngestTask(config=cfg, butler=self.btl)
        task.run([filename])

    def getName(self):
        return "gen3"
