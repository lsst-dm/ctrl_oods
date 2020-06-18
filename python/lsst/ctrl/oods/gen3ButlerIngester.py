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

import os
from lsst.daf.butler import Butler
from lsst.obs.base.ingest import RawIngestTask, RawIngestConfig
from lsst.obs.base.utils import getInstrument


class Gen3ButlerIngester(object):
    """Processes files for ingestion into a Gen3 Butler.
    """
    def __init__(self, butlerConfig):
        repo = butlerConfig["repoDirectory"]
        run = butlerConfig["run"]

        register = False
        # Create Butler repository.
        if not os.path.exists(os.path.join(repo, "butler.yaml")):
            Butler.makeRepo(repo)
            register = True
    
        run = "oods_temp_placeholder"

        opts = dict(run=run, writeable=True)
        self.btl = Butler(repo, **opts)

        if register:
           # Register an instrument.
           instr = getInstrument("lsst.obs.lsst.LsstComCam")
           instr.register(btl.registry)

    def ingest(self, filename):
        # repo = "/tmp/foobar"
        # image = "/tmp/data/2020060500001-R22-S00-det000.fits"

        # Ingest image.
        cfg = RawIngestConfig()
        cfg.transfer = "link"
        task = RawIngestTask(config=cfg, butler=self.btl)
        try:
            task.run([image])
        except RuntimeError as ex:
            print(ex)
