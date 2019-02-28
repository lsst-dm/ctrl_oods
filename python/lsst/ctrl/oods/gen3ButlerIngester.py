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
import subprocess
import logging

from lsst.daf.butler import Butler
from lsst.obs.base.gen3 import RawIngestTask
#from lsst.obs.subaru.gen.hcs import HyperSuprimeCam

class Gen3ButlerIngester(object):
    """Processes files for ingestion into a Gen3 Butler.
    """
    def __init__(self, logger, repo):
        lvls = {logging.DEBUG: 'debug',
                logging.INFO: 'info',
                logging.WARNING: 'warn',
                logging.ERROR: 'error',
                logging.CRITICAL: 'fatal'}

        num = logger.getEffectiveLevel()
        name = lvls[num]
        self.logger = logger

        self.config = RawIngestTask.ConfigClass()
        self.butler = Butler(repo, run="raw")
        # HyperSuprimeCam().register(self.butler.registry)

    def ingest(self, files, batchSize=0):
        """Ingest files in 'batchSize' increments.
        """

        task = RawIngestTask(config=self.config, butler=self.butler)
        task.log.setLevel(task.log.FATAL) # silence logs
        task.run(files)
