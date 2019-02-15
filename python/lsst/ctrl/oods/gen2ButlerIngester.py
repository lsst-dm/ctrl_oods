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


class Gen2ButlerIngester(object):
    """Processes files for ingestion into a Gen2 Butler.
    """
    def __init__(self, logger, repo):
        lvls = {logging.DEBUG: 'debug',
                logging.INFO: 'info',
                logging.WARNING: 'warn',
                logging.ERROR: 'error',
                logging.CRITICAL: 'fatal'}

        num = logger.getEffectiveLevel()
        name = lvls[num]
        self.precmd = ['ingestImages.py',
                       repo,
                       '--ignore-ingested',
                       '--mode', 'move',
                       '--loglevel=%s' % name]
        self.logger = logger

    def ingest(self, files, batchSize):
        """Ingest files in 'batchSize' increments.
        """
        if len(files) == 0:
            return

        #
        # The current method for ingest is to append files to the command
        # line.  Since there is a character limit for command lines, this
        # splits up the file ingestion into groups so the limit isn't reached.
        #
        if batchSize == -1:
            batchSize = len(files)
        # This should calculate to just below some high water mark for
        # better efficiency, but splitting on batchSize will do for now.
        chunks = [files[x:x+batchSize] for x in range(0, len(files), batchSize)]
        for chunk in chunks:
            self.logger.info("ingesting ", chunk)
            cmd = self.precmd + chunk
            subprocess.call(cmd)
