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
import subprocess


class Gen2ButlerIngester(object):
    """ Processes files for ingestion into a Gen2 Butler
    """
    def __init__(self, repo):
        self.precmd = ['ingestImages.py', repo, '--ignore-ingested', '--mode', 'move']
        pass

    def ingest(self, files, batchSize, verbose=False):
        """ ingest files in 'batchSize' increments
        """
        if len(files) == 0:
            return

        #
        # since this is being handed off from to a command line, subprocess
        # split this up into more manageable chunks such that we don't go
        # over the line limit for subprocesses.
        #
        if batchSize == -1:
            if verbose:
                print("ingesting ", files)
            cmd = self.precmd + files
            subprocess.call(cmd)
            return
        # this should calculate to just below some high water mark for
        # better efficiency, but splitting on batchSize will do for now.
        chunks = [files[x:x+batchSize] for x in range(0, len(files), batchSize)]
        for chunk in chunks:
            if verbose:
                print("ingesting ", chunk)
            cmd = self.precmd + chunk
            subprocess.call(cmd)
