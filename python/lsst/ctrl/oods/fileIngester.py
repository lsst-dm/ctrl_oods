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
from lsst.ctrl.oods.directoryScanner import DirectoryScanner


class FileIngester(object):
    """Ingest files into the butler specified in the configuration
    """

    def __init__(self, config, verbose=False):
        self.config = config
        self.verbose = verbose

        self.scanner = DirectoryScanner(config)

        butlerConfig = config["butler"]

        classConfig = butlerConfig["class"]

        # create the butler
        importFile = classConfig["import"]
        name = classConfig["name"]

        butlerClass = self.createClass(importFile, name)
        self.butler = butlerClass(butlerConfig["repoDirectory"])

        self.batchSize = config["batchSize"]

    def runTask(self, verbose=False):
        """scan to get the files, and ingest them in batches
        """
        files = self.scanner.getAllFiles()
        self.butler.ingest(files, self.batchSize, self.verbose)

    def createClass(self, importFile, name):
        """ create a class, given the source name and the object name
        """
        module = __import__(importFile, globals(), locals(), [name], 0)
        classobj = getattr(module, name)
        return classobj
