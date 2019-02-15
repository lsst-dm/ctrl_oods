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


class Validator(object):
    """Validate a configuration data structure.
    @param config: a configuration data structure
    @param logger: python logging
    """

    def __init__(self, logger):
        self.isValid = True
        self.logger = logger
        self.missingElements = []
        self.missingValues = []

    def verify(self, config):
        """Validate a configuration, emitting messages about errors.
        """
        self.isValid = True
        self.missingElements = []
        self.missingValues = []

        if config is None:
            self.missingElement("ingester")
            self.missingElement("cacheCleaner")
            return self.isValid

        if "oods" not in config:
            self.missingElement("ingester")
            self.missingElement("cacheCleaner")
            return self.isValid

        self.oodsConfig = config["oods"]

        configName = "ingester"
        if configName not in self.oodsConfig:
            self.missingElement(configName)

        else:
            ingesterConfig = self.oodsConfig[configName]
            if "directories" in ingesterConfig:
                dirs = ingesterConfig["directories"]
                if dirs is None:
                    self.missingValue("ingester:directories")
            else:
                self.missingElement("ingester:directories")
            if "butler" in ingesterConfig:
                butlerConfig = ingesterConfig["butler"]
                if "class" in butlerConfig:
                    classConfig = butlerConfig["class"]
                    if "import" not in classConfig:
                        self.missingElement("butler:class:import")
                    if "name" not in classConfig:
                        self.missingElement("butler:class:name")
                else:
                    self.missingElement("butler:class")
                if "repoDirectory" not in butlerConfig:
                    self.missingElement("butler:repoDirectory")
            else:
                self.missingElement("ingester:butler")

            if "batchSize" not in ingesterConfig:
                self.missingElement("ingester:batchSize")

            self.checkIntervalBlock("scanInterval", configName, ingesterConfig)

        configName = "cacheCleaner"
        if configName not in self.oodsConfig:
            self.missingElement(configName)
        else:
            cacheConfig = self.oodsConfig[configName]
            name = "directories"
            if name in cacheConfig:
                dirs = cacheConfig[name]
                if dirs is None:
                    self.missingValue("%s:%s" % (configName, name))
            else:
                self.missingElement("%s:directories" % configName)

            self.checkIntervalBlock("scanInterval", configName, cacheConfig)
            self.checkIntervalBlock("filesOlderThan", configName, cacheConfig)
            self.checkIntervalBlock("directoriesEmptyForMoreThan",
                                    configName, cacheConfig)
        return self.isValid

    def checkIntervalBlock(self, name, configName, config):
        """Check that an Interval block is valid
        @param name: configuration element name
        @param configName: configuration block name
        @param config: a configuration data structure
        """
        interval = None
        if name not in config:
            self.missingElement("%s:%s" % (configName, name))
            return self.isValid

        interval = config[name]
        if "days" not in interval:
            self.missingElement("%s:%s:days" % (configName, name))
        if "hours" not in interval:
            self.missingElement("%s:%s:hours" % (configName, name))
        if "minutes" not in interval:
            self.missingElement("%s:%s:minutes" % (configName, name))
        if "seconds" not in interval:
            self.missingElement("%s:%s:seconds" % (configName, name))
        return self.isValid

    def missingElement(self, element):
        """Emit a message about a missing configuration element
        @param element: missing element name
        """
        if self.logger is not None:
            self.logger.error("missing '%s'" % element)

        # also add this name to the missing elements list.  We can use
        # this to programmatically identify missing elements.
        self.isValid = False
        self.missingElements.append(element)

    def missingValue(self, element):
        """Emit a message about a missing value of an  element
        @param element: element name which is missing a value
        """
        if self.logger is not None:
            self.logger.error("'%s' is missing a value" % element)

        # also add this name to the missing values list.  We can use
        # this to programmatically identify elements which are missng values.
        self.isValid = False
        self.missingValues.append(element)
