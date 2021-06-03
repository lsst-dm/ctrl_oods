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

import logging

LOGGER = logging.getLogger(__name__)


class Validator(object):
    """Validate a configuration data dictionary
    """

    def __init__(self):
        self.isValid = True
        self.missingElements = []
        self.missingValues = []

    def verify(self, config):
        """Validate a configuration, emitting messages about errors.

        Parameters
        ----------
        config: `dict`
            a configuration data dictionary
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
            if "forwarderStagingDirectory" in ingesterConfig:
                dirs = ingesterConfig["forwarderStagingDirectory"]
                if dirs is None:
                    self.missingValue("ingester:forwarderStagingDirectory")
            else:
                self.missingElement("ingester:forwarderStagingDirectory")
            if "butlers" in ingesterConfig:
                butlerEntries = ingesterConfig["butlers"]
                for entry in butlerEntries:
                    butlerConfig = entry["butler"]
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
                self.missingElement("ingester:butlers")

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

        Parameters
        ----------
        name: `str`
            configuration element name
        configName: `str`
            configuration block name
        config: `dict`
            configuration data dictionary
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

        Parameters
        ----------
        element: `str`
            missing element name
        """
        LOGGER.error("missing '%s'" % element)

        # also add this name to the missing elements list.  We can use
        # this to programmatically identify missing elements.
        self.isValid = False
        self.missingElements.append(element)

    def missingValue(self, element):
        """Emit a message about a missing value of an  element

        Parameters
        ----------
        element: `str`
            element name which is missing a value
        """
        LOGGER.error("'%s' is missing a value" % element)

        # also add this name to the missing values list.  We can use
        # this to programmatically identify elements which are missng values.
        self.isValid = False
        self.missingValues.append(element)
