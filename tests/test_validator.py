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
from lsst.ctrl.oods.validator import Validator
import lsst.utils.tests
import logging
import unittest
import yaml


class ValidatorTestCase(lsst.utils.tests.TestCase):
    """Test cache cleaning"""

    def setUp(self):
        self.logger = logging.getLogger("test_validator")

    def testIntervalBlock(self):

        # check a valid interval block
        interval = """test:
                        days: 1,
                        hours: 1,
                        minutes: 1,
                        seconds: 1"""

        config = yaml.safe_load(interval)
        val = Validator(self.logger)
        isValid = val.checkIntervalBlock("test", "fake", config)
        self.assertTrue(isValid)

        # check a invalid interval block
        badInterval = {}
        config = {}

        config["test"] = badInterval
        isValid = val.checkIntervalBlock("test", "fake", config)
        self.assertFalse(isValid)

    def testValidator(self):

        # create a complete, valid OODS YAML description
        configStr = """oods:
                         ingester:
                           directories:
                             - data
                           butler:
                             class:
                               import: lsst.ctrl.oods.gen2ButlerIngester
                               name: Gen2ButlerIngester
                             repoDirectory : repo
                           batchSize: 20
                           scanInterval:
                             days: 0
                             hours: 0
                             minutes: 0
                             seconds: 10
                         cacheCleaner:
                           directories:
                             - repo/raw
                           scanInterval:
                             days: 0
                             hours: 0
                             minutes: 0
                             seconds: 30
                           filesOlderThan:
                             days: 30
                             hours: 0
                             minutes: 0
                             seconds: 0
                           directoriesEmptyForMoreThan:
                             days: 10
                             hours: 0
                             minutes: 0
                             seconds: 0"""

        config = yaml.safe_load(configStr)

        val = Validator(self.logger)
        isValid = val.verify(config)
        self.assertTrue(isValid)

    def testEmptyConfigYaml(self):
        # create bad YAML, and check to see if the errors are all
        # flagged correctly

        # configuration set to None
        val = Validator(self.logger)
        isValid = val.verify(None)

        self.assertFalse(isValid)
        self.verifyMissingElement(val, "ingester")
        self.verifyMissingElement(val, "cacheCleaner")

        # completely empty config
        config = {}
        val = Validator(self.logger)
        isValid = val.verify(config)

        self.assertFalse(isValid)
        self.verifyMissingElement(val, "ingester")
        self.verifyMissingElement(val, "cacheCleaner")

        config["some"] = "nonsense"
        val = Validator(self.logger)
        isValid = val.verify(config)

        self.assertFalse(isValid)
        self.verifyMissingElement(val, "ingester")
        self.verifyMissingElement(val, "cacheCleaner")

    def testBadIngesterBlock(self):
        # check ingester block

        configStr = """oods:
                         foo: bar"""
        config = yaml.safe_load(configStr)

        val = Validator(self.logger)
        isValid = val.verify(config)

        self.assertFalse(isValid)
        self.verifyMissingElement(val, "ingester")

        configStr = """oods:
                         ingester:
                            foo: bar"""
        config = yaml.safe_load(configStr)

        val = Validator(self.logger)
        isValid = val.verify(config)

        self.assertFalse(isValid)
        self.verifyMissingElement(val, "ingester:directories")
        self.verifyMissingElement(val, "ingester:butler")
        self.verifyMissingElement(val, "ingester:batchSize")
        self.verifyMissingElement(val, "ingester:scanInterval")
        self.verifyMissingElement(val, "cacheCleaner")

    def testMissingIngesterDirectory(self):
        # check ingester:directories
        configStr = """oods:
                         ingester:
                            directories:"""
        config = yaml.safe_load(configStr)
        val = Validator(self.logger)
        isValid = val.verify(config)

        self.assertFalse(isValid)
        self.verifyMissingElementValue(val, "ingester:directories")

        configStr = """oods:
                         ingester:
                            directories:
                            foo: bar"""
        config = yaml.safe_load(configStr)
        val = Validator(self.logger)
        isValid = val.verify(config)

        self.assertFalse(isValid)
        self.verifyMissingElementValue(val, "ingester:directories")

    def testValidButlerBlock(self):
        # check ingester:butler
        configStr = """oods:
                         ingester:
                            directories:
                                - dir
                            batchSize: 20
                            butler:
                                foo: bar
                            scanInterval:
                                foo: bar"""
        config = yaml.safe_load(configStr)
        val = Validator(self.logger)
        isValid = val.verify(config)

        self.assertFalse(isValid)
        self.verifyMissingElement(val, "butler:class")
        self.verifyMissingElement(val, "butler:repoDirectory")

        prefix = "ingester:scanInterval"
        self.verifyMissingElement(val, "%s:%s" % (prefix, "days"))
        self.verifyMissingElement(val, "%s:%s" % (prefix, "hours"))
        self.verifyMissingElement(val, "%s:%s" % (prefix, "minutes"))
        self.verifyMissingElement(val, "%s:%s" % (prefix, "seconds"))

    def testValidButlerClassBlock(self):
        configStr = """oods:
                         ingester:
                            directories:
                                - dir
                            batchSize: 20
                            butler:
                                class:
                                    foo: bar
                                repoDirectory: repo
                            scanInterval:
                                days: 1
                                hours: 2
                                minutes: 3
                                seconds: 4"""
        config = yaml.safe_load(configStr)

        val = Validator(self.logger)
        isValid = val.verify(config)

        self.assertFalse(isValid)
        self.verifyMissingElement(val, "butler:class:name")
        self.verifyMissingElement(val, "butler:class:import")

    def testMissingCacheCleanerBlock(self):
        configStr = """oods:
                         ingester:
                            directories:
                                - dir
                            batchSize: 20
                            butler:
                                class:
                                    import: somefile
                                    name: someobject
                                repoDirectory: repo
                            scanInterval:
                                days: 1
                                hours: 2
                                minutes: 3
                                seconds: 4"""
        config = yaml.safe_load(configStr)

        val = Validator(self.logger)
        isValid = val.verify(config)

        self.assertFalse(isValid)
        self.verifyMissingElement(val, "cacheCleaner")
        self.assertEqual(len(val.missingElements), 1)

        # check cacheCleaner
        configStr = """oods:
                         ingester:
                            directories:
                                - dir
                            batchSize: 20
                            butler:
                                class:
                                    import: somefile
                                    name: someobject
                                repoDirectory: repo
                            scanInterval:
                                days: 1
                                hours: 2
                                minutes: 3
                                seconds: 4
                         cacheCleaner:
                            foo: bar"""
        config = yaml.safe_load(configStr)
        val = Validator(self.logger)
        isValid = val.verify(config)

        self.assertFalse(isValid)
        self.verifyMissingElement(val, "cacheCleaner:directories")
        self.verifyMissingElement(val, "cacheCleaner:scanInterval")
        self.verifyMissingElement(val, "cacheCleaner:filesOlderThan")
        self.verifyMissingElement(val, "cacheCleaner:directoriesEmptyForMoreThan")

    def testDirectoriesInCacheCleanerBlock(self):
        configStr = """oods:
                         ingester:
                            directories:
                                - dir
                            batchSize: 20
                            butler:
                                class:
                                    import: somefile
                                    name: someobject
                                repoDirectory: repo
                            scanInterval:
                                days: 1
                                hours: 2
                                minutes: 3
                                seconds: 4
                         cacheCleaner:
                            scanInterval:
                                days: 0
                                hours: 0
                                minutes: 0
                                seconds: 10
                            filesOlderThan:
                                days: 30
                                hours: 0
                                minutes: 0
                                seconds: 0
                            directoriesEmptyForMoreThan:
                                days: 1
                                hours: 0
                                minutes: 0
                                seconds: 0"""
        config = yaml.safe_load(configStr)
        val = Validator(self.logger)
        isValid = val.verify(config)

        self.assertFalse(isValid)
        self.verifyMissingElement(val, "cacheCleaner:directories")

        configStr = """oods:
                         ingester:
                            directories:
                                - dir
                            batchSize: 20
                            butler:
                                class:
                                    import: somefile
                                    name: someobject
                                repoDirectory: repo
                            scanInterval:
                                days: 1
                                hours: 2
                                minutes: 3
                                seconds: 4
                         cacheCleaner:
                            directories:
                            scanInterval:
                                days: 0
                                hours: 0
                                minutes: 0
                                seconds: 10
                            filesOlderThan:
                                days: 30
                                hours: 0
                                minutes: 0
                                seconds: 0
                            directoriesEmptyForMoreThan:
                                days: 1
                                hours: 0
                                minutes: 0
                                seconds: 0"""
        config = yaml.safe_load(configStr)

        val = Validator(self.logger)
        isValid = val.verify(config)

        self.assertFalse(isValid)
        self.verifyMissingElementValue(val, "cacheCleaner:directories")

    def verifyMissingElementValue(self, validator, name):
        self.assertTrue(name in validator.missingValues)

    def verifyMissingElement(self, validator, name):
        self.assertTrue(name in validator.missingElements)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
