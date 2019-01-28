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

from lsst.ctrl.oods.validator import Validator
import lsst.utils.tests


def setup_module(module):
    lsst.utils.tests.init()


class ValidatorTestCase(lsst.utils.tests.TestCase):
    """Test cache cleaning"""

    def testIntervalBlock(self):

        # check a valid interval block
        interval = {}
        interval["days"] = 1
        interval["hours"] = 1
        interval["minutes"] = 1
        interval["seconds"] = 1

        config = {}
        config["test"] = interval

        val = Validator(config)
        val.isValid = True
        val.checkIntervalBlock("test", "fake", config)
        self.assertTrue(val.isValid)

        # check a invalid interval block
        val.isValid = True
        badInterval = {}
        config = {}

        config["test"] = badInterval
        val.checkIntervalBlock("test", "fake", config)
        self.assertFalse(val.isValid)

    def testValidator(self):

        # create a complete, valid OODS YAML description
        config = {}

        ingester = {}
        ingester["directories"] = ["dir"]
        butler = {}
        butler["class"] = {"import": "file", "name": "object"}
        butler["repoDirectory"] = "dir"

        ingester["butler"] = butler
        ingester["batchSize"] = 20
        interval = {"days": 1, "hours": 0, "minutes": 0, "seconds": 0}
        ingester["scanInterval"] = interval

        config["ingester"] = ingester

        cacheCleaner = {}
        cacheCleaner["directories"] = ["dir"]

        scan = {"days": 0, "hours": 0, "minutes": 0, "seconds": 10}
        cacheCleaner["scanInterval"] = scan

        files = {"days": 0, "hours": 10, "minutes": 10, "seconds": 0}
        cacheCleaner["filesOlderThan"] = files

        dirs = {"days": 1, "hours": 10, "minutes": 0, "seconds": 0}
        cacheCleaner["directoriesEmptyForMoreThan"] = dirs

        config["cacheCleaner"] = cacheCleaner

        # verify that it was parsed without error
        val = Validator(config, True)
        val.verify()
        self.assertTrue(val.isValid)

    def testBadYaml(self):
        # create bad YAML, and check to see if the errors are all
        # flagged correctly

        # configuration set to None
        val = Validator(None)
        val.verify()
        self.verifyMissingElement(val, "ingester")
        self.verifyMissingElement(val, "cacheCleaner")

        # completely empty config
        config = {}
        val = Validator(config, True)
        val.verify()
        self.verifyMissingElement(val, "ingester")
        self.verifyMissingElement(val, "cacheCleaner")

        config["some"] = "nonsense"
        val.verify()

        self.verifyMissingElement(val, "ingester")
        self.verifyMissingElement(val, "cacheCleaner")

        # check ingester block
        ingester = {}
        ingester["foo"] = ["bar"]
        config = {}
        config["ingester"] = ingester

        val = Validator(config)
        val.verify()

        self.verifyMissingElement(val, "ingester:directories")
        self.verifyMissingElement(val, "ingester:butler")
        self.verifyMissingElement(val, "ingester:batchSize")
        self.verifyMissingElement(val, "ingester:scanInterval")
        self.verifyMissingElement(val, "cacheCleaner")

        # check ingester:directories
        ingester["directories"] = None
        val.verify()
        self.verifyMissingElementValue(val, "ingester:directories")

        val = Validator(config, True)
        ingester["directories"] = []
        val.verify()
        self.verifyMissingElementValue(val, "ingester:directories")

        # check ingester:butler
        ingester["directories"] = ["dir"]
        ingester["batchSize"] = 20
        butler = {}
        butler["foo"] = "bar"
        ingester["butler"] = butler

        scanInterval = {}
        scanInterval["foo"] = "bar"
        ingester["scanInterval"] = scanInterval

        val.verify()

        self.verifyMissingElement(val, "butler:class")
        self.verifyMissingElement(val, "butler:repoDirectory")

        prefix = "ingester:scanInterval"
        self.verifyMissingElement(val, "%s:%s" % (prefix, "days"))
        self.verifyMissingElement(val, "%s:%s" % (prefix, "hours"))
        self.verifyMissingElement(val, "%s:%s" % (prefix, "minutes"))
        self.verifyMissingElement(val, "%s:%s" % (prefix, "seconds"))

        scanInterval["days"] = 1
        scanInterval["hours"] = 2
        scanInterval["minutes"] = 3
        scanInterval["seconds"] = 4

        bclass = {}
        bclass["foo"] = "bar"
        butler["class"] = bclass
        butler["repoDirectory"] = "repo"

        val.verify()
        self.verifyMissingElement(val, "butler:class:name")
        self.verifyMissingElement(val, "butler:class:import")

        # check ingester:butler:class
        bclass["import"] = "somefile"
        bclass["name"] = "someobject"
        val.verify()
        self.verifyMissingElement(val, "cacheCleaner")
        self.assertEqual(len(val.missingElements), 1)

        # check cacheCleaner
        cacheCleaner = {}
        config["cacheCleaner"] = cacheCleaner
        val.verify()
        self.verifyMissingElement(val, "cacheCleaner:directories")
        self.verifyMissingElement(val, "cacheCleaner:scanInterval")
        self.verifyMissingElement(val, "cacheCleaner:filesOlderThan")
        self.verifyMissingElement(val, "cacheCleaner:directoriesEmptyForMoreThan")

        cacheCleaner["directories"] = None
        val.verify()
        self.verifyMissingElementValue(val, "cacheCleaner:directories")

    def verifyMissingElementValue(self, validator, name):
        self.assertTrue(name in validator.missingValues)

    def verifyMissingElement(self, validator, name):
        self.assertTrue(name in validator.missingElements)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass
