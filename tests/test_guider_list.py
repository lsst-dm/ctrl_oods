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


import time

import lsst.utils.tests
from lsst.ctrl.oods.guiderEntry import GuiderEntry
from lsst.ctrl.oods.guiderList import GuiderList
from lsst.resources import ResourcePath


class GuiderListTestCase(lsst.utils.tests.TestCase):

    def testGuiderList(self):

        guider_list = GuiderList()

        a_file = ResourcePath("a.txt")
        b_file = ResourcePath("b.txt")
        c_file = ResourcePath("c.txt")
        d_file = ResourcePath("d.txt")
        e_file = ResourcePath("e.txt")

        guider_list.append(GuiderEntry(guider_resource_path=a_file))
        time.sleep(1)
        guider_list.append(GuiderEntry(guider_resource_path=b_file))
        time.sleep(1)
        guider_list.append(GuiderEntry(guider_resource_path=c_file))

        time.sleep(3)
        guider_list.append(GuiderEntry(guider_resource_path=d_file))
        guider_list.append(GuiderEntry(guider_resource_path=e_file))

        removed_list = guider_list.purge_old_entries(10)
        self.assertEqual(len(removed_list), 0)
        time.sleep(2)

        removed_list = guider_list.purge_old_entries(3.0)
        self.assertEqual(len(removed_list), 3)

        removed_names = removed_list.get_guider_resource_paths()
        self.assertTrue(len(removed_names), 3)
        self.assertTrue(all(item in removed_names for item in [a_file, b_file, c_file]))

        names = guider_list.get_guider_resource_paths()
        self.assertTrue(len(names), 2)
        self.assertTrue(all(item in names for item in [d_file, e_file]))

        guider_list.remove_by_name(d_file)
        self.assertTrue(len(guider_list), 1)
        self.assertTrue(all(item in names for item in [e_file]))
