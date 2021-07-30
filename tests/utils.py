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


def removeEntries(directory):
    if os.path.exists(directory) is False:
        return
    if os.path.isdir(directory) is False:
        os.unlink(directory)
        return
    with os.scandir(directory) as entries:
        for ent in entries:
            if os.path.isdir(ent):
                removeEntries(ent)
            else:
                os.unlink(ent)
    os.rmdir(directory)
