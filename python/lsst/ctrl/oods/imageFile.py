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


class ImageFile():
    def __init__(self, filename):
        base_name = os.path.basename(filename)
        name = base_name.split('.')
        prefix = name[0]

        if prefix.find('-') == -1:
            # CC_O_20210709_000012_R22S21
            camera_abbrev = prefix.partition('_')[0]
            s = prefix.rpartition('_')
            obsid = s[0]
            raft_sensor = s[2]
        else:
            # CC_O_20210709_000012-R22S21
            camera_abbrev = prefix.partition('_')[0]
            s = prefix.split('-')
            obsid = s[0]
            raft_sensor = s[1]
        raft = raft_sensor[1:3]
        sensor = raft_sensor[4:6]

        if camera_abbrev == 'AT':
            self.archiver = "ATArchiver"
            self.camera = "LATISS"
        elif camera_abbrev == 'CC':
            self.archiver = "CCArchiver"
            self.camera = "COMCAM"
        else:
            self.archiver = "MTArchiver"
            self.camera = "ALLSKY"

        self.filename = filename
        self.obsid = obsid
        self.raft = raft
        self.sensor = sensor
