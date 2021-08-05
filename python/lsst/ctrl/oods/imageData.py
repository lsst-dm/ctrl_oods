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


class ImageData():
    """TODO:
    extract: obs_id, raft, name_in_raft, instrument
    map to: obs_id, raft, sensor, camera, and add archiver
    """
    def __init__(self, refs):
        print(f"imagedata refs: {refs}")
        for ref in refs:
            print(f"...imagedata type = {type(ref.dataId)} ref.dataId = {ref.dataId}")
            for x in ref.dataId:
                print(f">>> {x} = {ref.dataId[x]}")
            if ref.dataId.hasRecords:
                for x in ref.dataId.records:
                    print(f"<<< record {x} = {ref.dataId.records[x]}")
                
        
