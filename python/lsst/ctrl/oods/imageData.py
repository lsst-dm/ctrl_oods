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
import logging

LOGGER = logging.getLogger(__name__)


class ImageData():
    """TODO:
    extract: obs_id, raft, name_in_raft, instrument
    map to: obs_id, raft, sensor, camera, and add archiver
    """
    def __init__(self, dataset):
        """Initiailize the object using DatasetRef
        """
        self.info = {"camera": "", "raft": "", "sensor": "", "obsid": ""}
        try:
            self.info["filename"] = os.path.basename(dataset.path.ospath)
        except Exception as e:
            LOGGER.info(f"Failed to extract filename for {dataset}: {e}")
            return

        try:
            refs = dataset.refs
            ref = refs[0]  # TODO: eech...should be a better way to do this
            if ref.dataId.hasRecords is False:
                LOGGER.info(f"Failed to extract data for {dataset}; no records")
                return

            records = ref.dataId.records

            instrument = records['instrument'].toDict()
            self.info["camera"] = instrument["name"]

            detector = records['detector'].toDict()
            self.info["raft"] = detector["raft"]
            self.info["sensor"] = detector["name_in_raft"]

            exposure = records['exposure'].toDict()
            self.info["obsid"] = exposure["obs_id"]
        except Exception as e:
            LOGGER.info(f"Failed to extract data for {dataset}: {e}")

    def __repr__(self) -> str:
        return str(self.info)
