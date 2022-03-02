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
import os

LOGGER = logging.getLogger(__name__)


class ImageData:
    """Encapsulate information extracted from an DatasetRef"""

    def __init__(self, dataset):
        """Initialize the object using DatasetRef

        Parameters
        ----------
        dataset: `DatasetRef`
            The DatasetRef to extract information from
        """
        self.info = {"CAMERA": "", "RAFT": "", "SENSOR": "", "OBSID": ""}
        try:
            self.info["FILENAME"] = os.path.basename(dataset.path.ospath)
        except Exception as e:
            LOGGER.info("Failed to extract filename for %s: %s", dataset, e)
            return

        try:
            refs = dataset.refs
            ref = refs[0]  # OODS only gets a single element in the list
            if ref.dataId.hasRecords() is False:
                LOGGER.info("Failed to extract data for %s; no records", dataset)
                return

            records = ref.dataId.records

            if "instrument" in records:
                instrument = records["instrument"].toDict()
                self.info["CAMERA"] = instrument.get("name", "UNDEF")

            if "detector" in records:
                detector = records["detector"].toDict()
                self.info["RAFT"] = detector.get("raft", "R??")
                self.info["SENSOR"] = detector.get("name_in_raft", "S??")

            if "exposure" in records:
                exposure = records["exposure"].toDict()
                self.info["OBSID"] = exposure.get("obs_id", "??")
        except Exception as e:
            LOGGER.info("Failed to extract data for %s: %s", dataset, e)

    def get_info(self):
        return self.info

    def __repr__(self) -> str:
        return str(self.info)
