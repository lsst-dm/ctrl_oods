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

from lsst.ctrl.oods.butlerAttendant import ButlerAttendant
from lsst.ctrl.oods.imageData import ImageData

LOGGER = logging.getLogger(__name__)


class MessageAttendant(ButlerAttendant):
    """Processes files from S3 into the Butler.

    Parameters
    ----------
    config: `dict`
        configuration of this butler ingester
    csc: `OodsCSC`
        Observatory Operations Data Service Commandable SAL component
    """

    def __init__(self, config, csc=None):
        super().__init__(
            butler_config = config.message_ingester.butler,
            csc=csc,
        )


    def rawexposure_info(self, data):
        """Return a sparsely initialized dictionary

        Parameters
        ----------
        data: `RawFileData`
            information about the raw file

        Returns
        -------
        info: `dict`
            Dictionary with file name and data_id elements
        """
        info = super().rawexposure_info(data=data)
        info["FILENAME"] = f"{data.filename}"
        return info

    def on_success(self, datasets):
        """Callback used on successful ingest. Used to transmit
        successful data ingestion status

        Parameters
        ----------
        datasets: `list`
            list of DatasetRefs
        """
        self.definer_run(datasets)
        for dataset in datasets:
            LOGGER.info("file %s successfully ingested", dataset.path)
            image_data = ImageData(dataset)
            LOGGER.debug("image_data.get_info() = %s", image_data.get_info())
            self.transmit_status(image_data.get_info(), code=0, description="file ingested")

    def on_ingest_failure(self, exposures, exc):
        """Callback used on ingest failure. Used to transmit
        unsuccessful data ingestion status

        Parameters
        ----------
        exposures: `RawExposureData`
            exposures that failed in ingest
        exc: `Exception`
            Exception which explains what happened

        """
        for f in exposures.files:
            cause = self.extract_cause(exc)
            info = self.rawexposure_info(f)
            self.transmit_status(info, code=1, description=f"ingest failure: {cause}")

    def on_metadata_failure(self, resource_path, exc):
        """Callback used on metadata extraction failure. Used to transmit

        Parameters
        ----------
        filename: `ResourcePath`
            ResourcePath that failed in ingest
        exc: `Exception`
            Exception which explains what happened
        """
        real_file = f"{resource_path}"
        cause = self.extract_cause(exc)
        info = self.undef_metadata(real_file)
        self.transmit_status(info, code=2, description=f"metadata failure: {cause}")
