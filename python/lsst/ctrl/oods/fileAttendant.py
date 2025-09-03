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
import shutil

from lsst.ctrl.oods.butlerAttendant import ButlerAttendant
from lsst.ctrl.oods.imageData import ImageData

LOGGER = logging.getLogger(__name__)


class FileAttendant(ButlerAttendant):
    """Processes files on behalf of a Gen3 Butler.

    Parameters
    ----------
    config: `dict`
        configuration of this butler ingester
    csc: `OodsCSC`
        Observatory Operations Data Service Commandable SAL component
    """

    def __init__(self, config, csc=None):
        super().__init__(
            butler_config=config.file_ingester.butler,
            csc=csc,
        )

        self.staging_dir = config.file_ingester.staging_directory
        self.bad_file_dir = config.file_ingester.bad_file_directory

    def rawexposure_info(self, data):
        """Return a sparsely initialized dictionary

        Parameters
        ----------
        data: `RawFileData`
            information about the raw file

        Returns
        -------
        info: `dict`
            Dictionary with file name and dataId elements
        """
        info = super().rawexposure_info(data=data)
        info["FILENAME"] = os.path.basename(data.filename.ospath)
        return info

    def undef_metadata(self, filename):
        """Return a sparsely initialized metadata dictionary

        Parameters
        ----------
        filename: `str`
            name of the file specified by ingest

        Returns
        -------
        info: `dict`
            Dictionary containing file name and placeholders
        """
        info = super().undef_metadata(filename=filename)
        info["FILENAME"] = os.path.basename(filename)
        return info

    def print_msg(self, msg):
        """Print message dictionary - used if a CSC has not been created

        Parameters
        ----------
        msg: `dict`
            Dictionary to print
        """

        LOGGER.info(f"would have sent {msg=}")

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
            LOGGER.info("file %s successfully ingested", dataset.path.ospath)
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
            real_file = f.filename.ospath
            self._move_file_to_bad_dir(real_file)
            cause = self.extract_cause(exc)
            info = self.rawexposure_info(f)
            self.transmit_status(info, code=1, description=f"ingest failure: {cause}")

    def on_metadata_failure(self, filename, exc):
        """Callback used on metadata extraction failure. Used to transmit
        unsuccessful data ingestion status

        Parameters
        ----------
        filename: `ButlerURI`
            ButlerURI that failed in ingest
        exc: `Exception`
            Exception which explains what happened
        """
        real_file = filename.ospath
        self._move_file_to_bad_dir(real_file)

        cause = self.extract_cause(exc)
        info = self.undef_metadata(real_file)
        self.transmit_status(info, code=2, description=f"metadata failure: {cause}")

    def _move_file_to_bad_dir(self, filename):
        bad_dir = self.create_bad_dirname(self.bad_file_dir, self.staging_dir, filename)
        try:
            shutil.move(filename, bad_dir)
        except Exception as e:
            LOGGER.info("Failed to move %s to %s: %s", filename, bad_dir, e)
