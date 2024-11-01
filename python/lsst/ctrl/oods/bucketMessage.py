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

import json
import logging

LOGGER = logging.getLogger(__name__)


class BucketMessage(object):
    """Report on new messages

    Parameters
    ----------
    message: `str`
        json string
    """

    def __init__(self, message):
        self.message = message

    def extract_urls(self):
        """Extract object IDs from an S3 notification.

        If one record is invalid, an error is logged but the function tries to
        process the remaining records.

        Yields
        ------
        oid : `str`
            The filename referred to by each message.
        """
        LOGGER.debug(f"extracting from {self.message}")
        msg = json.loads(self.message)
        for record in msg["Records"]:
            try:
                bucket_name = record["s3"]["bucket"]["name"]
                key = record["s3"]["object"]["key"]
                url = f"s3://{bucket_name}/{key}"
                LOGGER.debug(f"yielding {url}")
                yield url
            except KeyError as e:
                LOGGER.error(f"Invalid msg: Couldn't find key in {record=}")
                raise e
