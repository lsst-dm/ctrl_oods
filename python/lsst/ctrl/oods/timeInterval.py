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
SECONDS_PER_DAY = 86400
SECONDS_PER_HOUR = 3600
SECONDS_PER_MINUTE = 60


class TimeInterval(object):
    """representation of a time interval from a configuration
    """

    @staticmethod
    def calculateTotalSeconds(config):
        """calculate the number of seconds represented by this configuration
        """
        days = config["days"]
        hours = config["hours"]
        minutes = config["minutes"]
        seconds = config["seconds"]

        total = days * SECONDS_PER_DAY
        total = total + (hours * SECONDS_PER_HOUR)
        total = total + (minutes * SECONDS_PER_MINUTE)
        total = total + seconds
        return total
