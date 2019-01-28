#
# LSST Data Management System
#
# Copyright 2008-2019  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#
SECONDS_PER_DAY = 86400
SECONDS_PER_HOUR = 3600
SECONDS_PER_MINUTE = 60


class TimeInterval(object):
    """representation of a time interval from a configuration
    """
    def __init__(self, config):
        self.days = config["days"]
        self.hours = config["hours"]
        self.minutes = config["minutes"]
        self.seconds = config["seconds"]

    def calculateTotalSeconds(self):
        """calculate the number of seconds represented by this configuration
        """
        total = self.days * SECONDS_PER_DAY
        total = total + (self.hours * SECONDS_PER_HOUR)
        total = total + (self.minutes * SECONDS_PER_MINUTE)
        total = total + self.seconds
        return total
