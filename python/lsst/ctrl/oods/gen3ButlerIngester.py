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

import asyncio
import collections
from lsst.ctrl.oods.butlerIngester import ButlerIngester
from lsst.ctrl.oods.timeInterval import TimeInterval
from lsst.daf.butler import Butler
from lsst.obs.base.ingest import RawIngestTask, RawIngestConfig
from lsst.obs.base.utils import getInstrument
from astropy.time import Time
from astropy.time import TimeDelta
import astropy.units as u


class Gen3ButlerIngester(ButlerIngester):
    """Processes files for ingestion into a Gen3 Butler.

    Parameters
    ----------
    config: `dict`
        configuration of this butler ingester
    """
    def __init__(self, config):
        self.config = config
        repo = self.config["repoDirectory"]
        instrument = self.config["instrument"]
        self.scanInterval = self.config["scanInterval"]
        self.olderThan = self.config["filesOlderThan"]
        self.collections = self.config["collections"]

        try:
            butlerConfig = Butler.makeRepo(repo)
        except FileExistsError:
            butlerConfig = repo

        instr = getInstrument(instrument)
        run = instr.makeDefaultRawIngestRunName()
        opts = dict(run=run, writeable=True, collections=self.collections)
        self.butler = Butler(butlerConfig, **opts)

        # Register an instrument.
        instr.register(self.butler.registry)

        cfg = RawIngestConfig()
        cfg.transfer = "direct"
        self.task = RawIngestTask(config=cfg, butler=self.butler)

    def ingest(self, filename):
        """Ingest a file into a butler

        Parameters
        ----------
        filename: `str`
            file name of the file to ingest
        """

        # Ingest image.
        self.task.run([filename])

    def getName(self):
        """Return the name of this ingester

        Returns
        -------
        ret: `str`
            name of this ingester
        """
        return "gen3"

    async def run_task(self):
        """run the clean() method at the configured interval
        """
        seconds = TimeInterval.calculateTotalSeconds(self.scanInterval)
        while True:
            self.clean()
            await asyncio.sleep(seconds)

    def clean(self):
        """Remove all the datasets in the butler that
        were ingested before the configured Interval
        """

        # calculate the time value which is Time.now - the
        # "olderThan" configuration
        t = Time.now()
        interval = collections.namedtuple("Interval", self.olderThan.keys())(*self.olderThan.values())
        td = TimeDelta(interval.days*u.d + interval.hours * u.h +
                       interval.minutes*u.min + interval.seconds*u.s)
        t = t - td

        # get the datasets
        ref = set(self.butler.registry.queryDatasets(datasetType=...,
                                                     collections=self.collections,
                                                     where="ingest_date < ref_date",
                                                     bind={"ref_date": t}))

        # References outside of the Butler's datastore
        # need to be cleaned up, since the Butler will
        # not delete those file artifacts.
        #
        # retrieve the URI,  prune the dataset from
        # the Butler, and if the URI was available,
        # remove it.
        for x in ref:
            print(f"removing {x}")

            uri = None
            try:
                uri = self.butler.getURI(x, collections=x.run)
            except Exception as e:
                print(f"butler is missing uri for {x}: {e}")

            if uri is not None:
                uri.remove()

        self.butler.pruneDatasets(ref, purge=True, unstore=True)
