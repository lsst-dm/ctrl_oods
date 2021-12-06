#!/usr/bin/env python
import asyncio
import logging
import lsst.log as lsstlog
from lsst.ctrl.oods.atoods_csc import ATOodsCSC

lsstlog.usePythonLogging()

LOGGER = logging.getLogger(__name__)
F = '%(levelname) -10s %(asctime)s.%(msecs)03dZ %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s'
logging.basicConfig(level=logging.INFO, format=(F), datefmt="%Y-%m-%d %H:%M:%S")

asyncio.run(ATOodsCSC.amain(index=None))
