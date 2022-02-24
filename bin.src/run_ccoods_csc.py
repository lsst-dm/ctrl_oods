#!/usr/bin/env python
import asyncio
import logging
import lsst.log as lsstlog
from lsst.ctrl.oods.ccoods_csc import CcOodsCsc

lsstlog.usePythonLogging()

LOGGER = logging.getLogger(__name__)
F = '%(levelname) -10s %(asctime)s.%(msecs)03dZ %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s'
logging.basicConfig(level=logging.INFO, format=(F), datefmt="%Y-%m-%d %H:%M:%S")

asyncio.run(CcOodsCsc.amain(index=None))
