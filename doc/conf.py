"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documentation builds.
"""

from documenteer.sphinxconfig.stackconf import build_package_configs
import lsst.ctrl.oods


_g = globals()
_g.update(build_package_configs(
    project_name='ctrl_oods',
    version=lsst.ctrl.oods.version.__version__))
