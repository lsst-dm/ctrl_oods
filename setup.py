from setuptools import setup, find_namespace_packages
  
setup(
    name="ctrl_oods",
    version="1.0.0",
    description="Observatory Operations Data Service daemon",
    package_dir={"": "python"},
    packages=find_namespace_packages(where="python"),
    scripts=["bin/oods.py"]
    license="GPL",
    project_urls={
        "Bug Tracker": "https://jira.lsstcorp.org/secure/Dashboard.jspa",
        "Source Code": "https://github.com/lsst-dm/ctrl_oods",
    }
)
