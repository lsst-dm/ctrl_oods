#########
ctrl_oods
#########

``ctrl_oods`` is a package in the `LSST Science Pipelines <https://pipelines.lsst.io>`_.

.. Add a brief (few sentence) description of what this package provides.

The Observatory Operations Data Service watches for files in one or more directories, and then ingests them into an LSST Butler repository.   Files are expired from the repository at specified intervals.

Usage:  oods.py [[-c config][-y config] [-v]

Options:

.. code:: bash

 -c config use specified OODS YAML configuration file
 -y config validate YAML configuration file
 -v give verbose output

Set up and usage
----------------

1) create the Gen2 Butler repository:

.. code:: bash

 mkdir repo
 echo "lsst.obs.lsst.auxTel.AuxTelMapper" > repo/_mapper

2) Edit the YAML configuration file.  The default example is located in:

.. code:: bash

    $CTRL_OODS_DIR/etc/oods.yaml

3) Run the OODS:

.. code:: bash

    $CTRL_OODS_DIR/oods.py -c oods.yaml

NOTE:  if you run the OODS without modifying the directory paths, it expects to scan for files in the directory in which the OODS has been invoked.   It will scan the directory "data" and use the Gen2 Butler repository "repo" by default.

Configuration
-------------

The OODS is configured with the following YAML file:

.. code:: yaml

 defaultInterval: &interval
     days: 0
     hours: 0
     minutes: 0
     seconds: 0
 
 ingester:
     directories:
         - data
     butler:
         class:
             import : lsst.ctrl.oods.gen2ButlerIngester
             name : Gen2ButlerIngester
         repoDirectory : repo
     batchSize: 20
     scanInterval:
         <<: \*interval
         seconds: 10
 
 cacheCleaner:
     directories:
         - repo/raw
     scanInterval:
         <<: \*interval
         seconds: 30
     filesOlderThan:
         <<: \*interval
         days: 30
     directoriesEmptyForMoreThan:
         <<: \*interval
         days: 1

The "defaultInterval" block is used as shorthand for the intervals used throughout the rest of the YAML configuration.

The "ingester" block
--------------------

This has four sections:  directories, butler, scanInterval, and batchSize.

The "directories" section takes a list of directories to watch.   By default this watches the "data" directory which is expected to be in the same directory in which the OODS runs.

The "butler" section specified which type of LSST Butler to run, and the repository to use.  By default, it uses an object called Gen2ButlerIngester, specified by the import "lsst.ctrl.oods.gen2ButlerIngester".   If you write your own ingestion object, follow the pattern specified in this file.   By default the "repo" directory is expected to be in the same directory which the OODS runs.  This butler repository is expected to be set up properly (see below) before the first invocation of the OODS.

The "scanInterval" section specifies the frequency at which to scan the "directories" specified above.  In the example, it scans every 10 seconds.

The "batchSize" is set to the number of files to attempt to ingest at once.  The current version (0.1) of the OODS calls the obs_lsst package's "ingestImages.py" script, and it is possible to overload the command line beyond it's limit if too many files are specified on the command line at one time.  To prevent this, files are ingested in batches of "batchSize" or less.   Note that all files that are found when an ingestion requested at that "scanInterval" will attempt to be ingested.  Also note that in future versions, (Gen3 Butler), there will be
a programmatic interface to the butler ingestion code, so this parameter will 
likely be deprecated.

The "cacheCleaner" block
------------------------

This has four sections: directories, scanInterval, filesOlderThan and directoriesEmptyForMoreThan.

The "directories" section specifies the location of the ingested Butler files to clean up.   By default this is "repo/raw" and is expected to be in the same directory in which the OODS runs.

The "scanInterval" section specifies the frequency at which to scan the "directories" specified above.  In the example, it scans every 30 seconds.

The "filesOlderthan" section specifies how old files must be in order for them to be considered for removal.   This is checked against the last modification date of the file.  In this example, the file must be at least 30 days old to be considered for removal.

The "directoriesEmptyForMoreThan" section specifies how long directories must be empty for before they are to be considered for removal.   This is checked against the last modification date of the directory.  In this example, the directory must be at least  1 day old and empty to be considered for removal.
