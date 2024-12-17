##############
ctrl_oods yaml
##############

The OODS is configured with a YAML file.   The YAML file specifies a "file_ingester" section, or an "message_ingester" section.  These are outlined below.

file_ingester
=============

.. code:: yaml

 defaultInterval: &interval
     days: 0
     hours: 0
     minutes: 0
     seconds: 0
 
 file_ingester:
    imageStagingDirectory: /tmp
    butlers: 
        - butler:
         collections:
            - LATISS/raw/all
         cleanCollections:
             - collection: FARG/raw/all
               filesOlderThan:
                   <<: *interval
                   days: 20
             - collection: FARG/runs/quickLook
               filesOlderThan:
                   <<: *interval
                   days: 30
         instrument: lsst.obs.lsst.LsstComCam
         stagingDirectory: staging
         repoDirectory : repo
         scanInterval:
             <<: \*interval
             minutes: 10
     batchSize: 20
     scanInterval:
         <<: \*interval
         seconds: 2
 
 cacheCleaner:
     directories:
         - repo/raw
     scanInterval:
         <<: \*interval
         days: 1
     filesOlderThan:
         <<: \*interval
         days: 95
     directoriesEmptyForMoreThan:
         <<: \*interval
         days: 2

defaultInterval
---------------

The "defaultInterval" section is used as shorthand for the intervals used throughout the rest of the YAML configuration.

The existance of the "file_ingester" section tells the OODS to instantiate a file system scanning system, which looks into a directory for new files.  When the new files appear, they're moved to the configuration-specified location, and ingested into the specified butler.

file_ingester
-------------

The "file_ingester" section, has four subsections:  imageStagingDirectory, butlers, scanInterval, and batchSize.

The keyword "imageStagingDirectory" indicates where the camera deposits images.  The OODS scans this directory for new files.

The keyword "batchSize" is the maximum number of files to process at one time.

butlers
^^^^^^^

The "butlers" section allows multiple "butler"s, which is an artifact of when
we ran multiple butler instances with one OODS. In practice, we have a one-to-one OODS to butler correspondence, and eventually the "butlers" section will be removed.

Each "butler" section specifies: collections, instrument, stagingDirectory, repoDirectory, cleanCollections, and scanInterval

The "collections" section indicates which collections are used to instantiate the Butler object used by the OODS.

The "instrument" keyword indicates which camera's images will be written to this OODS.

The "staging" keyword indicates the directory where the OODS will set the files before working on them.  Files are moved from "imageStagingDirectory" to "stagingDirectory".

The "repoDirectory" is where the Butler repo exists.

The "cleanCollections" section specifies collections to clean up after a specified time.  The two subsections are "collection" and "filesOlderThan". This directs the OODS to remove data from the specified "collection" at a time interval specified by "filesOlderThan".

The "scanInterval" in this "butler" section specifies the frequency at which to process the "cleanCollections" directives.

scanInterval
^^^^^^^^^^^^

The "scanInterval" section specifies how often to scan for new files.  When the OODS transitions to "enabled" state, the directories are scanned and ingest occurs, if any data is found.  The OODS then waits for "scanInterval" before scanning again.  In the example, after the last ingest is completed, it waits 2 seconds before doing another directory scan.



cacheCleaner
------------

The "cacheCleaner" section is used to clean up old files, and empty directories. THis 1

This has four subsections: directories, scanInterval, filesOlderThan and directoriesEmptyForMoreThan.

The "directories" section specifies the location of the ingested Butler files to clean up.
By default this is "repo/raw" and is expected to be within the current directory where the OODS is invoked.

The "scanInterval" section specifies the frequency at which to scan the "directories" specified above.
In the example, it scans every 30 seconds.

The "filesOlderthan" section specifies how old files must be in order for them to be considered for removal.
This is checked against the last modification date of the file.
In this example, the file must be at least 30 days old to be considered for removal.

The "directoriesEmptyForMoreThan" section specifies how long directories must be empty for before they are to be considered for removal.
This is checked against the last modification date of the directory.
In this example, the directory must be at least  1 day old and empty to be considered for removal.


message_ingester
================

.. code:: yaml

 defaultInterval: &interval
     days: 0
     hours: 0
     minutes: 0
     seconds: 0
 
 message_ingester:
     kafka:
        brokers:
            - kafka:9092
        topics:
            - atoods
        group_id: ATOODS
        max_messages: 10
     butlers:
         - butler:
             repoDirectory : repo
             instrument: lsst.obs.lsst.Latiss
             collections:
                 - LATISS/raw/all
             cleanCollections:
                 - collection: LATISS/raw/all
                   filesOlderThan:
                       <<: *interval
                       days: 30
             scanInterval:
                 <<: *interval
                 minutes: 1
     scanInterval:
         <<: *interval
         seconds: 10

defaultInterval
---------------

The "defaultInterval" section is used as shorthand for the intervals used throughout the rest of the YAML configuration.

The existance of the "message_ingester" section tells the OODS to instantiate a message retrieval system, which subscribes to a Kafka message broker to receive messages about incoming data.  When the new files appear in an S3 store, the store issues a Kafka message indicating that the file has arrived.  That message used to ingest the specified data.

message_ingester
----------------

The "message_ingester" section, has three subsections:  kafka, butlers, and scanInterval.

kafka
^^^^^

The "kafka" subsection describes the location of kafka broker(s), kafka topics, kafka group id and the maximum number of messages to retrieve at a time.

butlers
^^^^^^^

The "butlers" subsection allows multiple "butler"s, which is an artifact of when
we ran multiple butler instances with one OODS. In practice, we have a one-to-one OODS to butler correspondence, and eventually the "butlers" section will be removed.

Each "butler" section specifies: repoDirectory, instrument, collections, cleanCollections, scanInterval

The "repoDirectory" is where the Butler repo exists.

The "instrument" keyword indicates which camera's images will be written to this OODS.

The "collections" section indicates which collections are used to instantiate the Butler object used by the OODS.

The "cleanCollections" section specifies collections to clean up after a specified time.  The two subsections are "collection" and "filesOlderThan". This directs the OODS to remove data from the specified "collection" at a time interval specified by "filesOlderThan".

The "scanInterval" in this "butler" section specifies the frequency at which to process the "cleanCollections" directives.

scanInterval
^^^^^^^^^^^^
The "scanInterval" section specifies how often wait for incoming messages.

