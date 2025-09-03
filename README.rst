#########
ctrl_oods
#########

``ctrl_oods`` is an `LDF Prompt Enclave Software` package.

.. Add a brief (few sentence) description of what this package provides.

The Observatory Operations Data Service watches for files in one or more directories, and then ingests them into an LSST Butler repository.   
Files are expired from the repository at specified intervals.

Example YAML file for file ingest
---------------------------------

.. code-block:: yaml

    default_interval: &interval
        days: 0
        hours: 0
        minutes: 0
        seconds: 0
    file_ingester:
        image_staging_directory: /tmp/image_staging
        bad_file_directory: /tmp/bad
        staging_directory: /tmp/staging
        batch_size: 20
        new_file_scan_interval:
            <<: *interval
            seconds: 1
        butler:
            instrument: lsst.obs.lsst.LsstComCam
            repo_directory : /tmp/repo
            collections:
                - LSSTCam/raw/all
            collection_cleaner:
                collections_to_clean:
                    - collection: LSSTCam/raw/all
                      files_older_than:
                          <<: *interval
                          seconds: 10
                cleaning_interval:
                    <<: *interval
                    seconds: 15
        cache_cleaner:
            clear_empty_directories_and_old_files:
                - /tmp/repo/raw
            files_older_than:
                <<: *interval
                days: 30 
            directories_empty_for_more_than:
                <<: *interval
                days: 2
            cleaning_interval:
                <<: *interval
                seconds: 30

The ``default_interval`` section is used for the initial values of `intervals` used in the rest of the YAML configuration.

The ``file_ingester`` section has several elements and sections.

``image_staging_directory`` is the directory location where the camera moves files after it writes them.  The camera writes a file to a private directory, and then moves that file to this location.

``bad_file_directory`` is where the OODS will put files that have failed to be ingested. Check the log output of the OODS for an explanation.

``staging_directory`` is the location of where the OODS will act on files.  Files from move from ``image_staging_directory`` to ``staging_directory``

The ``new_file_scan_interval`` section is the interval at which the file system is scanned.  In this example, scans pause for one section, before a new file system scan starts.

The ``butler`` section describes:
    * ``instrument`` - the camera type
    * ``repo_directory`` - the butler repository location
    * ``collections`` - the butler collection used to initialize the OODS butler
    * ``collection_cleaner`` section describes, collections and how long files will remain in the Butler before being removed, and the interval at which files are cleaned

The ``cache_cleaner`` section describes:
    * ``clear_empty_directories_and_old_files`` - a list of directories to scan for empty directories, or old files
    * ``cleaning_interval`` - how often to clean
    * ``files_older_than`` - how old the files have to be before they'll be removed
    * ``directories_empty_for_more_than`` - how long directories have to be empty before they are removed



Sections are:

The ``collections_to_clean`` section describes lists of collections, and cleaning intervals:
    * ``collection`` - the collection to clean
    * ``files_older_than`` - the interval at which files will be removed

In this case, files in collection ``LSSTCam/raw/all`` will be removed after 10 seconds

The ``cleaning_interval`` is the interval at which cleaning takes place.  In this case, a cleaning check takes places every 15 seconds.
