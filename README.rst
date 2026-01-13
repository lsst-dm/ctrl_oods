#########
ctrl_oods
#########

``ctrl_oods`` is an `LDF Prompt Enclave Software` package.

.. Add a brief (few sentence) description of what this package provides.

The Observatory Operations Data Service watches for files in one or more directories, and then ingests them into an LSST Butler repository.   
Files are expired from the repository at specified intervals.

Setting logging level
---------------------
Set the environment variable CTRL_OODS_DEBUG_LEVEL to control the logging level.

.. code-block:: shell

    export CTRL_OODS_DEBUG_LEVEL=DEBUG

sets it to ``logging.DEBUG``

The default is ``logging.INFO``

Guiders
-------
Guider files must be ingested after a WF or regular CCD has been ingested, or the guider ingest will fail. If the guider ingest fails,
that guider file is added to an ingestion candidate list, with an expiration time.  When new 
WFs or regular CCDs arrive for the OODS to be ingested, another attempt to ingest the guider file will happen.  If after the guider
ingest attempt fails, the expiration time has past, the guider file will be deemed uningestible, and removed from the ingestion candidates list.
The expiration time ``guider_max_age_seconds`` should be set to a time by which a WF or regular CCD message should have arrived to the OODS.  Since
ingests should happen quickly, setting that expiration time to 30 or 60 seconds is reasonable, and well beyond when a WF or CCDS for that image
should have arrived.

Collection Cleaner
------------------
The collection cleaner section of the butler block specifies the Butler datasets to expire.  This is done for each Butler collection specified.
Files that are older than a specified time period will be removed from the butler.  The default is to remove all dataset types (or if you prefer to
specify all dataset types, add ``"*`` as the only list entry for ``dataset_types``.  If ``dataset_type``are specified, only those dataset types
will be removed.  Datasets in tagged collections can be marked as exempt from being removed. By specifying ``exclude_tagged: true``, any datasets
that would have been deleted will NOT be deleted.  If ``exclude_tagged`` is set to ``false``, the datasets will be removed.  Multiple collections
can be grouped if you'd like to delete the same dataset_types and if ``exclude_tagged`` for each is the same.  Otherwise, you can list each
collection individually.

The ``cleaning__interval`` specifies out often collections are scanned for Butler datasets to expire.

Note that if collection cleaner is not present, the OODS will not perform any cleaning.   This is useful if there are multiple OODSes working with the same repo.



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
            guider_max_age_seconds: 30
            instrument: lsst.obs.lsst.LsstComCam
            repo_directory : /tmp/repo
            collections:
                - LSSTCam/raw/all
            collection_cleaner:
                collections_to_clean:
                    - collections: 
                          - LSSTCam/raw/all
                      files_older_than:
                          <<: *interval
                          seconds: 10
                      dataset_types:
                          - "*"
                      exclude_tagged: true
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

The ``butler`` section describes
    * ``guider_max_age_seconds`` - the number of seconds guiders that haven't been successfully ingested will attempt to be ingested before giving up
    * ``instrument`` - the camera type
    * ``repo_directory`` - the butler repository location
    * ``collections`` - the butler collection used to initialize the OODS butler
    * ``collection_cleaner`` section describes, collections and how long files will remain in the Butler before being removed, and the interval at which files are cleaned

The ``collection_cleaner`` section describes
    * ``collections_to_clean`` - a list of collections from which entries will be removed
    * ``cleaning_interval`` - the cycle at which all collections will be evaluated

The ``collections_to_clean`` section describes
    * ``collections`` - a list of collections
    * ``files_older_than`` - a time interval which, when receached, files are canidates for removal
    * ``dataset_types`` - the dataset types to clean.  These are the only types to be removed.  ``*`` means all dataset types
    * ``exclude_tagged`` - indicates whether entries that are part of a tagged collection will be removed

In this case, files in collection ``LSSTCam/raw/all`` will be removed after 10 seconds
The ``cleaning_interval`` is the interval at which cleaning takes place.  In this case, a cleaning check takes places every 15 seconds.

The ``cache_cleaner`` section describes
    * ``clear_empty_directories_and_old_files`` - a list of directories to scan for empty directories, or old files
    * ``cleaning_interval`` - how often to clean
    * ``files_older_than`` - how old the files have to be before they'll be removed
    * ``directories_empty_for_more_than`` - how long directories have to be empty before they are removed


Example YAML file for message ingest
------------------------------------

.. code-block:: yaml

    default_interval: &interval
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
            max_wait_time: 1.0
        butler:
            guider_max_age_seconds: 30
            instrument: lsst.obs.lsst.LsstCam
            repo_directory : /tmp/repo
            s3profile: testprofile
            collections:
                - LSSTCam/raw/all
            collection_cleaner:
                collections_to_clean:
                    - collections: 
                          - LSSTCam/raw/all
                      files_older_than:
                          <<: *interval
                          seconds: 10
                      dataset_types:
                          - "*"
                      exclude_tagged: true
                    - collections: 
                          - LSSTCam/raw/guider
                      files_older_than:
                          <<: *interval
                          seconds: 10
                      dataset_types:
                          - "*"
                      exclude_tagged: true
                cleaning_interval:
                    <<: *interval
                    seconds: 10

The ``default_interval`` section is used for the initial values of `intervals` used in the rest of the YAML configuration.

The ``message_ingester`` section has two sections: ``kafka`` and ``butler``

The `kafka` section describes
    * ``brokers`` - a list of Kafka brokers the OODS will connect  to for messages
    * ``topics`` - a list of Kafka topics the OODS will listen on
    * ``group_id`` - the group id of this client
    * ``max_messages`` - the maximum number of messages to wait for before returning.  Note that the OODS may read less messages if it times out before ``max_wait_time``.
    * ``max_wait_time`` - the maximum about of time to wait for before returning, regardless of the number of messages retrieved.

The ``butler`` section describes
    * ``guider_max_age_seconds`` - the number of seconds guiders that haven't been successfully ingested will attempt to be ingested before giving up
    * ``instrument`` - the camera type
    * ``repo_directory`` - the butler repository location
    * ``s3profile`` - the S3 profile used to connect to the message store
    * ``collections`` - the butler collection used to initialize the OODS butler
    * ``collection_cleaner`` section describes, collections and how long files will remain in the Butler before being removed, and the interval at which files are cleaned

The ``collection_cleaner`` section describes
    * ``collections_to_clean`` - a list of collections from which entries will be removed
    * ``cleaning_interval`` - the cycle at which all collections will be evaluated

The ``collections_to_clean`` section describes
    * ``collections`` - a list of collections
    * ``files_older_than`` - a time interval which, when receached, files are canidates for removal
    * ``dataset_types`` - the dataset types to clean.  These are the only types to be removed.  ``*`` means all dataset types
    * ``exclude_tagged`` - indicates whether entries that are part of a tagged collection will be removed
