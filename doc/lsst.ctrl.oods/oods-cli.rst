#########
ctrl_oods
#########
usage: oods.py [-y] [-l {DEBUG,INFO,WARN,ERROR,FATAL}] [config]

Options:

.. code:: bash

\-y
    config validate YAML configuration file
\-h
    show this help message and exit
\-l [{DEBUG,INFO,WARN,ERROR,FATAL}]
    print logging statements


Set up and usage
================

1) Create the Gen2 Butler repository:

.. code:: bash

 mkdir repo
 echo "lsst.obs.lsst.auxTel.AuxTelMapper" > repo/_mapper

2) Edit the YAML configuration file.  The default example is located in:

.. code:: bash

    $CTRL_OODS_DIR/etc/oods.yaml

3) Run the OODS:

.. code:: bash

    nohup $CTRL_OODS_DIR/bin/oods.py $CTRL_OODS_DIR/etc/oods.yaml 2>&1 >oods.log

NOTE:  if you run the OODS without modifying the directory paths, it expects to scan for files in the directory in which the OODS has been invoked.
It will scan the directory "data" and use the Gen2 Butler repository "repo" by default.
