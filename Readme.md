# Kongsberg "KMALL" file reader

Modern Kongsberg bathymetric sonar systems which acquire data in "Seafloor Inforation System Version 5" produce data files in the ".kmall" data format. This module (class) and utility reads these data formats and provides several tools for reporting information about the data within the file.

Although low-level readers for many of the datagram types is in place, little other infrastucture exists. This reader remains a work in progress. 

    ./kmall.py -h
    usage: kmall.py [-h] [-f KMALL_FILENAME] [-d KMALL_DIRECTORY] [-V] [-v]
    A python script (and class) for parsing the Kongsberg KMALL data files.
    
    optional arguments:
    -h, --help  show this help message and exit
    -f KMALL_FILENAME The path and filename to parse.
    -d KMALL_DIRECTORY  A directory containing kmall data files to parse.
    -V  Perform series of checks to verify the kmall file.
    -v  Increasingly verbose output (e.g. -v -vv -vvv),for debugging use -vvv

 


> Written with [StackEdit](https://stackedit.io/).
