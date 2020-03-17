# Kongsberg "KMALL" file reader

Modern Kongsberg bathymetric sonar systems, which acquire data using "Seafloor Inforation System - Version 5" produce data files in the ".kmall" data format. This module (class) and utility reads these data formats and provides several tools for reporting information about the data within the file.

Although low-level readers for many of the datagram types is in place, little other infrastucture exists. This reader remains a work in progress. 

    ./kmall.py -h
    usage: kmall.py [-h] [-f KMALL_FILENAME] [-d KMALL_DIRECTORY] [-V] [-z]
                [-l COMPRESSIONLEVEL] [-Z] [-v]

    A python script (and class) for parsing Kongsberg KMALL data files.

    optional arguments:
      -h, --help           show this help message and exit
      -f KMALL_FILENAME    The path and filename to parse.
      -d KMALL_DIRECTORY   A directory containing kmall data files to parse.
      -V                   Perform series of checks to verify the kmall file.
      -z                   Create a compressed (somewhat lossy) version of the
                           file. See -l
      -l COMPRESSIONLEVEL  Set the compression level (Default: 0). 0: Somewhat
                           lossy compression of soundings and imagery
                           data.(Default) 1: Somewhat lossy compression of
                           soundings with imagery omitted.
      -Z                   Decompress a file compressed with this library. Files
                           must end in .Lz, where L is an integer indicating the
                           compression level (set by -l when compresssing)
      -v                   Increasingly verbose output (e.g. -v -vv -vvv),for
                           debugging use -vvv    

See [the examples](KMALL_examples.rst) for details about using the module.


> Written with [StackEdit](https://stackedit.io/).
