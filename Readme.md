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

## Coordinate systems

### Vessel Coordinate System (VCS)

Origo of the VCS is the vessel reference point. The VCS is defined according to the right hand rule.

    x-axis pointing forward parallel to the vessel main axis.
    y-axis pointing starboard parallel to the deck plane.
    z-axis pointing down parallel to the mast.

Rotation of the vessel coordinate system around an axis is defined as positive in the clockwise direction, also according to the right hand rule.

    Roll - rotation around the x-axis. (positive when starboard is down)
    Pitch - rotation around the y-axis. (positive when bow is up)
    Heading - rotation around the z-axis. Heading as input in depth calculations is sensor data referred to true north. (positive is clockwise when looking down, like traditional heading)

### Array Coordinate System (ACS)

Origo of the ACS is at the centre of the array face. The ACS is defined according to the right hand rule.

    x-axis pointing forward along the array (parallel to the vessel main axis).
    y-axis pointing starboard along the array plane.
    z-axis pointing down orthogonal to the array plane.

### Surface Coordinate System (SCS)

Origo of the SCS is the vessel reference point at the time of transmission. The SCS is defined according to the right hand rule.

    x-axis pointing forward along the horizontal projection of the vessel main axis.
    y-axis pointing horizontally to starboard, orthogonal to the horizontal projection of the vessel main axis.
    z-axis pointing down along the g-vector.

To move SCS into the waterline, use reference point height corrected for roll and pitch at the time of transmission.

### Fixed Coordinate System (FCS)

Origo of the FCS is fixed somewhere in the nominal sea surface. The FCS is defined according to the right hand rule.

    x-axis pointing north.
    y-axis pointing east.
    z-axis pointing down along the g-vector.

> Written with [StackEdit](https://stackedit.io/).
