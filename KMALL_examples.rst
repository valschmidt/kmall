The KMALL Data Reader
=====================

This KMALL data reader is incomplete, but a good start. The following
will guide one through what’s currently possible. At the end is a
section on commandline capability of ``kmall.py`` - be sure to check
that out. Finally there’s also a list of obvious things that are yet to
be completed or might be in place, but warrant rethinking.

The kmall Python Module
-----------------------

.. code:: ipython3

    import kmall

Each kmall object is associated with a datafile. So when creating an
object, pass the filename to be associated with it.

.. code:: ipython3

    K = kmall.kmall('data/0007_20190513_154724_ASVBEN.kmall')

The file can be indexed with the following. This happens automatically
when reading the file for other purposes, but sometimes you want the
index itself, so you can call it directly as shown here. The index
itself is a pandas DataFrame, and we can look at first several rows of
the index to get an idea of what’s inside.

.. code:: ipython3

    K.index_file()
    print(K.Index.iloc[0:20,:])


.. parsed-literal::

                  ByteOffset  MessageSize MessageType
    Time                                             
    1.557762e+09           0         1054     b'#IIP'
    1.557762e+09        1054         1332     b'#IOP'
    1.557762e+09        2386          292     b'#SVP'
    1.557762e+09        2678           68     b'#SVT'
    1.557762e+09        2746           68     b'#SVT'
    1.557762e+09        2814           68     b'#SVT'
    1.557762e+09        2882           68     b'#SVT'
    1.557762e+09        2950           68     b'#SVT'
    1.557762e+09        3018           68     b'#SVT'
    1.557762e+09        3086           68     b'#SVT'
    1.557762e+09        3154           68     b'#SVT'
    1.557762e+09        3222           68     b'#SVT'
    1.557762e+09        3290        13368     b'#SKM'
    1.557762e+09       16658           68     b'#SVT'
    1.557762e+09       16726           68     b'#SVT'
    1.557762e+09       16794           68     b'#SVT'
    1.557762e+09       16862          156     b'#SPO'
    1.557762e+09       17018        69036     b'#MRZ'
    1.557762e+09       86054        68996     b'#MRZ'
    1.557762e+09      155050        68938     b'#MRZ'


It is often useful to have a summary of the types of packets in a file.
This can be done with the ``report_packet_types()`` method.

.. code:: ipython3

    K.report_packet_types()


.. parsed-literal::

                 Count     Size:  Min Size  Max Size
    MessageType                                     
    b'#CPO'         87     13572       156       156
    b'#IIP'          1      1054      1054      1054
    b'#IOP'          1      1332      1332      1332
    b'#MRZ'        472  32556126     68688     69120
    b'#SCL'         87      6612        76        76
    b'#SKM'         86   1159680     13368     13632
    b'#SPO'         87     13572       156       156
    b'#SVP'          1       292       292       292
    b'#SVT'       2181    148308        68        68


When Kongsberg system installation suffer from networking problems (or
hardware malfunctions of the CPU), they sometimes loose data packets.
While there is indication in SIS 4/5 if incoming data to the PPU
(navigation, attitude, velocity) has gaps, there is often not any
indication that the sonar records coming out of the PPU have gaps. If
the extent of the problem is great enough, there will be a “Failure to
report depths.” error, but if not, the system will silently log data
with the occasional missing record.

In an effort to try to detect this in logged data files a routine has
been written, ``check_ping_count()``, which compares the ping indices
(these values increase with each ping cycle, but repeat for each swath
within a ping cycle), indices of expected receive fans and tne number of
received *MRZ* records (there should be 1 per receive fan), and reports
anything missing.

In the future, the check will also audit the navigation and attitude
data (which are stored in the same record when supplied by a POS/MV
Group record via Ethernet) and report gaps in the record when they
exist. This is done currently in if the module is run as a script on a
file (more about that later), but not yet implemented as a class method.

Here’s an example, where the results are both printed out by default and
returned as a tuple for internal use later.

.. code:: ipython3

    result = K.check_ping_count()


.. parsed-literal::

                                       File  NpingsTotal  Pings Missed  MissingMRZRecords
     data/0007_20190513_154724_ASVBEN.kmall          238             2                  0


In this example, there should be 238 pings based on the difference in
first and last ping indices, but two pings were missed in the middle.
However all the MRZ records associated with each existing ping record
were found.

In the future there will be utilty functions to make this process
easier, for now one must extract desired data manually. Not all records
can be read yet, but reading of complete MRZ records is supported. First
lets filter the index for MRZ records:

.. code:: ipython3

    iMRZ = K.Index["MessageType"] == "b'#MRZ'"
    MRZIndex = K.Index[iMRZ]
    MRZIndex.head()




.. raw:: html

    <div>
    <style scoped>
        .dataframe tbody tr th:only-of-type {
            vertical-align: middle;
        }
    
        .dataframe tbody tr th {
            vertical-align: top;
        }
    
        .dataframe thead th {
            text-align: right;
        }
    </style>
    <table border="1" class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>ByteOffset</th>
          <th>MessageSize</th>
          <th>MessageType</th>
        </tr>
        <tr>
          <th>Time</th>
          <th></th>
          <th></th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>1.557762e+09</th>
          <td>17018</td>
          <td>69036</td>
          <td>b'#MRZ'</td>
        </tr>
        <tr>
          <th>1.557762e+09</th>
          <td>86054</td>
          <td>68996</td>
          <td>b'#MRZ'</td>
        </tr>
        <tr>
          <th>1.557762e+09</th>
          <td>155050</td>
          <td>68938</td>
          <td>b'#MRZ'</td>
        </tr>
        <tr>
          <th>1.557762e+09</th>
          <td>223988</td>
          <td>69006</td>
          <td>b'#MRZ'</td>
        </tr>
        <tr>
          <th>1.557762e+09</th>
          <td>294110</td>
          <td>69044</td>
          <td>b'#MRZ'</td>
        </tr>
      </tbody>
    </table>
    </div>



Now we can open the file, seek to the first record location and read the
record.

.. code:: ipython3

    K.OpenFiletoRead()
    K.FID.seek(MRZIndex["ByteOffset"].iloc[0],0)
    dg = K.read_EMdgmMRZ()
    print("MRZ Records:  " + ",".join( dg.keys()))
    print("Soundings Record Fields: " + ",\n\t".join(dg["soundings"].keys()))


.. parsed-literal::

    MRZ Records:  header,Mpart,Mbody,pinginfo,txSectorinfo,rxinfo,extraDetClassInfo,soundings,Slsample_desidB
    Soundings Record Fields: soundingIndex,
    	txSectorNumb,
    	detectionType,
    	detectionMethod,
    	rejectionInfo1,
    	rejectionInfo2,
    	postProcessingInfo,
    	detectionClass,
    	detectionConfidenceLevel,
    	padding,
    	rangeFactor,
    	qualityFactor,
    	detectionUncertaintyVer_m,
    	detectionUncertaintyHor_m,
    	detectionWindowLength_m,
    	echo_Length_sec,
    	WCBeamNumb,
    	WCrange_samples,
    	WCNomBeamAngleAcross_deg,
    	meanAbsCoeff_dbPerkm,
    	reflectivity1_dB,
    	reflectivity2_dB,
    	receiverSensitivityApplied_dB,
    	sourceLevelApplied_dB,
    	BScalibration_dB,
    	TVG_dB,
    	beamAngleReRx_deg,
    	beamAngleCorrection_deg,
    	twoWayTravelTime_sec,
    	twoWayTravelTimeCorrection_sec,
    	deltaLatitude_deg,
    	deltaLongitude_deg,
    	z_reRefPoint_m,
    	y_reRefPoint_m,
    	x_reRefPoint_m,
    	beamIncAngleAdj_deg,
    	realTimeCleanInfo,
    	SlstartRange_samples,
    	SlcenterSample,
    	SlnumSamples


There is also a debugging method ``print_datagram()`` for printing the
fields of a record. It is very verbose, but can be helpful to dump
everything to sort out a problem. Here’s an example on the MRZ header,
which is not so large.

.. code:: ipython3

    K.print_datagram(dg["header"])


.. parsed-literal::

    
    
    numBytesDgm:			69036
    dgmType:			b'#MRZ'
    dgmVersion:			0
    systemID:			40
    echoSounderID:			2040
    dgtime:			1557762443.1261249
    dgdatetime:			2019-05-13 15:47:23.126125


The kmall.py Commandline Utility
--------------------------------

In addition to being able to parse kmall data files, kmall.py has a lot
of functionality build right in when called on the command line. Here
are some examples: (Note that access to the bash shell from this python
notebook requires pre-pending each line with ``!``. This should be
omitted when calling directly from the command line.)

First we can see what is possible by asking for help.

.. code:: ipython3

    !./kmall.py -h


.. parsed-literal::

    usage: kmall.py [-h] [-f KMALL_FILENAME] [-d KMALL_DIRECTORY] [-V] [-z]
                    [-l COMPRESSIONLEVEL] [-Z] [-v]
    
    A python script (and class)for parsing Kongsberg KMALL data files.
    
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


File Verification
~~~~~~~~~~~~~~~~~

Suppose I want to verify that no ping records are missing from a data
file and there are no gaps in the navigation. I can check it with the
following:

.. code:: ipython3

    !./kmall.py -f data/0007_20190513_154724_ASVBEN.kmall -V


.. parsed-literal::

    
    Processing: data/0007_20190513_154724_ASVBEN.kmall
                 Count     Size:  Min Size  Max Size
    MessageType                                     
    b'#CPO'         87     13572       156       156
    b'#IIP'          1      1054      1054      1054
    b'#IOP'          1      1332      1332      1332
    b'#MRZ'        472  32556126     68688     69120
    b'#SCL'         87      6612        76        76
    b'#SKM'         86   1159680     13368     13632
    b'#SPO'         87     13572       156       156
    b'#SVP'          1       292       292       292
    b'#SVT'       2181    148308        68        68
                                       File  NpingsTotal  Pings Missed  MissingMRZRecords
     data/0007_20190513_154724_ASVBEN.kmall          238             2                  0
    Packet statistics:
                                         File  Npings  NpingsMissing  NMissingMRZ  NavMinTimeGap  NavMaxTimeGap  NavMeanTimeGap  NavMeanFreq  NavNGaps>1s
    0  data/0007_20190513_154724_ASVBEN.kmall     238              2            0            0.0       0.010001        0.009997   100.034501            0


Above the number of packets of each type are reported, along with how
many bytes that packet type takes up in the file. It is sometimes useful
to see the minimum and maximum size for a given packet type when
troubleshooting, so these are reported too.

Next the file is checked for missing pings records and this is assessed
from the ping counter index. But a single ping can consist for multiple
“MRZ” records. Two are reported for each swath in dual-swath mode, and
the file format is agile such that is is possible to report them for
individual transmit sectors. Every MRZ record reports an index
indicating which “receive fan” this data holds, and the total number of
receive fans (e.g. MRZ records) to expect. These numbers are used to
look for missing MRZ records and these are also reported.

Finally, the attitude data is extracted from the file (this may or may
not include position information, for example, when the system logs
Group 102/103 messages from a POS/MV over Ethernet), and the difference
in successive time-stamps is calculated. Statistis of these differences
is reported.

Compression
~~~~~~~~~~~

**This is an exerimental feature.**

Another useful tool in the ``kmall.py`` utilty belt is file compression.
The kmall data format is rather inefficiently encoded and a few routines
exist to reorganize and compress the data. The goal of these routines is
to provide a significantly smaller file for more efficient transmission
over a telemetry link.

| To accomplish this, new datagram format types are defined. Currently
  two methods are used, and the resulting datagrams have 3-letter
  identifiers “#CZ0” and “#CZ1”. These are non-standard, unapproved by
  Kongsberg, and an application not capabile of ignoring datagrams it
  doesn’t understand will likely crash when trying to read them. Thus it
  is recommended that these formats be used in a temporary way for file
  transport, then decompressed and the compressed versions deleted to
  ensure compressed version are never accidentally archived.
| THESE ROUTINES ARE LOSSY, meaning that a decompressed file is not
  identical to the original. However, the portions of the file not
  retained largely result from converting floating point values into
  integers and an effort has been made to do so in a way that will not
  loose data of any significance. Reasonable people can disagree about
  this (Do we need position to mm’s or beam reflectivity to 0.000001?),
  and there may be errors (or bugs) in the methods resulting from
  testing only on shallow water systems. Thus the exerimental nature.

Compression levels 0 and 1 are defined (hence CZ0 and CZ1 above). Level
0 reorganizes the sounding and imagery data, re-encodes it and
compresses it before writing it to disk. Level 1 does the same but omits
the imagery data altogether, because sometimes getting a start on the
bathy processing is enough. Obviously Level 1 is not really compression
and is very lossy.

Note: There is more work to be done here and an additional file size
reduction can be had by running a standard compression tool on the
resulting file.

Here’s how it works:

.. code:: ipython3

    # Standard bzip2 compression on a test file...
    !ls -lh compressiondata/0007_20190513_154724_ASVBEN.kmall
    !cp compressiondata/0007_20190513_154724_ASVBEN.kmall compressiondata/0007_20190513_154724_ASVBEN.kmall.test
    !bzip2 -f compressiondata/0007_20190513_154724_ASVBEN.kmall.test
    !ls -lh compressiondata/0007_20190513_154724_ASVBEN.kmall.test.bz2
    
    # kmall compresssion on the same file. 
    !./kmall.py -f compressiondata/0007_20190513_154724_ASVBEN.kmall -z -l0
    !ls -lh compressiondata/0007_20190513_154724_ASVBEN.kmall.0z
    !./kmall.py -f compressiondata/0007_20190513_154724_ASVBEN.kmall -z -l1
    !ls -lh compressiondata/0007_20190513_154724_ASVBEN.kmall.1z
    
    # Now bzip2 that.
    !bzip2 compressiondata/0007_20190513_154724_ASVBEN.kmall.0z
    !bzip2 compressiondata/0007_20190513_154724_ASVBEN.kmall.1z
    !ls -lh compressiondata/0007_20190513_154724_ASVBEN.kmall.0z.bz2
    !ls -lh compressiondata/0007_20190513_154724_ASVBEN.kmall.1z.bz2
    
    # Now decompress those files to see the difference in file size.
    # Note that kmall.py is careful not to clobber the original file.
    !bunzip2 compressiondata/0007_20190513_154724_ASVBEN.kmall.0z.bz2
    !bunzip2 compressiondata/0007_20190513_154724_ASVBEN.kmall.1z.bz2
    !./kmall.py -f compressiondata/0007_20190513_154724_ASVBEN.kmall.0z -Z
    !ls -lh compressiondata/0007_20190513_154724_ASVBEN_01.kmall
    
    
    !./kmall.py -f compressiondata/0007_20190513_154724_ASVBEN.kmall.1z -Z
    !ls -lh compressiondata/0007_20190513_154724_ASVBEN_02.kmall
    



.. parsed-literal::

    -rwxr-xr-x  1 vschmidt  1129769604    32M Mar 17 09:36 [31mcompressiondata/0007_20190513_154724_ASVBEN.kmall[m[m
    -rwxr-xr-x  1 vschmidt  1129769604    20M Mar 17 17:24 [31mcompressiondata/0007_20190513_154724_ASVBEN.kmall.test.bz2[m[m
    
    Processing: compressiondata/0007_20190513_154724_ASVBEN.kmall
    Compressing soundings and imagery.
    -rw-r--r--  1 vschmidt  1129769604    14M Mar 17 17:24 compressiondata/0007_20190513_154724_ASVBEN.kmall.0z
    
    Processing: compressiondata/0007_20190513_154724_ASVBEN.kmall
    Compressing soundings, omitting imagery.
    -rw-r--r--  1 vschmidt  1129769604   7.6M Mar 17 17:24 compressiondata/0007_20190513_154724_ASVBEN.kmall.1z
    -rw-r--r--  1 vschmidt  1129769604    13M Mar 17 17:24 compressiondata/0007_20190513_154724_ASVBEN.kmall.0z.bz2
    -rw-r--r--  1 vschmidt  1129769604   7.0M Mar 17 17:24 compressiondata/0007_20190513_154724_ASVBEN.kmall.1z.bz2
    
    Processing: compressiondata/0007_20190513_154724_ASVBEN.kmall.0z
    Decompressing soundings and imagery.(Level: 0)
    -rw-r--r--  1 vschmidt  1129769604    32M Mar 17 17:25 compressiondata/0007_20190513_154724_ASVBEN_01.kmall
    
    Processing: compressiondata/0007_20190513_154724_ASVBEN.kmall.1z
    Decompessing soundings, imagery was omitted in this format. (Level: 1)
    -rw-r--r--  1 vschmidt  1129769604    23M Mar 17 17:25 compressiondata/0007_20190513_154724_ASVBEN_02.kmall


In the example we start with a 32 MB file. Native bzip2 compression
alone produces a 20 MB file.

``kmall.py`` compression at Level 0 produces a 14 MB file, and bzip2
compression of that gives a 13 MB file.

``kmall.py`` compression at Level 1 (omitting imagery) produces a 7.6 MB
file, and bzip2 compression of that gives a 7.0 MB file.

On this file, the Level 0 method reduces the file size to about 40% of
the original, and the Level 1 method reduces it to about 20% of the
orginal.

What’s next:
------------

Here’s a list of improvements that need to be made:

1.  The installation parameters datagram can be read, but the text
    string cannot yet be parsed.
2.  The runtime parameters datagram can be read, but the text string
    cannot yet be parsed.
3.  The file Index is indexed by time in Unix format. These could/should
    be converted to human readable times.
4.  In file index messag type is not a simple “MRZ” but rather the text
    “b’#MRZ’”. This could be simplified.
5.  There is not yet a read_next_datagram() method, which can be useful
    to walk through a file. (although the index helps)
6.  There is not yet a utilty function that can extract all the sounding
    data in x,y,z re vessel and x,y,z in geographic coordinates and
    meters for a) the ping and b) all pings between two indices and c)
    the whole file.
7.  The packets related to BIS error reports, reply, and short reply
    cannot yet be read / interpreted.
8.  The water column datagram, #MWC, cannot yet be read. (DONE)
9.  A “compression” method could drop the high rate navigation
    datagrams, (assuming there is no need for it)
10. Lots of improvements in efficiency.



