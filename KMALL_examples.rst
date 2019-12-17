The KMALL Data Reader
=====================

This KMALL data reader is incomplete, but a good start. The following
will guide one through what’s currently possible. At the end is a list
of obvious things that are yet to be completed or might be in place, but
warrant rethinking.

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


What’s next:
------------

Here’s a list of obvious additions and improvements to the reader:

1. The installation parameters datagram can be read, but the text string
   cannot yet be parsed.
2. The runtime parameters datagram can be read, but the text string
   cannot yet be parsed.
3. The file Index is indexed by time in Unix format. These could/should
   be converted to human readable times.
4. In file index messag type is not a simple “MRZ” but rather the text
   “b’#MRZ’”. This could be simplified.
5. There is not yet a read_next_datagram() method, which can be useful
   to walk through a file. (although the index helps)
6. There is not yet a utilty function that can extract all the sounding
   data in x,y,z re vessel and x,y,z in geographic coordinates and
   meters for a) the ping and b) all pings between two indices and c)
   the whole file.
7. The packets related to BIS error reports, reply, and short reply
   cannot yet be read / interpreted.
8. The water column datagram, #MWC, cannot yet be read.

