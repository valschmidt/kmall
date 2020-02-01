# !/usr/bin/env python
# -*- coding: utf-8 -*-

# NOTE: From Val's GitHub repository: kmall/kmall.py

"""
A python class to read Kongsberg KMALL data format for swath mapping
bathymetric echosounders.
"""

import pandas as pd
import sys
import numpy as np
import struct
import datetime
import argparse
import os
from pyproj import Proj


class kmall():
    """ A class for reading a Kongsberg KMALL data file. """

    def __init__(self, filename=None):
        self.verbose = 0
        self.filename = filename
        self.FID = None
        self.file_size = None
        self.Index = pd.DataFrame({'Time': [np.nan],
                                   'ByteOffset': [np.nan],
                                   'MessageSize': [np.nan],
                                   'MessageType': ['test']})

        self.pingDataCheck = None
        self.navDataCheck = None

    def read_datagram():
        '''
        /*********************************************
        274             Datagram names
        275  *********************************************/
        276
        277 /* I - datagrams */
        278 #define EM_DGM_I_INSTALLATION_PARAM    "#IIP"
        279 #define EM_DGM_I_OP_RUNTIME            "#IOP"
        280
        281 /* S-datagrams */
        282 #define EM_DGM_S_POSITION               "#SPO"
        283 #define EM_DGM_S_KM_BINARY              "#SKM"
        284 #define EM_DGM_S_SOUND_VELOCITY_PROFILE "#SVP"
        285 #define EM_DGM_S_CLOCK                  "#SCL"
        286 #define EM_DGM_S_DEPTH                  "#SDE"
        287 #define EM_DGM_S_HEIGHT                 "#SHI"
        288 #define EM_DGM_S_HEADING                "#SHA"
        289
        290 /* M-datagrams */
        291 #define EM_DGM_M_RANGE_AND_DEPTH        "#MRZ"
        292 #define EM_DGM_M_WATER_COLUMN           "#MWC"

        Possible strategy for reading from a stream:
            - Read data into buffer
            - Search for all of these datagram types.
            - seek to first one minus 4 bytes for packet size.
            - Read packet size.
            - Check to see if packet is wholly contained within buffer.
            - IF not, increase size of buffer.
            - Read header
            - Read rest of packet.
        '''
        pass

    def read_EMdgmHeader(self):
        """
        Read general datagram header.
        :return: A dictionary containing EMdgmHeader ('header').
        """
        # LMD tested.

        dg = {}
        format_to_unpack = "1I4s2B1H2I"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Datagram length in bytes. The length field at the start (4 bytes) and end
        # of the datagram (4 bytes) are included in the length count.
        dg['numBytesDgm'] = fields[0]
        # Array of length 4. Multibeam datagram type definition, e.g. #AAA
        dg['dgmType'] = fields[1]
        # Datagram version.
        dg['dgmVersion'] = fields[2]
        # System ID. Parameter used for separating datagrams from different echosounders
        # if more than one system is connected to SIS/K-Controller.
        dg['systemID'] = fields[3]
        # Echo sounder identity, e.g. 122, 302, 710, 712, 2040, 2045, 850.
        dg['echoSounderID'] = fields[4]
        # UTC time in seconds + Nano seconds remainder. Epoch 1970-01-01.
        dg['dgtime'] = fields[5] + fields[6] / 1.0E9
        dg['dgdatetime'] = datetime.datetime.utcfromtimestamp(dg['dgtime'])

        if self.verbose > 2:
            self.print_datagram(dg)

        return dg

    def read_EMdgmIIP(self):
        """
        Read #IIP - installation parameters and sensor format settings.
        :return: A dictionary containging EMdgmIIP.
        """
        # LMD tested.

        dg = {}
        dg['header'] = self.read_EMdgmHeader()

        format_to_unpack = "3H1B"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Size in bytes of body part struct. Used for denoting size of rest of the datagram.
        dg['numBytesCmnPart'] = fields[0]
        # Information. For future use.
        dg['info'] = fields[1]
        # Status. For future use.
        dg['status'] = fields[2]

        # Installation settings as text format. Parameters separated by ; and lines separated by , delimiter.
        tmp = self.FID.read(dg['numBytesCmnPart'] - struct.Struct(format_to_unpack).size)
        i_text = tmp.decode('UTF-8')
        dg['install_txt'] = i_text

        # Skip unknown fields.
        self.FID.seek(dg['numBytesCmnPart'] - struct.Struct(format_to_unpack).size, 1)

        return dg

    def read_EMdgmIOP(self):
        """
        Read #IOP - runtime parameters, exactly as chosen by operator in K-Controller/SIS menus.
        :return: A dictionary containing EMdgmIOP.
        """
        # LMD tested.

        dg = {}
        dg['header'] = self.read_EMdgmHeader()

        format_to_unpack = "3H"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Size in bytes of body part struct. Used for denoting size of rest of the datagram.
        dg['numBytesCmnPart'] = fields[0]
        # Information. For future use.
        dg['info'] = fields[1]
        # Status. For future use.
        dg['status'] = fields[2]

        # Runtime parameters as text format. Parameters separated by ; and lines separated by , delimiter.
        # Text strings refer to names in menus of the K-Controller/SIS.
        tmp = self.FID.read(dg['numBytesCmnPart'] - struct.Struct(format_to_unpack).size)
        rt_text = tmp.decode('UTF-8')
        #print(rt_text)
        dg['runtime_txt'] = rt_text

        # Skip unknown fields.
        self.FID.seek(dg['numBytesCmnPart'] - struct.Struct(format_to_unpack).size, 1)

        return dg

    def read_EMdgmIB(self):
        """
        Read #IB - results from online built-in test (BIST). Definition used for three different BIST datagrams,
        i.e. #IBE (BIST Error report), #IBR (BIST reply) or #IBS (BIST short reply).
        :return: A dictionary containing EMdgmIB.
        """
        # LMD added, untested.
        # TODO: Test with file containing BIST.
        print("WARNING: You are using an incomplete, untested function: read_EMdgmIB.")

        dg = {}
        dg['header'] = self.read_EMdgmHeader()

        format_to_unpack = "1H3B1b1B"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Size in bytes of body part struct. Used for denoting size of rest of the datagram.
        dg['numBytesCmnPart'] = fields[0]
        # 0 = last subset of the message; 1 = more messages to come
        dg['BISTInfo'] = fields[1]
        # 0 = plain text; 1 = use style sheet
        dg['BISTStyle'] = fields[2]
        # The BIST number executed.
        dg['BISTNumber'] = fields[3]
        # 0 = BIST executed with no errors; positive number = warning; negative number = error
        dg['BISTStatus'] = fields[4]

        # Result of the BIST. Starts with a synopsis of the result, followed by detailed descriptions.
        tmp = FID.read(dg['numBytesCmnPart'] - struct.Struct(format_to_unpack).size)
        bist_text = tmp.decode('UTF-8')
        #print(bist_text)
        dg['BISTText'] = bist_text

        # Skip unknown fields.
        self.FID.seek(dg['numBytesCmnPart'] - struct.Struct(format_to_unpack).size, 1)

        '''
        if self.verbose > 2:
            self.print_datagram(dg)
        '''

        return dg

    def read_EMdgmMpartition(self):
        """
        Read multibeam (M) datagrams - data partition info. General for all M datagrams.
        Kongsberg documentation: "If a multibeam depth datagram (or any other large datagram) exceeds the limit of a
        UDP package (64 kB), the datagram is split into several datagrams =< 64 kB before sending from the PU.
        The parameters in this struct will give information of the partitioning of datagrams. K-Controller/SIS merges
        all UDP packets/datagram parts to one datagram, and store it as one datagram in the .kmall files. Datagrams
        stored in .kmall files will therefore always have numOfDgm = 1 and dgmNum = 1, and may have size > 64 kB.
        The maximum number of partitions from PU is given by MAX_NUM_MWC_DGMS and MAX_NUM_MRZ_DGMS."
        :return: A dictionary containing EMdgmMpartition ('partition').
        """
        # LMD tested.

        dg = {}
        format_to_unpack = "2H"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Number of datagram parts to re-join to get one Multibeam datagram. E.g. 3.
        dg['numOfDgms'] = fields[0]
        # Datagram part number, e.g. 2 (of 3).
        dg['dgmNum'] = fields[1]

        if self.verbose > 2:
            self.print_datagram(dg)

        return dg

    def read_EMdgmMbody(self):
        """
        Read multibeam (M) datagrams - body part. Start of body of all M datagrams.
        Contains information of transmitter and receiver used to find data in datagram.
        :return: A dictionary containing EMdgmMbody ('cmnPart').
        """
        # LMD tested.

        dg = {}
        format_to_unpack = "2H8B"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Used for denoting size of current struct.
        dg['numBytesCmnPart'] = fields[0]
        # A ping is made of one or more RX fans and one or more TX pulses transmitted at approximately the same time.
        # Ping counter is incremented at every set of TX pulses
        # (one or more pulses transmitted at approximately the same time).
        dg['pingCnt'] = fields[1]
        # Number of rx fans per ping gives information of how many #MRZ datagrams are generated per ping.
        # Combined with swathsPerPing, number of datagrams to join for a complete swath can be found.
        dg['rxFansPerPing'] = fields[2]
        # Index 0 is the aft swath, port side.
        dg['rxFanIndex'] = fields[3]
        # Number of swaths per ping. A swath is a complete set of across track data.
        # A swath may contain several transmit sectors and RX fans.
        dg['swathsPerPing'] = fields[4]
        # Alongship index for the location of the swath in multi swath mode. Index 0 is the aftmost swath.
        dg['swathAlongPosition'] = fields[5]
        # Transducer used in this tx fan. Index: 0 = TRAI_TX1; 1 = TRAI_TX2 etc.
        dg['txTransducerInd'] = fields[6]
        # Transducer used in this rx fan. Index: 0 = TRAI_RX1; 1 = TRAI_RX2 etc.
        dg['rxTransducerInd'] = fields[7]
        # Total number of receiving units.
        dg['numRxTransducers'] = fields[8]
        # For future use. 0 - current algorithm, >0 - future algorithms.
        dg['algorithmType'] = fields[9]

        # Skip unknown fields.
        self.FID.seek(dg['numBytesCmnPart'] - struct.Struct(format_to_unpack).size, 1)

        if self.verbose > 2:
            self.print_datagram(dg)

        return dg

    def read_EMdgmMRZ_pingInfo(self):
        """
        Read #MRZ - ping info. Information on vessel/system level,
        i.e. information common to all beams in the current ping.
        :return: A dictionary containing EMdgmMRZ_pingInfo ('pingInfo').
        """
        # LMD tested.

        dg = {}
        format_to_unpack_a = "2H1f6B1H11f2h2B1H1I3f2H1f2H6f4B"
        fields = struct.unpack(format_to_unpack_a, self.FID.read(struct.Struct(format_to_unpack_a).size))

        # Number of bytes in current struct.
        dg['numBytesInfoData'] = fields[0]
        # Byte alignment.
        dg['padding0'] = fields[1]

        # # # # # Ping Info # # # # #
        # Ping rate. Filtered/averaged.
        dg['pingRate_Hz'] = fields[2]
        # 0 = Eqidistance; 1 = Equiangle; 2 = High density
        dg['beamSpacing'] = fields[3]
        # Depth mode. Describes setting of depth in K-Controller. Depth mode influences the PUs choice of pulse length
        # and pulse type. If operator has manually chosen the depth mode to use, this is flagged by adding 100 to the
        # mode index. 0 = Very Shallow; 1 = Shallow; 2 = Medium; 3 = Deep; 4 = Deeper; 5 = Very Deep; 6 = Extra Deep;
        # 7 = Extreme Deep
        dg['depthMode'] = fields[4]
        # For advanced use when depth mode is set manually. 0 = Sub depth mode is not used (when depth mode is auto).
        dg['subDepthMode'] = fields[5]
        # Achieved distance between swaths, in percent relative to required swath distance.
        # 0 = function is not used; 100 = achieved swath distance equals required swath distance.
        dg['distanceBtwSwath'] = fields[6]
        # Detection mode. Bottom detection algorithm used. 0 = normal; 1 = waterway; 2 = tracking;
        # 3 = minimum depth; If system running in simulation mode: detectionmode + 100 = simulator.
        dg['detectionMode'] = fields[7]
        # Pulse forms used for current swath. 0 = CW; 1 = mix; 2 = FM
        dg['pulseForm'] = fields[8]
        # TODO: Kongsberg documentation lists padding1 as "Ping rate. Filtered/averaged." This appears to be incorrect.
        # In testing, padding1 prints all zeros. I'm assuming this is for byte alignment, as with other 'padding' cases.
        # Byte alignment.
        dg['padding1'] = fields[9]
        # Ping frequency in hertz. E.g. for EM 2040: 200 000 Hz, 300 000 Hz or 400 000 Hz.
        # If values is less than 100, it refers to a code defined below:
        # -1 = Not used; 0 = 40 - 100 kHz, EM 710, EM 712; 1 = 50 - 100 kHz, EM 710, EM 712;
        # 2 = 70 - 100 kHz, EM 710, EM 712; 3 = 50 kHz, EM 710, EM 712; 4 = 40 kHz, EM 710, EM 712;
        # 180 000 - 400 000 = 180-400 kHz, EM 2040C (10 kHz steps)
        # 200 000 = 200 kHz, EM 2040; 300 000 = 300 kHz, EM 2040; 400 000 = 400 kHz, EM 2040
        dg['frequencyMode_Hz'] = fields[10]
        # Lowest centre frequency of all sectors in this swath. Unit hertz. E.g. for EM 2040: 260 000 Hz.
        dg['freqRangeLowLim_Hz'] = fields[11]
        # Highest centre frequency of all sectors in this swath. Unit hertz. E.g. for EM 2040: 320 000 Hz.
        dg['freqRangeHighLim_Hz'] = fields[12]
        # Total signal length of the sector with longest tx pulse. Unit second.
        dg['maxTotalTxPulseLength_sec'] = fields[13]
        # Effective signal length (-3dB envelope) of the sector with longest effective tx pulse. Unit second.
        dg['maxEffTxPulseLength_sec'] = fields[14]
        # Effective bandwidth (-3dB envelope) of the sector with highest bandwidth.
        dg['maxEffTxBandWidth_Hz'] = fields[15]
        # Average absorption coefficient, in dB/km, for vertical beam at current depth. Not currently in use.
        dg['absCoeff_dBPerkm'] = fields[16]
        # Port sector edge, used by beamformer, Coverage is refered to z of SCS.. Unit degree.
        dg['portSectorEdge_deg'] = fields[17]
        # Starboard sector edge, used by beamformer. Coverage is referred to z of SCS. Unit degree.
        dg['starbSectorEdge_deg'] = fields[18]
        # Coverage achieved, corrected for raybending. Coverage is referred to z of SCS. Unit degree.
        dg['portMeanCov_deg'] = fields[19]
        # Coverage achieved, corrected for raybending. Coverage is referred to z of SCS. Unit degree.
        dg['stbdMeanCov_deg'] = fields[20]
        # Coverage achieved, corrected for raybending. Coverage is referred to z of SCS. Unit meter.
        dg['portMeanCov_m'] = fields[21]
        # Coverage achieved, corrected for raybending. Unit meter.
        dg['starbMeanCov_m'] = fields[22]
        # Modes and stabilisation settings as chosen by operator. Each bit refers to one setting in K-Controller.
        # Unless otherwise stated, default: 0 = off, 1 = on/auto.
        # Bit: 1 = Pitch stabilisation; 2  = Yaw stabilisation; 3 = Sonar mode; 4 = Angular coverage mode;
        # 5 = Sector mode; 6 = Swath along position (0 = fixed, 1 = dynamic); 7-8 = Future use
        dg['modeAndStabilisation'] = fields[23]
        # Filter settings as chosen by operator. Refers to settings in runtime display of K-Controller.
        # Each bit refers to one filter setting. 0 = off, 1 = on/auto.
        # Bit: 1 = Slope filter; 2 = Aeration filter; 3 = Sector filter;
        # 4 = Interference filter; 5 = Special amplitude detect; 6-8 = Future use
        dg['runtimeFilter1'] = fields[24]
        # Filter settings as chosen by operator. Refers to settings in runtime display of K-Controller. 4 bits used per filter.
        # Bits: 1-4 = Range gate size: 0 = small, 1 = normal, 2 = large
        # 5-8 = Spike filter strength: 0 = off, 1= weak, 2 = medium, 3 = strong
        # 9-12 = Penetration filter: 0 = off, 1 = weak, 2 = medium, 3 = strong
        # 13-16 = Phase ramp: 0 = short, 1 = normal, 2 = long
        dg['runtimeFilter2'] = fields[25]
        # Pipe tracking status. Describes how angle and range of top of pipe is determined.
        # 0 = for future use; 1 = PU uses guidance from SIS.
        dg['pipeTrackingStatus'] = fields[26]
        # Transmit array size used. Direction along ship. Unit degree.
        dg['transmitArraySizeUsed_deg'] = fields[27]
        # Receiver array size used. Direction across ship. Unit degree.
        dg['receiveArraySizeUsed_deg'] = fields[28]
        # Operator selected tx power level re maximum. Unit dB. E.g. 0 dB, -10 dB, -20 dB.
        dg['transmitPower_dB'] = fields[29]
        # For marine mammal protection. The parameters describes time remaining until max source level (SL) is achieved.
        # Unit %.
        dg['SLrampUpTimeRemaining'] = fields[30]
        # Byte alignment.
        dg['padding2'] = fields[31]
        # Yaw correction angle applied. Unit degree.
        dg['yawAngle_deg'] = fields[32]

        # # # # # Info of Tx Sector Data Block # # # # #
        # Number of transmit sectors. Also called Ntx in documentation. Denotes how
        # many times the struct EMdgmMRZ_txSectorInfo is repeated in the datagram.
        dg['numTxSectors'] = fields[33]
        # Number of bytes in the struct EMdgmMRZ_txSectorInfo, containing tx sector
        # specific information. The struct is repeated numTxSectors times.
        dg['numBytesPerTxSector'] = fields[34]

        # # # # # Info at Time of Midpoint of First Tx Pulse # # # # #
        # Heading of vessel at time of midpoint of first tx pulse. From active heading sensor.
        dg['headingVessel_deg'] = fields[35]
        # At time of midpoint of first tx pulse. Value as used in depth calculations.
        # Source of sound speed defined by user in K-Controller.
        dg['soundSpeedAtTxDepth_mPerSec'] = fields[36]
        # Tx transducer depth in meters below waterline, at time of midpoint of first tx pulse.
        # For the tx array (head) used by this RX-fan. Use depth of TX1 to move depth point (XYZ)
        # from water line to transducer (reference point of old datagram format).
        dg['txTransducerDepth_m'] = fields[37]
        # Distance between water line and vessel reference point in meters. At time of midpoint of first tx pulse.
        # Measured in the surface coordinate system (SCS).See Coordinate systems 'Coordinate systems' for definition.
        # Used this to move depth point (XYZ) from vessel reference point to waterline.
        dg['z_waterLevelReRefPoint_m'] = fields[38]
        # Distance between *.all reference point and *.kmall reference point (vessel referenece point) in meters,
        # in the surface coordinate system, at time of midpoint of first tx pulse. Used this to move depth point (XYZ)
        # from vessel reference point to the horisontal location (X,Y) of the active position sensor's reference point
        # (old datagram format).
        dg['x_kmallToall_m'] = fields[39]
        # Distance between *.all reference point and *.kmall reference point (vessel referenece point) in meters,
        # in the surface coordinate system, at time of midpoint of first tx pulse. Used this to move depth point (XYZ)
        # from vessel reference point to the horisontal location (X,Y) of the active position sensor's reference point
        # (old datagram format).
        dg['y_kmallToall_m'] = fields[40]
        # Method of position determination from position sensor data:
        # 0 = last position received; 1 = interpolated; 2 = processed.
        dg['latLongInfo'] = fields[41]
        # Status/quality for data from active position sensor. 0 = valid data, 1 = invalid data, 2 = reduced performance
        dg['posSensorStatus'] = fields[42]
        # Status/quality for data from active attitude sensor. 0 = valid data, 1 = invalid data, 2 = reduced performance
        dg['attitudeSensorStatus'] = fields[43]
        # Padding for byte alignment.
        dg['padding3'] = fields[44]

        # For some reason, it doesn't work to do this all in one step, but it works broken up into two steps. *shrug*
        format_to_unpack_b = "2d1f"
        fields = struct.unpack(format_to_unpack_b, self.FID.read(struct.Struct(format_to_unpack_b).size))

        # Latitude (decimal degrees) of vessel reference point at time of midpoint of first tx pulse.
        # Negative on southern hemisphere. Parameter is set to define UNAVAILABLE_LATITUDE if not available.
        dg['latitude_deg'] = fields[0]
        # Longitude (decimal degrees) of vessel reference point at time of midpoint of first tx pulse.
        # Negative on western hemisphere. Parameter is set to define UNAVAILABLE_LONGITUDE if not available.
        dg['longitude_deg'] = fields[1]
        # Height of vessel reference point above the ellipsoid, derived from active GGA sensor.
        # ellipsoidHeightReRefPoint_m is GGA height corrected for motion and installation offsets
        # of the position sensor.
        dg['ellipsoidHeightReRefPoint_m'] = fields[2]

        # Skip unknown fields.
        self.FID.seek(dg['numBytesInfoData'] - struct.Struct(format_to_unpack_a).size
                      - struct.Struct(format_to_unpack_b).size, 1)

        if self.verbose > 2:
            self.print_datagram(dg)

        return dg

    def read_EMdgmMRZ_txSectorInfo(self):
        """
        Read #MRZ - sector info. Information specific to each transmitting sector.
        sectorInfo is repeated numTxSectors (Ntx)- times in datagram.
        :return: A dictionary containing EMdgmMRZ_txSectorInfo ('sectorInfo').
        """
        # NOTE: There's no fields for the number of bytes in this record. Odd.
        # LMD tested.

        dg = {}
        format_to_unpack = "4B7f2B1H"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # TX sector index number, used in the sounding section. Starts at 0.
        dg['txSectorNumb'] = fields[0]
        # TX array number. Single TX, txArrNumber = 0.
        dg['txArrNumber'] = fields[1]
        # Default = 0. E.g. for EM2040, the transmitted pulse consists of three sectors, each transmitted from separate
        # txSubArrays. Orientation and numbers are relative the array coordinate system. Sub array installation offsets
        # can be found in the installation datagram, #IIP. 0 = Port subarray; 1 = middle subarray; 2 = starboard subarray
        dg['txSubArray'] = fields[2]
        # Byte alignment.
        dg['padding0'] = fields[3]
        # Transmit delay of the current sector/subarray. Delay is the time from the midpoint of the current transmission
        # to midpoint of the first transmitted pulse of the ping, i.e. relative to the time used in the datagram header.
        dg['sectorTransmitDelay_sec'] = fields[4]
        # Along ship steering angle of the TX beam (main lobe of transmitted pulse),
        # angle referred to transducer array coordinate system. Unit degree.
        dg['tiltAngleReTx_deg'] = fields[5]
        # Unit dB re 1 microPascal.
        dg['txNominalSourceLevel_dB'] = fields[6]
        # 0 = no focusing applied.
        dg['txFocusRange_m'] = fields[7]
        # Centre frequency. Unit hertz.
        dg['centreFreq_Hz'] = fields[8]
        # FM mode: effective bandwidth; CW mode: 1/(effective TX pulse length)
        dg['signalBandWidth_Hz'] = fields[9]
        # Also called pulse length. Unit second.
        dg['totalSignalLength_sec'] = fields[10]
        # Transmit pulse is shaded in time (tapering). Amplitude shading in %.
        # cos2- function used for shading the TX pulse in time.
        dg['pulseShading'] = fields[11]
        # Transmit signal wave form. 0 = CW; 1 = FM upsweep; 2 = FM downsweep.
        dg['signalWaveForm'] = fields[12]
        # Byte alignment.
        dg['padding1'] = fields[13]

        if self.verbose > 2:
            self.print_datagram(dg)

        return dg

    def read_EMdgmMRZ_rxInfo(self):
        """
        Read #MRZ - receiver specific information. Information specific to the receiver unit used in this swath.
        :return: A dictionary containing EMdgmMRZ_rxInfo ('rxInfo').
        """
        # LMD tested.

        dg = {}
        format_to_unpack = "4H4f4H"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Bytes in current struct.
        dg['numBytesRxInfo'] = fields[0]
        # Maximum number of main soundings (bottom soundings) in this datagram, extra detections
        # (soundings in water column) excluded. Also referred to as Nrx. Denotes how many bottom points
        # (or loops) given in the struct EMdgmMRZ_sounding_def.
        dg['numSoundingsMaxMain'] = fields[1]
        # Number of main soundings of valid quality. Extra detections not included.
        dg['numSoundingsValidMain'] = fields[2]
        # Bytes per loop of sounding (per depth point), i.e. bytes per loops of the struct EMdgmMRZ_sounding_def.
        dg['numBytesPerSounding'] = fields[3]
        # Sample frequency divided by water column decimation factor. Unit hertz.
        dg['WCSampleRate'] = fields[4]
        # Sample frequency divided by seabed image decimation factor. Unit hertz.
        dg['seabedImageSampleRate'] = fields[5]
        # Backscatter level, normal incidence. Unit dB.
        dg['BSnormal_dB'] = fields[6]
        # Backscatter level, oblique incidence. Unit dB.
        dg['BSoblique_dB'] = fields[7]
        # extraDetectionAlarmFlag = sum of alarm flags. Range 0-10.
        dg['extraDetectionAlarmFlag'] = fields[8]
        # Sum of extradetection from all classes. Also refered to as Nd.
        dg['numExtraDetections'] = fields[9]
        # Range 0-10.
        dg['numExtraDetectionClasses'] = fields[10]
        # Number of bytes in the struct EMdgmMRZ_extraDetClassInfo_def.
        dg['numBytesPerClass'] = fields[11]

        # Skip unknown fields.
        self.FID.seek(dg['numBytesRxInfo'] - struct.Struct(format_to_unpack).size, 1)

        if self.verbose > 2:
            self.print_datagram(dg)

        return dg

    def read_EMdgmMRZ_extraDetClassInfo(self):
        """
        Read #MRZ - extra detection class information. To be entered in loop numExtraDetectionClasses times.
        :return: A dictionary containing EMdgmMRZ_extra DetClassInfo ('extraDetClassInfo').
        """
        # NOTE: There's no fields for the number of bytes in this record. Odd.
        # TODO: Need to test with file containing extra detections.

        dg = {}
        format_to_unpack = "1H1b1B"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Number of extra detection in this class.
        dg['numExtraDetInClass'] = fields[0]
        # Byte alignment.
        dg['padding'] = fields[1]
        # 0 = no alarm; 1 = alarm.
        dg['alarmFlag'] = fields[2]

        if self.verbose > 2:
            self.print_datagram(dg)

        return dg

    def read_EMdgmMRZ_sounding(self):
        """
        Read #MRZ - data for each sounding, e.g. XYZ, reflectivity, two way travel time etc. Also contains
        information necessary to read seabed image following this datablock (number of samples in SI etc.).
        To be entered in loop (numSoundingsMaxMain + numExtraDetections) times.
        :return: A dictionary containing EMdgmMRZ_sounding ('sounding').
        """
        # NOTE: There's no fields for the number of bytes in this record. Odd.
        # LMD tested.

        dg = {}
        format_to_unpack = "1H8B1H6f2H18f4H"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Sounding index. Cross reference for seabed image.
        # Valid range: 0 to (numSoundingsMaxMain+numExtraDetections)-1, i.e. 0 - (Nrx+Nd)-1.
        dg['soundingIndex'] = fields[0]
        # Transmitting sector number. Valid range: 0-(Ntx-1), where Ntx is numTxSectors.
        dg['txSectorNumb'] = fields[1]

        # # # # # D E T E C T I O N   I N F O # # # # #
        # Bottom detection type. Normal bottom detection, extra detection, or rejected.
        # 0 = normal detection; 1 = extra detection; 2 = rejected detection
        # In case 2, the estimated range has been used to fill in amplitude samples in the seabed image datagram.
        dg['detectionType'] = fields[2]
        # Method for determining bottom detection, e.g. amplitude or phase.
        # 0 = no valid detection; 1 = amplitude detection; 2 = phase detection; 3-15 for future use.
        dg['detectionMethod'] = fields[3]
        # For Kongsberg use.
        dg['rejectionInfo1'] = fields[4]
        # For Kongsberg use.
        dg['rejectionInfo2'] = fields[5]
        # For Kongsberg use.
        dg['postProcessingInfo'] = fields[6]
        # Only used by extra detections. Detection class based on detected range.
        # Detection class 1 to 7 corresponds to value 0 to 6. If the value is between 100 and 106,
        # the class is disabled by the operator. If the value is 107, the detections are outside the treshhold limits.
        dg['detectionClass'] = fields[7]
        # Detection confidence level.
        dg['detectionConfidenceLevel'] = fields[8]
        # Byte alignment.
        dg['padding'] = fields[9]
        # Unit %. rangeFactor = 100 if main detection.
        dg['rangeFactor'] = fields[10]
        # Estimated standard deviation as % of the detected depth. Quality Factor (QF) is
        # calculated from IFREMER Quality Factor (IFQ): QF=Est(dz)/z=100*10^-IQF
        dg['qualityFactor'] = fields[11]
        # Vertical uncertainty, based on quality factor (QF, qualityFactor).
        dg['detectionUncertaintyVer_m'] = fields[12]
        # Horizontal uncertainty, based on quality factor (QF, qualityFactor).
        dg['detectionUncertaintyHor_m'] = fields[13]
        # Detection window length. Unit second. Sample data range used in final detection.
        dg['detectionWindowLength_sec'] = fields[14]
        # Measured echo length. Unit second.
        dg['echoLength_sec'] = fields[15]

        # # # # # W A T E R   C O L U M N   P A R A M E T E R S # # # # #
        # Water column beam number. Info for plotting soundings together with water column data.
        dg['WCBeamNumb'] = fields[16]
        # Water column range. Range of bottom detection, in samples.
        dg['WCrange_samples'] = fields[17]
        # Water column nominal beam angle across. Re vertical.
        dg['WCNomBeamAngleAcross_deg'] = fields[18]

        # # # # # REFLECTIVITY DATA (BACKSCATTER (BS) DATA) # # # # #
        # Mean absorption coefficient, alfa. Used for TVG calculations. Value as used. Unit dB/km.
        dg['meanAbsCoeff_dbPerkm'] = fields[19]
        # Beam intensity, using the traditional KM special TVG.
        dg['reflectivity1_dB'] = fields[20]
        # Beam intensity (BS), using TVG = X log(R) + 2 alpha R. X (operator selected) is common to all beams in
        # datagram. Alpha (variabel meanAbsCoeff_dBPerkm) is given for each beam (current struct).
        # BS = EL - SL - M + TVG + BScorr, where EL= detected echo level (not recorded in datagram),
        # and the rest of the parameters are found below.
        dg['reflectivity2_dB'] = fields[21]
        # Receiver sensitivity (M), in dB, compensated for RX beampattern
        # at actual transmit frequency at current vessel attitude.
        dg['receiverSensitivityApplied_dB'] = fields[22]
        # Source level (SL) applied (dB): SL = SLnom + SLcorr, where SLnom = Nominal maximum SL,
        # recorded per TX sector (variable txNominalSourceLevel_dB in struct EMdgmMRZ_txSectorInfo_def) and
        # SLcorr = SL correction relative to nominal TX power based on measured high voltage power level and
        # any use of digital power control. SL is corrected for TX beampattern along and across at actual transmit
        # frequency at current vessel attitude.
        dg['sourceLevelApplied_dB'] = fields[23]
        # Backscatter (BScorr) calibration offset applied (default = 0 dB).
        dg['BScalibration_dB'] = fields[24]
        # Time Varying Gain (TVG) used when correcting reflectivity.
        dg['TVG_dB'] = fields[25]

        # # # # # R A N G E   A N D   A N G L E   D A T A # # # # #
        # Angle relative to the RX transducer array, except for ME70,
        # where the angles are relative to the horizontal plane.
        dg['beamAngleReRx_deg'] = fields[26]
        # Applied beam pointing angle correction.
        dg['beamAngleCorrection_deg'] = fields[27]
        # Two way travel time (also called range). Unit second.
        dg['twoWayTravelTime_sec'] = fields[28]
        # Applied two way travel time correction. Unit second.
        dg['twoWayTravelTimeCorrection_sec'] = fields[29]

        # # # # # G E O R E F E R E N C E D   D E P T H   P O I N T S # # # # #
        # Distance from vessel reference point at time of first tx pulse in ping, to depth point.
        # Measured in the surface coordinate system (SCS), see Coordinate systems for definition. Unit decimal degrees.
        dg['deltaLatitude_deg'] = fields[30]
        # Distance from vessel reference point at time of first tx pulse in ping, to depth point.
        # Measured in the surface coordinate system (SCS), see Coordinate systems for definition. Unit decimal degrees.
        dg['deltaLongitude_deg'] = fields[31]
        # Vertical distance z. Distance from vessel reference point at time of first tx pulse in ping, to depth point.
        # Measured in the surface coordinate system (SCS), see Coordinate systems for definition.
        dg['z_reRefPoint_m'] = fields[32]
        # Horizontal distance y. Distance from vessel reference point at time of first tx pulse in ping, to depth point.
        # Measured in the surface coordinate system (SCS), see Coordinate systems for definition.
        dg['y_reRefPoint_m'] = fields[33]
        # Horizontal distance x. Distance from vessel reference point at time of first tx pulse in ping, to depth point.
        # Measured in the surface coordinate system (SCS), see Coordinate systems for definition.
        dg['x_reRefPoint_m'] = fields[34]
        # Beam incidence angle adjustment (IBA) unit degree.
        dg['beamIncAngleAdj_deg'] = fields[35]
        # For future use.
        dg['realTimeCleanInfo'] = fields[36]

        # # # # # S E A B E D   I M A G E # # # # #
        # Seabed image start range, in sample number from transducer. Valid only for the current beam.
        dg['SIstartRange_samples'] = fields[37]
        # Seabed image. Number of the centre seabed image sample for the current beam.
        dg['SIcentreSample'] = fields[38]
        # Seabed image. Number of range samples from the current beam, used to form the seabed image.
        dg['SInumSamples'] = fields[39]

        if self.verbose > 2:
            self.print_datagram(dg)

        return dg

    def read_EMdgmMRZ(self):
        """
        A method to read a full #MRZ datagram.
        Kongsberg documentation: "The datagram also contains seabed image data. Depths points (x,y,z) are calculated
        in meters, georeferred to the position of the vessel reference point at the time of the first transmitted pulse
        of the ping. The depth point coordinates x and y are in the surface coordinate system (SCS), and are also given
        as delta latitude and delta longitude, referred to origo of the VCS/SCS, at the time of the midpoint of the
        first transmitted pulse of the ping (equals time used in the datagram header timestamp). See Coordinate systems
        for introduction to spatial reference points and coordinate systems. Reference points are also described in
        Reference points and offsets."
        :return: A dictionary including full MRZ datagram information including EMdgmHeader ('header'), EMdgmMpartition
        ('Mpart'), EMdgmbody ('Mbody'), EMdgmMRZ_pingInfo ('pingInfo'), EMdgmMRZ_txSectorInfo ('txSectorInfo'),
        EMdgmMRZ_rxInfo ('rxinfo'), EMdgmMRZ_sounding ('soundings'), and ('SIsample_desidB').
        """
        # LMD tested.

        start = self.FID.tell()

        dg = {}
        dg['header'] = self.read_EMdgmHeader()
        dg['partition'] = self.read_EMdgmMpartition()
        dg['cmnPart'] = self.read_EMdgmMbody()
        dg['pingInfo'] = self.read_EMdgmMRZ_pingInfo()

        # Read TX sector info for each sector
        txSectorInfo = []
        for sector in range(dg['pingInfo']['numTxSectors']):
            txSectorInfo.append(self.read_EMdgmMRZ_txSectorInfo())
        dg['txSectorInfo'] = self.listofdicts2dictoflists(txSectorInfo)

        # Read reInfo
        dg['rxInfo'] = self.read_EMdgmMRZ_rxInfo()

        # Read extra detect metadata if they exist.
        extraDetClassInfo = []
        for detclass in range(dg['rxInfo']['numExtraDetectionClasses']):
            extraDetClassInfo.append(self.read_EMdgmMRZ_extraDetClassInfo())
        dg['extraDetClassInfo'] = self.listofdicts2dictoflists(extraDetClassInfo)

        # Read the sounding data.
        soundings = []
        Nseabedimage_samples = 0
        for record in range(dg['rxInfo']['numExtraDetections'] +
                              dg['rxInfo']['numSoundingsMaxMain']):
            soundings.append(self.read_EMdgmMRZ_sounding())
            Nseabedimage_samples += soundings[record]['SInumSamples']
        dg['sounding'] = self.listofdicts2dictoflists(soundings)

        # Read the seabed imagery.
        # Seabed image sample amplitude, in 0.1 dB. Actual number of seabed image samples (SIsample_desidB) to be found
        # by summing parameter SInumSamples in struct EMdgmMRZ_sounding_def for all beams. Seabed image data are raw
        # beam sample data taken from the RX beams. The data samples are selected based on the bottom detection ranges.
        # First sample for each beam is the one with the lowest range. The centre sample from each beam is geo
        # referenced (x, y, z data from the detections). The BS corrections applied at the centre sample are the same
        # as used for reflectivity2_dB (struct EMdgmMRZ_sounding_def).
        format_to_unpack = str(Nseabedimage_samples) + "h"

        dg['SIsample_desidB'] = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Seek to end of the packet.
        self.FID.seek(start + dg['header']['numBytesDgm'], 0)

        return dg

    def read_EMdgmMWCtxInfo(self):
        """
        Read #MWC - data block 1: transmit sectors, general info for all sectors.
        :return: A dictionary containing EMdgmMWCtxInfo.
        """
        # LMD added, tested.

        dg = {}
        format_to_unpack = "3H1h1f"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Number of bytes in current struct.
        dg['numBytesTxInfo'] = fields[0]
        # Number of transmitting sectors (Ntx). Denotes the number of times
        # the struct EMdgmMWCtxSectorData is repeated in the datagram.
        dg['numTxSectors'] = fields[1]
        # Number of bytes in EMdgmMWCtxSectorData.
        dg['numBytesPerTxSector'] = fields[2]
        # Byte alignment.
        dg['padding'] = fields[3]
        # Heave at vessel reference point, at time of ping, i.e. at midpoint of first tx pulse in rxfan.
        dg['heave_m'] = fields[4]

        # Skip unknown fields.
        self.FID.seek(dg['numBytesTxInfo'] - struct.Struct(format_to_unpack).size, 1)

        '''
        if self.verbose > 2:
            self.print_datagram(dg)
        '''

        return dg

    def read_EMdgmMWCtxSectorData(self):
        """
        Read #MWC - data block 1: transmit sector data, loop for all i = numTxSectors.
        :return: A dictionary containing EMdgmMWCtxSectorData
        """
        # NOTE: There's no fields for the number of bytes in this record. Odd.
        # LMD added, tested.

        dg = {}
        format_to_unpack = "3f1H1h"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Along ship steering angle of the TX beam (main lobe of transmitted pulse), angle referred to transducer face.
        # Angle as used by beamformer (includes stabilisation). Unit degree.
        dg['tiltAngleReTx_deg'] = fields[0]
        # Centre frequency of current sector. Unit hertz.
        dg['centreFreq_Hz'] = fields[1]
        # Corrected for frequency, sound velocity and tilt angle. Unit degree.
        dg['txBeamWidthAlong_deg'] = fields[2]
        # Transmitting sector number.
        dg['txSectorNum'] = fields[3]
        # Byte alignment.
        dg['padding'] = fields[4]

        '''
        if self.verbose > 2:
            self.print_datagram(dg)
        '''

        return dg

    def read_EMdgmMWCrxInfo(self):
        """
        Read #MWC - data block 2: receiver, general info.
        :return: A dictionary containing EMdgmMWCrxInfo.
        """
        # LMD added, tested.

        dg = {}
        format_to_unpack = "2H3B1b2f"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Number of bytes in current struct.
        dg['numBytesRxInfo'] = fields[0]
        # Number of beams in this datagram (Nrx).
        dg['numBeams'] = fields[1]
        # Bytes in EMdgmMWCrxBeamData struct, excluding sample amplitudes (which have varying lengths).
        dg['numBytesPerBeamEntry'] = fields[2]
        # 0 = off; 1 = low resolution; 2 = high resolution.
        dg['phaseFlag'] = fields[3]
        # Time Varying Gain function applied (X). X log R + 2 Alpha R + OFS + C, where X and C is documented
        # in #MWC datagram. OFS is gain offset to compensate for TX source level, receiver sensitivity etc.
        dg['TVGfunctionApplied'] = fields[4]
        # Time Varying Gain offset used (OFS), unit dB. X log R + 2 Alpha R + OFS + C, where X and C is documented
        # in #MWC datagram. OFS is gain offset to compensate for TX source level, receiver sensitivity etc.
        dg['TVGoffset_dB'] = fields[5]
        # The sample rate is normally decimated to be approximately the same as the bandwidth of the transmitted pulse.
        # Unit hertz.
        dg['sampleFreq_Hz'] = fields[6]
        # Sound speed at transducer, unit m/s.
        dg['soundVelocity_mPerSec'] = fields[7]

        # Skip unknown fields.
        self.FID.seek(dg['numBytesRxInfo'] - struct.Struct(format_to_unpack).size, 1)
        '''
        if self.verbose > 2:
            self.print_datagram(dg)
        '''

        return dg

    def read_EMdgmMWCrxBeamData(self):
        """
        Read #MWC - data block 2: receiver, specific info for each beam.
        :return: A dictionary containing EMdgmMWCrxBeamData.
        """
        # NOTE: There's no fields for the number of bytes in this record. Odd.
        # LMD added,  partially tested.
        # TODO: Test with water column data, phaseFlag = 1 and phaseFlag = 2 to ensure this continues to function properly.

        dg = {}
        format_to_unpack = "1f4H"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        dg['beamPointAngReVertical_deg'] = fields[0]
        dg['startRangeSampleNum'] = fields[1]
        # Two way range in samples. Approximation to calculated distance from tx to bottom detection
        # [meters] = soundVelocity_mPerSec * detectedRangeInSamples / (sampleFreq_Hz * 2).
        # The detected range is set to zero when the beam has no bottom detection.
        dg['detectedRangeInSamples'] = fields[2]
        dg['beamTxSectorNum'] = fields[3]
        # Number of sample data for current beam. Also denoted Ns.
        dg['numSampleData'] = fields[4]

        # Pointer to start of array with Water Column data. Length of array = numSampleData.
        # Sample amplitudes in 0.5 dB resolution. Size of array is numSampleData * int8_t.
        # Amplitude array is followed by phase information if phaseFlag >0.
        # Use (numSampleData * int8_t) to jump to next beam, or to start of phase info for this beam, if phase flag > 0.
        format_to_unpack = str(dg['numSampleData']) + "b"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        dg['sampleAmplitude05dB_p'] = fields

        '''
        if self.verbose > 2:
            self.print_datagram(dg)
        '''

        return dg

    def read_EMdgmMWCrxBeamPhase1(self, numBeams, numSampleData):
        """
        Read #MWC - Beam sample phase info, specific for each beam and water column sample.
        numBeams * numSampleData = (Nrx * Ns) entries. Only added to datagram if phaseFlag = 1.
        Total size of phase block is numSampleData * int8_t.
        :return: A dictionary containing EMdgmCrxBeamPhase1.
        """
        # LMD added, untested.
        # TODO: Test with water column data, phaseFlag = 1 to complete/test this function.
        print("WARNING: You are using an incomplete, untested function: read_EMdgmMWCrxBeamPhase1.")

        dg = {}
        format_to_unpack = str(numBeams * numSampleData) + "b"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Rx beam phase in 180/128 degree resolution.
        dg['rxBeamPhase'] = fields

        return dg

    def read_EMdgmMWCrxBeamPhase2(self, numBeams, numSampleData):
        """
        Read #MWC - Beam sample phase info, specific for each beam and water column sample.
        numBeams * numSampleData = (Nrx * Ns) entries. Only added to datagram if phaseFlag = 2.
        Total size of phase block is numSampleData * int16_t.
        :return: A dictionary containing EMdgmCrxBeamPhase2.
        """
        # LMD added, untested.
        # TODO: Test with water column data, phaseFlag = 2 to complete/test this function.
        print("WARNING: You are using an incomplete, untested function: read_EMdgmMWCrxBeamPhase2.")

        dg = {}
        format_to_unpack = str(numBeams * numSampleData) + "h"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Rx beam phase in 0.01 degree resolution.
        dg['rxBeamPhase'] = fields

        return dg

    def read_EMdgmMWC(self):
        """
        Read #MWC - Multibeam Water Column Datagram. Entire datagram containing several sub structs.
        :return: A dictionary containing EMdgmMWC.
        """
        # LMD added, partially tested.
        # NOTE: Tested with phaseFlag = 0.
        # TODO: Test with water column data, phaseFlag = 1 and phaseFlag = 2 to fully complete/test this function.

        start = self.FID.tell()

        dg = {}
        dg['header'] = self.read_EMdgmHeader()
        dg['partition'] = self.read_EMdgmMpartition()
        dg['cmnPart'] = self.read_EMdgmMbody()
        dg['txInfo'] = self.read_EMdgmMWCtxInfo()

        # Read TX sector info for each sector
        txSectorData = []
        for sector in range(dg['txInfo']['numTxSectors']):
            txSectorData.append(self.read_EMdgmMWCtxSectorData())
        dg['sectorData'] = self.listofdicts2dictoflists(txSectorData)

        dg['rxInfo'] = self.read_EMdgmMWCrxInfo()

        # Pointer to beam related information. Struct defines information about data for a beam. Beam information is
        # followed by sample amplitudes in 0.5 dB resolution . Amplitude array is followed by phase information if
        # phaseFlag >0. These data defined by struct EMdgmMWCrxBeamPhase1_def (int8_t) or struct
        # EMdgmMWCrxBeamPhase2_def (int16_t) if indicated in the field phaseFlag in struct EMdgmMWCrxInfo_def.
        # Length of data block for each beam depends on the operators choice of phase information (see table):
        '''
                phaseFlag:      Beam Block Size: 
                0               numBytesPerBeamEntry + numSampleData * size(sampleAmplitude05dB_p)
                1               numBytesPerBeamEntry + numSampleData * size(sampleAmplitude05dB_p)
                                    + numSampleData * size(EMdgmMWCrxBeamPhase1_def)
                2               numBytesPerBeamEntry + numSampleData * size(sampleAmplitude05dB_p)
                                    + numSampleData * size(EMdgmMWCrxBeamPhase2_def)
        '''

        rxBeamData = []
        rxPhaseInfo = []
        for idx in range(dg['rxInfo']['numBeams']):
            rxBeamData.append(self.read_EMdgmMWCrxBeamData())

            if dg['rxInfo']['phaseFlag'] == 0:
                pass

            elif dg['rxInfo']['phaseFlag'] == 1:
                # TODO: Test with water column data, phaseFlag = 1 to complete/test this function.
                rxPhaseInfo.append(self.read_EMdgmMWCrxBeamPhase1(dg['rxInfo']['numBeams'],
                                                                  rxBeamData[idx]['numSampleData']))

            elif dg['rxInfo']['phaseFlag'] == 2:
                # TODO: Test with water column data, phaseFlag = 2 to complete/test this function.
                rxPhaseInfo.append(self.read_EMdgmMWCrxBeamPhase1(dg['rxInfo']['numBeams'],
                                                                  rxBeamData[idx]['numSampleData']))

            else:
                print("ERROR: phaseFlag error in read_EMdgmMWC function.")

        dg['beamData'] = self.listofdicts2dictoflists(rxBeamData)

        # TODO: Should this be handled in a different way? By this method, number of fields in dg is variable.
        if dg['rxInfo']['phaseFlag'] == 1 or dg['rxInfo']['phaseFlag'] == 2:
            dg['phaseInfo'] = self.listofdicts2dictoflists(rxPhaseInfo)

        # Seek to end of the packet.
        self.FID.seek(start + dg['header']['numBytesDgm'], 0)

        return dg

    def read_EMdgmScommon(self):
        """
        Read sensor (S) output datagram - common part for all external sensors.
        :return: A dictionary containing EMdgmScommon ('cmnPart').
        """
        # LMD added, tested.

        dg = {}
        format_to_unpack = "4H"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Size in bytes of current struct. Used for denoting size of rest of
        # datagram in cases where only one datablock is attached.
        dg['numBytesCmnPart'] = fields[0]
        # Sensor system number, as indicated when setting up the system in K-Controller installation menu. E.g.
        # position system 0 refers to system POSI_1 in installation datagram #IIP. Check if this sensor system is
        # active by using #IIP datagram. #SCL - clock datagram:
        '''
                Bit:    Sensor system: 
                0       Time syncronisation from clock data
                1       Time syncronisation from active position data
                2       1 PPS is used
        '''
        dg['sensorSystem'] = fields[1]
        # Sensor status. To indicate quality of sensor data is valid or invalid. Quality may be invalid even if sensor
        # is active and the PU receives data. Bit code vary according to type of sensor.
        # Bits 0 -7 common to all sensors and #MRZ sensor status:
        '''
                Bit:    Sensor data: 
                0       0 = Data OK; 1 = Data OK and sensor is chosen as active; 
                        #SCL only: 1 = Valid data and 1PPS OK
                1       0
                2       0 = Data OK; 1 = Reduced performance; 
                        #SCL only: 1 = Reduced performance, no time synchronisation of PU
                3       0
                4       0 = Data OK; 1 = Invalid data
                5       0
                6       0 = Velocity from sensor; 1 = Velocity calculated by PU
                7       0
        '''
        # For #SPO (position) and CPO (position compatibility) datagrams, bit 8 - 15:
        '''
                Bit:    Sensor data: 
                8       0
                9       0 = Time from PU used (system); 1 = Time from datagram used (e.g. from GGA telegram)
                10      0 = No motion correction; 1 = With motion correction
                11      0 = Normal quality check; 1 = Operator quality check. Data always valid.
                12      0
                13      0
                14      0
                15      0
        '''
        dg['sensorStatus'] = fields[2]
        dg['padding'] = fields[3]

        # Skip unknown fields.
        self.FID.seek(dg['numBytesCmnPart'] - struct.Struct(format_to_unpack).size, 1)

        return dg

    def read_EMdgmSPOdataBlock(self):
        """
        Read #SPO - Sensor position data block. Data from active sensor is corrected data for position system
        installation parameters. Data is also corrected for motion (roll and pitch only) if enabled by K-Controller
        operator. Data given both decoded and corrected (active sensors), and raw as received from sensor in text
        string.
        :return: A dictionary containing EMdgmSPOdataBlock ('sensorData').
        """
        # NOTE: There's no fields for the number of bytes in this record. Odd.
        # LMD added, tested.

        dg = {}
        format_to_unpack = "2I1f"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # UTC time from position sensor. Unit seconds. Epoch 1970-01-01. Nanosec part to be added for more exact time.
        dg['timeFromSensor_sec'] = fields[0]
        # UTC time from position sensor. Unit nano seconds remainder.
        dg['timeFromSensor_nanosec'] = fields[1]
        dg['datetime'] = datetime.datetime.utcfromtimestamp(dg['timeFromSensor_sec']
                                                            + dg['timeFromSensor_nanosec'] / 1.0E9)
        # Only if available as input from sensor. Calculation according to format.
        dg['posFixQuality_m'] = fields[2]

        # For some reason, it doesn't work to do this all in one step, but it works broken up into two steps. *shrug*
        format_to_unpack = "2d3f250s"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Motion corrected (if enabled in K-Controller) data as used in depth calculations. Referred to vessel
        # reference point. Unit decimal degree. Parameter is set to define UNAVAILABLE_LATITUDE if sensor inactive.
        dg['correctedLat_deg'] = fields[0]
        # Motion corrected (if enabled in K-Controller) data as used in depth calculations. Referred to vessel
        # reference point. Unit decimal degree. Parameter is set to define UNAVAILABLE_LONGITUDE if sensor inactive.
        dg['correctedLong_deg'] = fields[1]
        # Speed over ground. Unit m/s. Motion corrected (if enabled in K-Controller) data as used in depth calculations.
        # If unavailable or from inactive sensor, value set to define UNAVAILABLE_SPEED.
        dg['speedOverGround_mPerSec'] = fields[2]
        # Course over ground. Unit degree. Motion corrected (if enabled in K-Controller) data as used in depth
        # calculations. If unavailable or from inactive sensor, value set to define UNAVAILABLE_COURSE.
        dg['courseOverGround_deg'] = fields[3]
        # Height of vessel reference point above the ellipsoid. Unit meter.
        # Motion corrected (if enabled in K-Controller) data as used in depth calculations.
        # If unavailable or from inactive sensor, value set to define UNAVAILABLE_ELLIPSOIDHEIGHT.
        dg['ellipsoidHeightReRefPoint_m'] = fields[4]

        # TODO: This is an array of (max?) length MAX_SPO_DATALENGTH; do something else here?
        # TODO: Get MAX_SPO_DATALENGTH from datagram instead of hard-coding in format_to_unpack.
        # TODO: This works for now, but maybe there is a smarter way?
        # Position data as received from sensor, i.e. uncorrected for motion etc.
        tmp = fields[5]
        dg['posDataFromSensor'] = tmp[0:tmp.find(b'\r\n')]

        if self.verbose > 2:
            self.print_datagram(dg)

        return dg

    def read_EMdgmSPO(self):
        """
        Read #SPO - Struct of position sensor datagram. From Data from active sensor will be motion corrected if
        indicated by operator. Motion correction is applied to latitude, longitude, speed, course and ellipsoidal
        height. If the sensor is inactive, the fields will be marked as unavailable, defined by the parameters define
        UNAVAILABLE_LATITUDE etc.
        :return: A dictionary of dictionaries, including EMdgmHeader ('header'), EMdgmScommon ('cmnPart'), and
        EMdgmSPOdataBlock ('sensorData').
        """
        # LMD added, tested.

        start = self.FID.tell()

        dg = {}
        dg['header'] = self.read_EMdgmHeader()
        dg['cmnPart'] = self.read_EMdgmScommon()
        dg['sensorData'] = self.read_EMdgmSPOdataBlock()

        # Seek to end of the packet.
        self.FID.seek(start + dg['header']['numBytesDgm'], 0)

        return dg

    def read_EMdgmSKMinfo(self):
        """
        Read sensor (S) output datagram - info of KMB datagrams.
        :return: A dictionary containing EMdgmSKMinfo ('infoPart').
        """
        # LMD tested.

        dg = {}
        format_to_unpack = "1H2B4H"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Size in bytes of current struct. Used for denoting size of rest of datagram
        # in cases where only one datablock is attached.
        dg['numBytesInfoPart'] = fields[0]
        # Attitude system number, as numbered in installation parameters.
        # E.g. system 0 referes to system ATTI_1 in installation datagram #IIP.
        dg['sensorSystem'] = fields[1]
        # Sensor status. Summarise the status fields of all KM binary samples added in this datagram (status in struct
        # KMbinary_def). Only available data from input sensor format is summarised. Available data found in
        # sensorDataContents. Bits 0 -7 common to all sensors and #MRZ sensor status:
        '''
                Sensor Status:
                    Bit: 0      0 Data OK, 1 Data OK and Sensor is active
                    Bit: 1      0
                    Bit: 2      0 Data OK, 1 Data Reduced Performance
                    Bit: 3      0
                    Bit: 4      0 Data OK, 1 Invalid Data
                    Bit: 5      0
                    Bit: 6      0 Velocity from Sensor, 1 Velocity from PU
        '''
        dg['sensorStatus'] = fields[2]
        # Format of raw data from input sensor, given in numerical code according to table below.
        '''
                Code:   Sensor Format: 
                1:      KM Binary Sensor Format
                2:      EM 3000 data
                3:      Sagem
                4:      Seapath binary 11
                5:      Seapath binary 23
                6:      Seapath binary 26
                7:      POS/MV Group 102/103
                8:      Coda Octopus MCOM
        '''
        dg['sensorInputFormat'] = fields[3]
        # Number of KM binary sensor samples added in this datagram.
        dg['numSamplesArray'] = fields[4]
        # Length in bytes of one whole KM binary sensor sample.
        dg['numBytesPerSample'] = fields[5]
        # Field to indicate which information is available from the input sensor, at the given sensor format.
        # 0 = not available; 1 = data is available
        # The bit pattern is used to determine sensorStatus from status field in #KMB samples. Only data available from
        # sensor is check up against invalid/reduced performance in status, and summaries in sensorStatus.
        # E.g. the binary 23 format does not contain delayed heave. This is indicated by setting bit 6 in
        # sensorDataContents to 0. In each sample in #KMB output from PU, the status field (struct KMbinary_def) for
        # INVALID delayed heave (bit 6) is set to 1. The summaries sensorStatus in struct EMdgmSKMinfo_def will then
        # be sets to 0 if all available data is ok. Expected data field in sensor input:
        '''
                    Indicates what data is available in the given sensor format
                    Bit:     Sensor Data:
                    0        Horizontal posistion and velocity
                    1        Roll and pitch
                    2        Heading
                    3        Heave and vertical velocity
                    4        Acceleration
                    5        Error fields
                    6        Delayed Heave
        '''
        dg['sensorDataContents'] = fields[6]

        # Skip unknown fields.
        self.FID.seek(dg['numBytesInfoPart'] - struct.Struct(format_to_unpack).size, 1)

        if self.verbose > 2:
            self.print_datagram(dg)

        return dg

    def read_KMdelayedHeave(self):
        """
        Read #SKM - delayed heave. Included if available from sensor.
        :return: A dictionary containing KMdelayedHeave.
        """
        # NOTE: There's no fields for the number of bytes in this record. Odd.
        # LMD tested with 'empty' delayed heave fields.
        # TODO: Test with data containing delayed heave.

        dg = {}
        format_to_unpack = "2I1f"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        dg['time_sec'] = fields[0]
        dg['time_nanosec'] = fields[1]
        dg['datetime'] = datetime.datetime.utcfromtimestamp(dg['time_sec'] + dg['time_nanosec'] / 1.0E9)
        # Delayed heave. Unit meter.
        dg['delayedHeave_m'] = fields[2]

        if self.verbose > 2:
            self.print_datagram(dg)

        return dg

    def read_KMbinary(self):
        """
        Read #SKM - sensor attitude data block. Data given timestamped, not corrected.
        See Coordinate Systems for definition of positive angles and axis.
        :return: A dictionary containing KMbinary.
        """
        # LMD tested.

        dg = {}
        format_to_unpack = "4B"
        fields = self.FID.read(struct.Struct(format_to_unpack).size)

        # KMB
        dg['dgmType'] = fields.decode('utf-8')

        format_to_unpack = "2H3I"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Datagram length in bytes. The length field at the start (4 bytes)
        # and end of the datagram (4 bytes) are included in the length count.
        dg['numBytesDgm'] = fields[0]
        # Datagram version.
        dg['dgmVersion'] = fields[1]
        # UTC time from inside KM sensor data. Unit second. Epoch 1970-01-01 time.
        # Nanosec part to be added for more exact time.
        dg['time_sec'] = fields[2]
        # Nano seconds remainder. Nanosec part to be added to time_sec for more exact time.
        # If time is unavailable from attitude sensor input, time of reception on serial port is added to this field.
        dg['time_nanosec'] = fields[3]
        dg['datetime'] = datetime.datetime.utcfromtimestamp(dg['time_sec'] + dg['time_nanosec'] / 1.0E9)
        # Bit pattern for indicating validity of sensor data, and reduced performance.
        # The status word consists of 32 single bit flags numbered from 0 to 31, where 0 is the least significant bit.
        # Bit number 0-7 indicate if from a sensor data is invalid: 0 = valid data, 1 = invalid data.
        # Bit number 16-> indicate if data from sensor has reduced performance: 0 = valid data, 1 = reduced performance.
        '''
                Invalid data:                               |       Reduced performance: 
                Bit:    Sensor data:                        |       Bit:    Sensor data: 
                0       Horizontal position and velocity    |       16      Horizontal position and velocity
                1       Roll and pitch                      |       17      Roll and pitch 
                2       Heading                             |       18      Heading
                3       Heave and vertical velocity         |       19      Heave and vertical velocity
                4       Acceleration                        |       20      Acceleration
                5       Error fields                        |       21      Error fields
                6       Delayed heave                       |       22      Delayed heave
        '''
        dg['status'] = fields[4]

        format_to_unpack = "2d"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # # # # # P O S I T I O N # # # # #
        # Position in decimal degrees.
        dg['latitude_deg'] = fields[0]
        # Position in decimal degrees.
        dg['longitude_deg'] = fields[1]

        format_to_unpack = "21f"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        dg['ellipsoidHeight_m'] = fields[0]

        # # # # # A T T I T U D E # # # # #
        dg['roll_deg'] = fields[1]
        dg['pitch_deg'] = fields[2]
        dg['heading_deg'] = fields[3]
        dg['heave_m'] = fields[4]

        # # # # # R A T E S # # # # #
        dg['rollRate'] = fields[5]
        dg['pitchRate'] = fields[6]
        dg['yawRate'] = fields[7]

        # # # # # V E L O C I T I E S # # # # #
        dg['velNorth'] = fields[8]
        dg['velEast'] = fields[9]
        dg['velDown'] = fields[10]

        # # # # # ERRORS IN DATA. SENSOR DATA QUALITY, AS STANDARD DEVIATIONS # # # # #
        dg['latitudeError_m'] = fields[11]
        dg['longitudeError_m'] = fields[12]
        dg['ellipsoidalHeightError_m'] = fields[13]
        dg['rollError_deg'] = fields[14]
        dg['pitchError_deg'] = fields[15]
        dg['headingError_deg'] = fields[16]
        dg['heaveError_m'] = fields[17]

        # # # # # A C C E L E R A T I O N # # # # #
        dg['northAcceleration'] = fields[18]
        dg['eastAcceleration'] = fields[19]
        dg['downAcceleration'] = fields[20]

        # In testing, it appears 'numBytesDgm' = KMbinary + KMdelayedHeave.
        # We will run into errors here if we use this method to skip unknown fields.
        # Skip unknown fields
        #self.FID.seek(dg['numBytesDgm'] - struct.Struct(format_to_unpack).size, 1)

        if self.verbose > 2:
            self.print_datagram(dg)

        return dg

    def read_EMdgmSKMsample(self, dgInfo):
        """
        Read #SKM - all available data. An implementation of the KM Binary sensor input format.
        :param dgInfo: A dictionary containing EMdgmSKMinfo (output of function read_EMdgmSKMinfo).
        :return: A dictionary of lists, containing EMdgmSKMsample ('sample').
        This includes keys 'KMdefault' and 'delayedHeave'.
        """
        # LMD tested.
        # TODO: Can add code to omit delayed heave if it is not included.

        dg = {}

        km_binary_data = []
        km_heave_data = []

        for idx in range(dgInfo['numSamplesArray']):
            km_binary_data.append(self.read_KMbinary())
            km_heave_data.append(self.read_KMdelayedHeave())

        # Convert list of dictionaries to dictionary of lists.
        dg['KMdefault'] = self.listofdicts2dictoflists(km_binary_data)
        dg['delayedHeave'] = self.listofdicts2dictoflists(km_heave_data)

        return dg

    def read_EMdgmSKM(self):
        """
        Read #SKM - data from attitude and attitude velocity sensors. Datagram may contain several sensor measurements.
        The number of samples in datagram is listed in numSamplesArray in the struct EMdgmSKMinfo_def. Time given in
        datagram header, is time of arrival of data on serial line or on network. Time inside #KMB sample is time from
        the sensors data. If input is other than KM binary sensor input format, the data are converted to the KM binary
        format by the PU. All parameters are uncorrected. For processing of data, installation offsets, installation
        angles and attitude values are needed to correct the data for motion.
        :return: A dictionary containing EMdgmSKM.
        """
        # LMD tested.

        start = self.FID.tell()

        # LMD implementation:
        dg = {}

        dg['header'] = self.read_EMdgmHeader()
        dg['infoPart'] = self.read_EMdgmSKMinfo()
        dg['sample'] = self.read_EMdgmSKMsample(dg['infoPart'])

        # VES implementation:
        '''
        dgH = self.read_EMdgmHeader()
        dgInfo = self.read_EMdgmSKMinfo()
        dgSamples = self.read_EMdgmSKMsample(dgInfo)

        dg = {**dgH, **dgInfo, **dgSamples}
        '''

        # Seek to end of the packet.
        self.FID.seek(start + dg['header']['numBytesDgm'], 0)

        return dg

    def read_EMdgmSVPpoint(self):
        """
        Read #SVP - Sound Velocity Profile. Data from one depth point contains information specified in this struct.
        :return: A dictionary containing EMdgmSVPpoint.
        """
        # NOTE: There's no fields for the number of bytes in this record. Odd.
        # LMD added, tested.

        dg = {}
        format_to_unpack = "2f1I2f"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Depth at which measurement is taken. Unit m. Valid range from 0.00 m to 12000 m.
        dg['depth_m'] = fields[0]
        # Measured sound velocity from profile. Unit m/s. For a CTD profile, this will be the calculated sound velocity.
        dg['soundVelocity_mPerSec'] = fields[1]
        # Former absorption coefficient. Voided.
        dg['padding'] = fields[2]
        # Water temperature at given depth. Unit Celsius. For a Sound velocity profile (S00), this will be set to 0.00.
        dg['temp_C'] = fields[3]
        # Salinity of water at given depth. For a Sound velocity profile (S00), this will be set to 0.00.
        dg['salinity'] = fields[4]

        return dg

    def read_EMdgmSVP(self):
        """
        Read #SVP - Sound Velocity Profile. Data from sound velocity profile or from CTD profile.
        Sound velocity is measured directly or estimated, respectively.
        :return: A dictionary containing EMdgmSVP.
        """
        # LMD added, tested.

        start = self.FID.tell()

        dg = {}
        dg['header'] = self.read_EMdgmHeader()

        format_to_unpack = "2H4s1I"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Size in bytes of body part struct. Used for denoting size of rest of datagram.
        dg['numBytesCmnPart'] = fields[0]
        # Number of sound velocity samples.
        dg['numSamples'] = fields[1]
        # Sound velocity profile format:
        '''
            'S00' = sound velocity profile
            'S01' = CTD profile
        '''
        dg['sensorFormat'] = fields[2]
        # Time extracted from the Sound Velocity Profile. Parameter is set to zero if not found.
        dg['time_sec'] = fields[3]
        dg['datetime'] = datetime.datetime.utcfromtimestamp(dg['time_sec'])

        format_to_unpack = "2d"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Latitude in degrees. Negative if southern hemisphere. Position extracted from the Sound Velocity Profile.
        # Parameter is set to define UNAVAILABLE_LATITUDE if not available.
        dg['latitude_deg'] = fields[0]
        # Longitude in degrees. Negative if western hemisphere. Position extracted from the Sound Velocity Profile.
        # Parameter is set to define UNAVAILABLE_LONGITUDE if not available.
        dg['longitude_deg'] = fields[1]

        # SVP point samples, repeated numSamples times.
        sensorData = []
        for record in range(dg['numSamples']):
            sensorData.append(self.read_EMdgmSVPpoint())
        dg['sensorData'] = self.listofdicts2dictoflists(sensorData)

        # Seek to end of the packet.
        self.FID.seek(start + dg['header']['numBytesDgm'], 0)

        return dg

    def read_EMdgmSVTinfo(self):
        """
        Read part of Sound Velocity at Transducer datagram.
        :return: A dictionary containing EMdgmSVTinfo.
        """
        # LMD added, tested.

        dg = {}
        format_to_unpack = "6H2f"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Size in bytes of current struct. Used for denoting size of rest of datagram in cases where only one
        # datablock is attached.
        dg['numBytesInfoPart'] = fields[0]
        # Sensor status. To indicate quality of sensor data is valid or invalid. Quality may be invalid even if sensor
        # is active and the PU receives data. Bit code vary according to type of sensor.
        # Bits 0-7 common to all sensors and #MRZ sensor status:
        '''
                Bit:   Sensor data: 
                0      0 Data OK; 1 Data OK and sensor chosen is active
                1      0
                2      0 Data OK; 1 Reduced Performance
                3      0
                4      0 Data OK; 1 Invalid Data
                5      0
                6      0 
        '''
        dg['sensorStatus'] = fields[1]
        # Format of raw data from input sensor, given in numerical code according to table below.
        '''
                Code:   Sensor format: 
                1       AML NMEA
                2       AML SV
                3       AML SVT
                4       AML SVP
                5       Micro SV
                6       Micro SVT
                7       Micro SVP
                8       Valeport MiniSVS
                9       KSSIS 80
                10      KSSIS 43
        '''
        dg['sensorInputFormat'] = fields[2]
        # Number of sensor samples added in this datagram.
        dg['numSamplesArray'] = fields[3]
        # Length in bytes of one whole SVT sensor sample.
        dg['numBytesPerSample'] = fields[4]
        # Field to indicate which information is available from the input sensor, at the given sensor format.
        # 0 = not available; 1 = data is available
        # Expected data field in sensor input:
        '''
                Bit:    Sensor data: 
                0       Sound Velocity
                1       Temperature
                2       Pressure
                3       Salinity
        '''
        dg['sensorDataContents'] = fields[5]
        # Time parameter for moving median filter. Unit seconds.
        dg['filterTime_sec'] = fields[6]
        # Offset for measured sound velocity set in K-Controller. Unit m/s.
        dg['soundVelocity_mPerSec_offset'] = fields[7]

        # Skip unknown fields.
        self.FID.seek(dg['numBytesInfoPart'] - struct.Struct(format_to_unpack).size, 1)

        return dg

    def read_EMdgmSVTsample(self):
        """
        Read #SVT - Sound Velocity at Transducer. Data sample.
        :return: A dictionary containing EMdgmSVTsample.
        """
        # NOTE: There's no fields for the number of bytes in this record. Odd.
        # LMD added, tested.

        dg = {}
        format_to_unpack = "2I4f"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Time in second. Epoch 1970-01-01. time_nanosec part to be added for more exact time.
        dg['time_sec'] = fields[0]
        # Nano seconds remainder. time_nanosec part to be added to time_sec for more exact time.
        dg['time_nanosec'] = fields[1]
        dg['datetime'] = datetime.datetime.utcfromtimestamp(dg['time_sec'] + dg['time_nanosec'] / 1.0E9)
        # Measured sound velocity from sound velocity probe. Unit m/s.
        dg['soundVelocity_mPerSec'] = fields[2]
        # Water temperature from sound velocity probe. Unit Celsius.
        dg['temp_C'] = fields[3]
        # Pressure. Unit Pascal.
        dg['pressure_Pa'] = fields[4]
        # Salinity of water. Measured in g salt/kg sea water.
        dg['salinity'] = fields[5]

        return dg

    def read_EMdgmSVT(self):
        """
        Read #SVT - Sound Velocity at Transducer. Data for sound velocity and temperature are measured directly
        on the sound velocity probe.
        :return: A dictionary containing EMdgmSVT.
        """
        # LMD added, tested.

        start = self.FID.tell()

        dg = {}
        dg['header'] = self.read_EMdgmHeader()
        dg['infoPart'] = self.read_EMdgmSVTinfo()

        sensorData = []
        for record in range(dg['infoPart']['numSamplesArray']):
            sensorData.append(self.read_EMdgmSVTsample())
        dg['sensorData'] = self.listofdicts2dictoflists(sensorData)

        # Seek to end of the packet.
        self.FID.seek(start + dg['header']['numBytesDgm'], 0)

        return dg

    def read_EMdgmSCLdataFromSensor(self):
        """
        Read part of clock datagram giving offsets and the raw input in text format.
        :return: A dictionary containing EMdgmSCLdataFromSensor.
        """
        # NOTE: There's no fields for the number of bytes in this record. Odd.
        # LMD tested.

        dg = {}
        format_to_unpack = "1f1i64s"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        # Offset in seconds from K-Controller operator input.
        dg['offset_sec'] = fields[0]
        # Clock deviation from PU. Difference between time stamp at receive of sensor data and time in the clock
        # source. Unit nanoseconds. Difference smaller than +/- 1 second if 1PPS is active and sync from ZDA.
        dg['clockDevPU_nanosec'] = fields[1]

        # TODO: This is an array of (max?) length MAX_SCL_DATALENGTH; do something else here?
        # TODO: Get MAX_SCL_DATALENGTH from datagram instead of hard-coding in format_to_unpack.
        # TODO: This works for now, but maybe there is a smarter way?
        # Position data as received from sensor, i.e. uncorrected for motion etc.
        tmp = fields[2]
        dg['dataFromSensor'] = tmp[0:tmp.find(b'\x00\x00L')]

        return dg

    def read_EMdgmSCL(self):
        """
        Read #SCL - Clock datagram.
        :return: A dictionary containing EMdgmSCL.
        """
        # LMD tested.

        start = self.FID.tell()

        dg = {}
        dg['header'] = self.read_EMdgmHeader()
        dg['cmnPart'] = self.read_EMdgmScommon()
        dg['sensData'] = self.read_EMdgmSCLdataFromSensor()

        # Seek to end of the packet.
        self.FID.seek(start + dg['header']['numBytesDgm'], 0)

        return dg

    def read_EMdgmSDEdataFromSensor(self):
        """
        # WARNING: INCOMPLETE
        Read part of depth datagram giving depth as used, offsets,
        scale factor and data as received from sensor (uncorrected).
        :return: A dictionary containing EMdgmSDEdataFromSensor
        """
        # NOTE: There's no fields for the number of bytes in this record. Odd.
        # LMD added, untested.
        # TODO: Test with depth data to complete this function!
        print("WARNING: You are using an incomplete, untested function: read_EMdgmSDEdataFromSensor.")

        dg = {}
        format_to_unpack = "3f2d32s"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        dg['depthUsed_m'] = fields[0]
        dg['offset'] = fields[1]
        dg['scale'] = fields[2]
        dg['latitude_deg'] = fields[3]
        dg['longitude_deg'] = fields[4]

        # TODO: This is an array of (max?) length MAX_SDE_DATALENGTH; do something else here?
        # TODO: Get MAX_SDE_DATALENGTH from datagram instead of hard-coding in format_to_unpack.
        # TODO: Test with depth data to complete this function!
        tmp = fields[5]
        #dg['dataFromSensor'] = ...

        return dg

    def read_EMdgmSDE(self):
        """
        Read #SDE - Depth datagram.
        :return: A dictionary containing EMdgmSDE.
        """
        # LMD added, untested.
        # TODO: Test with depth data!
        print("WARNING: You are using an incomplete, untested function: read_EMdgmSDE.")

        start = self.FID.tell()

        dg = {}
        dg['header'] = self.read_EMdgmHeader()
        dg['cmnPart'] = self.read_EMdgmScommon()
        dg['sensorData'] = self.read_EMdgmSDEdataFromSensor()

        # Seek to end of the packet.
        self.FID.seek(start + dg['header']['numBytesDgm'], 0)

        return dg

    def read_EMdgmSHIdataFromSensor(self):
        """
        # WARNING: INCOMPLETE
        Read part of Height datagram, giving corrected and uncorrected data as received from sensor.
        :return: A dictionary containing EMdgmSHIdataFromSensor.
        """
        # NOTE: There's no fields for the number of bytes in this record. Odd.
        # LMD added, untested.
        # TODO: Test with height data to complete this function!
        print("WARNING: You are using an incomplete, untested function: read_EMdgmSHIdataFromSensor.")

        dg = {}
        format_to_unpack = "1H1f32s"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        dg['sensorType'] = fields[0]
        dg['heightUsed_m'] = fields[1]

        # TODO: This is an array of (max?) length MAX_SHI_DATALENGTH; do something else here?
        # TODO: Get MAX_SHI_DATALENGTH from datagram instead of hard-coding in format_to_unpack.
        # TODO: Test with height data to complete this function!
        tmp = fields[2]
        #dg['dataFromSensor'] = ...

        print("DG: ", dg)
        return dg

    def read_EMdgmSHI(self):
        """
        Read #SHI - Height datagram.
        :return: A dictionary containing EMdgmSHI.
        """
        # LMD added, untested.
        # TODO: Test with height data!
        print("WARNING: You are using an incomplete, untested function: read_EMdgmSHI.")

        start = self.FID.tell()

        dg = {}
        dg['header'] = self.read_EMdgmHeader()
        dg['cmnPart'] = self.read_EMdgmScommon()
        dg['sensData'] = self.read_EMdgmSHIdataFromSensor()

        # Seek to end of the packet.
        self.FID.seek(start + dg['header']['numBytesDgm'], 0)

        return dg

    def read_EMdgmCPOdataBlock(self):
        """
        Read #CPO - Compatibility sensor position compatibility data block. Data from active sensor is referenced to
        position at antenna footprint at water level. Data is corrected for motion ( roll and pitch only) if enabled
        by K-Controller operator. Data given both decoded and corrected (active sensors), and raw as received from
        sensor in text string.
        :return: A dictionary containing EMdgmCPOdataBlock.
        """
        # NOTE: There's no fields for the number of bytes in this record. Odd.
        # LMD tested.

        dg = {}
        format_to_unpack = "2I1f"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        dg['timeFromSensor_sec'] = fields[0]
        dg['timeFromSensor_nanosec'] = fields[1]
        dg['datetime'] = datetime.datetime.utcfromtimestamp(dg['timeFromSensor_sec']
                                                            + dg['timeFromSensor_nanosec'] / 1.0E9)
        dg['posFixQuality'] = fields[2]

        # For some reason, it doesn't work to do this all in one step, but it works broken up into two steps. *shrug*
        format_to_unpack = "2d3f250s"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))
        dg['correctedLat_deg'] = fields[0]
        dg['correctedLong_deg'] = fields[1]
        dg['speedOverGround_mPerSec'] = fields[2]
        dg['courseOverGround_deg'] = fields[3]
        dg['ellipsoidHeightReRefPoint_m'] = fields[4]

        # TODO: This is an array of(max?) length MAX_CPO_DATALENGTH; do something else here?
        # TODO: Get MAX_CPO_DATALENGTH from datagram instead of hard-coding in format_to_unpack.
        # TODO: This works for now, but maybe there is a smarter way?
        tmp = fields[5]
        dg['posDataFromSensor'] = tmp[0:tmp.find(b'\r\n')]

        return dg

    def read_EMdgmCPO(self):
        """
        Read #CPO - Struct of compatibility position sensor datagram. Data from active sensor will be motion corrected
        if indicated by operator. Motion correction is applied to latitude, longitude, speed, course and ellipsoidal
        height. If the sensor is inactive, the fields will be marked as unavailable, defined by the parameters
        define UNAVAILABLE_LATITUDE etc.
        :return: A dictionary containing EMdgmCPO.
        """
        # LMD tested.

        start = self.FID.tell()

        dg = {}
        dg['header'] = self.read_EMdgmHeader()
        dg['cmnPart'] = self.read_EMdgmScommon()
        dg['sensorData'] = self.read_EMdgmCPOdataBlock()

        # Seek to end of the packet.
        self.FID.seek(start + dg['header']['numBytesDgm'], 0)

        return dg

    def read_EMdgmCHEdata(self):
        """
        Read #CHE - Heave compatibility data part. Heave reference point is at transducer instead of at vessel
        reference point.
        :return: A dictionary containing EMdgmCHEdata.
        """
        # NOTE: There's no fields for the number of bytes in this record. Odd.
        # LMD added, tested.

        dg = {}
        format_to_unpack = "1f"
        fields = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))

        dg['heave_m'] = fields[0]

        return dg

    def read_EMdgmCHE(self):
        """
        Read #CHE - Struct of compatibility heave sensor datagram. Used for backward compatibility with .all datagram
        format. Sent before #MWC (water column datagram) datagram if compatibility mode is enabled. The multibeam
        datagram body is common with the #MWC datagram.
        :return: A dictionary containing EMdgmCHE.
        """
        # LMD added, tested.

        start = self.FID.tell()

        dg = {}
        dg['header'] = self.read_EMdgmHeader()
        dg['cmnPart'] = self.read_EMdgmMbody()
        dg['data'] = self.read_EMdgmCHEdata()

        # Seek to end of the packet.
        self.FID.seek(start + dg['header']['numBytesDgm'], 0)

        return dg

    ###########################################################
    # Utilities
    ###########################################################

    def OpenFiletoRead(self, inputfilename=None):
        """ Open a KMALL data file for reading."""
        if self.filename is None:
            if inputfilename is None:
                print("No file name specified")
                sys.exit(1)
            else:
                filetoopen = inputfilename
        else:
            filetoopen = self.filename

        if self.verbose >= 1:
            print("Opening: %s" % filetoopen)

        self.FID = open(filetoopen, "rb")

    def closeFile(self):
        """ Close a file."""
        if self.FID is not None:
            self.FID.close()

    def print_datagram(self, dg):
        """ A utility function to print the fields of a parsed datagram. """
        print("\n")
        for k, v in dg.items():
            print("%s:\t\t\t%s" % (k, str(v)))

    def index_file(self):
        """ Index a KMALL file - message type, time, size, byte offset. """

        if self.FID is None:
            self.OpenFiletoRead()

        # Get size of the file.
        self.FID.seek(0, 2)
        self.file_size = self.FID.tell()
        self.FID.seek(0, 0)

        if (self.verbose == 1):
            print("Filesize: %d" % self.file_size)

        self.msgoffset = []
        self.msgsize = []
        self.msgtime = []
        self.msgtype = []
        self.pktcnt = 0

        while self.FID.tell() < self.file_size:

            try:
                # Get the byte offset.
                self.msgoffset.append(self.FID.tell())

                # Read the first four bytes to get the datagram size.
                msgsize = struct.unpack("I", self.FID.read(4))
                self.msgsize.append(msgsize[0])

                # Read the datagram.
                msg_buffer = self.FID.read(int(self.msgsize[self.pktcnt]) - 4)
            except:
                print("Error indexing file: %s" % self.filename)
                self.msgoffset = self.msgoffset[:-1]
                self.msgsize = self.msgsize[:-1]
                continue

            # Interpret the header.
            header_without_length = struct.Struct('ccccBBHII')

            (dgm_type0, dgm_type1, dgm_type2, dgm_type3, dgm_version,
             sysid, emid,
             sec,
             nsec) = header_without_length.unpack_from(msg_buffer, 0)

            dgm_type = dgm_type0 + dgm_type1 + dgm_type2 + dgm_type3

            self.msgtype.append(str(dgm_type))
            # Decode time
            # osec = sec
            # osec *= 1E9
            # osec += nsec
            # lisec = nanosec
            # lisec /= 1E6

            # Captue the datagram header timestamp.
            self.msgtime.append(sec + nsec / 1.0E9)

            if self.verbose:
                print("MSG_TYPE: %s,\tOFFSET:%0.0f,\tSIZE: %0.0f,\tTIME: %0.3f" %
                      (dgm_type,
                       self.msgoffset[self.pktcnt],
                       self.msgsize[self.pktcnt],
                       self.msgtime[self.pktcnt]))

            self.pktcnt += 1

        self.msgoffset = np.array(self.msgoffset)
        self.msgsize = np.array(self.msgsize)
        self.msgtime = np.array(self.msgtime)

        self.Index = pd.DataFrame({'Time': self.msgtime,
                                   'ByteOffset': self.msgoffset,
                                   'MessageSize': self.msgsize,
                                   'MessageType': self.msgtype})
        self.Index.set_index('Time', inplace=True)
        self.Index['MessageType'] = self.Index.MessageType.astype('category')
        if self.verbose >= 2:
            print(self.Index)

    def extract_nav(self):
        pass

    def extract_attitude(self):
        ''' Extract all raw attitude data from data file into self.att'''

        if self.Index is None:
            self.index_file()

        if self.FID is None:
            self.OpenFiletoRead()

        # Get offsets for 'SKM' attitude datagrams.
        SKMOffsets = [x for x, y in zip(self.msgoffset, self.msgtype) if y == "b'#SKM'"]

        dg = list()
        for offset in SKMOffsets:
            self.FID.seek(offset, 0)
            dg.append(self.read_EMdgmSKM())

        # Convert list of dictionaries to dictionary of lists.
        self.att = self.listofdicts2dictoflists(dg)
        # for k,v in dg[0].items():
        #    self.att[k] = [x for sublist in dg for x in sublist[k]]

        self.FID.seek(0, 0)
        return

    def listofdicts2dictoflists(self, listofdicts):
        """ A utility function to convert a list of dicts to a dict of lists."""
        dg = {}

        # This is done in two steps, handling both dictionary items that are
        # lists and scalars separately. As long as no item combines both lists
        # and scalars the method works.
        #
        # There is some mechanism to handle this in a single list
        # comprehension statement, checking for types on the fly, but I cannot
        # find any syntax that returns the proper result.
        if len(listofdicts) == 0:
            return None

        for k, v in listofdicts[0].items():
            dg[k] = [item for dictitem in listofdicts if isinstance(dictitem[k], list) for item in dictitem[k]]
            scalartmp = [dictitem[k] for dictitem in listofdicts if not isinstance(dictitem[k], list)]
            if len(dg[k]) == 0:
                dg[k] = scalartmp

        return dg

    def extract_xyz(self):
        pass

    def check_ping_count(self):
        """ A method to check to see that all required MRZ datagrams exist """

        if self.Index is None:
            self.index_file()

        if self.FID is None:
            self.OpenFiletoRead()

        # M = map( lambda x: x=="b'#MRZ'", self.msgtype)
        # MRZOffsets = self.msgoffset[list(M)]

        # Get the file byte count offset for each MRZ datagram.
        MRZOffsets = [x for x, y in zip(self.msgoffset, self.msgtype) if y == "b'#MRZ'"]
        self.pingcnt = []
        self.rxFans = []
        self.rxFanIndex = []

        # Skip through the file capturing the ping count information:
        #  The ping count values
        #  The number of receive fans specified for each ping
        #  The receive fan index for each received MRZ record.
        #
        # Notes:    A ping can span more than 1 MRZ datagrams. This happens when
        #           1 MRZ datagram exists for each receive "fan"
        #           In dual swath mode, at least two receive fans are generated.
        #           The ping counter will not change for the second MRZ packet.

        for offset in MRZOffsets:
            self.FID.seek(offset, 0)
            dg = self.read_EMdgmHeader()
            dg = self.read_EMdgmMpartition()
            dg = self.read_EMdgmMbody()
            self.pingcnt.append(dg['pingCnt'])
            self.rxFans.append(dg['rxFansPerPing'])
            self.rxFanIndex.append(dg['rxFanIndex'])

        self.pingcnt = np.array(self.pingcnt)
        self.rxFans = np.array(self.rxFans)
        self.rxFanIndex = np.array(self.rxFanIndex)

        # Things to check:
        # Is the total sum of rxFans equal to the number of MRZ packets?
        # Are the unique ping counter values sequential?
        # The number of multiple ping counter values has to be larger than the
        # number of rx fans and packets.

        # Sorting by ping count and then calculating the difference in
        # successive values allows one to check to see that at least one
        # packet exists for each ping (which may have more than one).
        if len(self.pingcnt) > 0:
            PingCounterRange = max(self.pingcnt) - min(self.pingcnt)
            dpu = np.diff(np.sort(np.unique(self.pingcnt)))
            NpingsMissed = sum((dpu[dpu > 1] - 1))
            NpingsSeen = len(np.unique(self.pingcnt))
            # MaxDiscontinuity = max(abs(dpu))

            if self.verbose > 1:
                print("File: %s\n\tPing Counter Range: %d:%d N=%d" %
                      (self.filename, min(self.pingcnt), max(self.pingcnt), PingCounterRange))
                print("\tNumbr of pings missing: %d of %d" % (NpingsMissed, NpingsMissed + NpingsSeen))

        else:
            PingCounterRange = 0
            NpingsSeen = 0
            NpingsMissed = 0
            if self.verbose > 1:
                print("No pings in file.")

        # print("\tNumbr of pings seen: %d" % NpingsSeen)
        # print('File: %s\n\tNumber of missed full pings: %d of %d' %
        #      (self.filename, PingCounterRange - NpingsSeen, PingCounterRange ))

        # dp = np.diff(self.pingcnt)
        # FirstPingInSeries = np.array([x==0 for x in dp])
        HaveAllMRZ = True
        MissingMRZCount = 0
        # Go through every "ping" these may span multiple packets...
        for idx in range(len(self.pingcnt)):
            # Side note: This method is going to produce a warning multiple
            # times for each ping series that fails the test. Sloppy.

            # Capture how many rx fans there should be for this ping.
            N_RxFansforSeries = self.rxFans[idx]
            # Get the rxFan indices associated with this ping record.
            PingsInThisSeriesMask = np.array([x == self.pingcnt[idx] for x in self.pingcnt])
            rxFanIndicesforThisSeries = self.rxFanIndex[PingsInThisSeriesMask]

            # Check to see that number of records equals the total.
            if len(rxFanIndicesforThisSeries) != N_RxFansforSeries:
                if HaveAllMRZ:
                    if self.verbose > 1:
                        print("\tDetected missing MRZ records!")

                if self.verbose > 1:
                    print('\tNot enough rxFan (MRZ) records for ping: %d: Indices %s of [0:%d] found' %
                          (self.pingcnt[idx],
                           ",".join(str(x) for x in rxFanIndicesforThisSeries),
                           N_RxFansforSeries - 1))
                HaveAllMRZ = False
                MissingMRZCount = MissingMRZCount + 1

        # Shamelessly creating a data frame just to get a pretty table.
        res = pd.DataFrame([["File", "NpingsTotal", "Pings Missed", "MissingMRZRecords"],
                            [self.filename, NpingsMissed + NpingsSeen, NpingsMissed, MissingMRZCount]])
        print(res.to_string(index=False, header=False))

        if HaveAllMRZ:
            if self.verbose > 1:
                print("\tNumber of MRZ records equals number required for each ping.")

        return (self.filename, NpingsMissed + NpingsSeen, NpingsMissed, MissingMRZCount)

    def report_packet_types(self):
        """ A method to report datagram packet count and size in a file. """

        if self.Index is None:
            self.index_file()

        # Get a list of packet types seen.
        types = list(set(self.msgtype))

        pktcount = {}
        pktSize = {}
        pktMinSize = {}
        pktMaxSize = {}
        # Calculate some stats.
        for type in types:
            M = np.array(list(map(lambda x: x == type, self.msgtype)))
            pktcount[type] = sum(M)
            pktSize[type] = sum(self.msgsize[M])
            pktMinSize[type] = min(self.msgsize[M])
            pktMaxSize[type] = max(self.msgsize[M])

        # print(self.Index.groupby("MessageType").describe().reset_index())
        msg_type_group = self.Index.groupby("MessageType")
        summary = {"Count": msg_type_group["MessageType"].count(),
                   "Size:": msg_type_group["MessageSize"].sum(),
                   "Min Size": msg_type_group["MessageSize"].min(),
                   "Max Size": msg_type_group["MessageSize"].max()}
        IndexSummary = pd.DataFrame(summary)

        print(IndexSummary)


if __name__ == '__main__':
    # Handle input arguments
    parser = argparse.ArgumentParser(description="A python script (and class)"
                                                 "for parsing Kongsberg KMALL data files.")
    parser.add_argument('-f', action='store', dest='kmall_filename',
                        help="The path and filename to parse.")
    parser.add_argument('-d', action='store', dest='kmall_directory',
                        help="A directory containing kmall data files to parse.")
    parser.add_argument('-V', action='store_true', dest='verify',
                        default=False, help="Perform series of checks to verify the kmall file.")
    parser.add_argument('-v', action='count', dest='verbose', default=0,
                        help="Increasingly verbose output (e.g. -v -vv -vvv),"
                             "for debugging use -vvv")
    args = parser.parse_args()

    verbose = args.verbose

    kmall_filename = args.kmall_filename
    kmall_directory = args.kmall_directory
    verify = args.verify

    if kmall_directory:
        filestoprocess = []
        suffix = "kmall"
        if verbose >= 3:
            print("directory: " + directory)

        # Recursively work through the directory looking for kmall files.
        for root, subFolders, files in os.walk(kmall_directory):
            for fileval in files:
                if fileval[-suffix.__len__():] == suffix:
                    filestoprocess.append(os.path.join(root, fileval))
    else:
        filestoprocess = [kmall_filename]

    if filestoprocess.__len__() == 0:
        print("No files found to process.")
        sys.exit()

    for filename in filestoprocess:
        print("")
        print("Processing: %s" % filename)

        # Create the class instance.
        K = kmall(filename)
        K.verbose = args.verbose
        if (K.verbose >= 1):
            print("Processing file: %s" % K.filename)

        pingcheckdata = []
        navcheckdata = []
        # Index file (check for index)
        K.index_file()
        if verify:
            K.report_packet_types()
            pingcheckdata.append([x for x in K.check_ping_count()])

            K.extract_attitude()
            # Report gaps in attitude data.
            dt_att = np.diff([x.timestamp() for x in K.att["datetime"]])
            navcheckdata.append([np.min(np.abs(dt_att)),
                                 np.max(dt_att),
                                 np.mean(dt_att),
                                 1.0 / np.mean(dt_att),
                                 sum(dt_att >= 1.0)])
            # print("Navigation Gaps min: %0.3f, max: %0.3f, mean: %0.3f (%0.3fHz)" %
            #      (np.min(np.abs(dt_att)),np.max(dt_att),np.mean(dt_att),1.0/np.mean(dt_att)))
            # print("Navigation Gaps >= 1s: %d" % sum(dt_att >= 1.0))
    print("Packet statistics:")

    # Print column headers
    # print('%s' % "\t".join(['File','Npings','NpingsMissing','NMissingMRZ'] +
    #                         ['Nav Min Time Gap','Nav Max Time Gap', 'Nav Mean Time Gap','Nav Mean Freq','Nav N Gaps >1s']))

    # Print columns
    # for x,y in zip(pingcheckdata,navcheckdata):
    #    row = x+y
    #    #print(row)
    #    print("\t".join([str(x) for x in row]))

    # Create DataFrame to make printing easier.
    DataCheck = pd.DataFrame([x + y for x, y in zip(pingcheckdata, navcheckdata)], columns=
    ['File', 'Npings', 'NpingsMissing', 'NMissingMRZ'] +
    ['NavMinTimeGap', 'NavMaxTimeGap', 'NavMeanTimeGap', 'NavMeanFreq', 'NavNGaps>1s'])
    # K.navDataCheck = pd.DataFrame(navcheckdata,columns=['Min Time Gap','Max Time Gap', 'Mean Time Gap','Mean Freq','N Gaps >1s'])
    pd.set_option('display.max_columns', 30)
    pd.set_option('display.expand_frame_repr', False)
    print(DataCheck)
    # print(DataCheck)
