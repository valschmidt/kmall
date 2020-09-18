#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
import re
import bz2
import copy
from pyproj import Proj
from scipy import stats

recs_categories = {'SKM': ['sample.KMdefault.dgtime', 'sample.KMdefault.roll_deg', 'sample.KMdefault.pitch_deg',
                           'sample.KMdefault.heave_m', 'sample.KMdefault.heading_deg',
                           'sample.KMdefault.latitude_deg', 'sample.KMdefault.longitude_deg',
                           'sample.KMdefault.ellipsoidHeight_m'],
                   'IIP': ['header.dgtime', 'install_txt'],
                   'MRZ': ['header.dgtime', 'cmnPart.pingCnt', 'cmnPart.rxTransducerInd',
                           'pingInfo.soundSpeedAtTxDepth_mPerSec', 'pingInfo.numTxSectors', 'header.systemID',
                           'txSectorInfo.txSectorNumb', 'txSectorInfo.tiltAngleReTx_deg',
                           'txSectorInfo.sectorTransmitDelay_sec', 'txSectorInfo.centreFreq_Hz',
                           'sounding.beamAngleReRx_deg', 'sounding.txSectorNumb', 'sounding.detectionType',
                           'sounding.qualityFactor', 'sounding.twoWayTravelTime_sec',
                           'pingInfo.modeAndStabilisation', 'pingInfo.pulseForm', 'pingInfo.depthMode'],
                   'IOP': ['header.dgtime', 'runtime_txt'],
                   'SVP': ['time_sec', 'sensorData.depth_m', 'sensorData.soundVelocity_mPerSec']}

recs_categories_translator = {'SKM': {'sample.KMdefault.dgtime': [['attitude', 'time'], ['navigation', 'time']],
                                      'sample.KMdefault.roll_deg': [['attitude', 'roll']],
                                      'sample.KMdefault.pitch_deg': [['attitude', 'pitch']],
                                      'sample.KMdefault.heave_m': [['attitude', 'heave']],
                                      'sample.KMdefault.heading_deg': [['attitude', 'heading']],
                                      'sample.KMdefault.latitude_deg': [['navigation', 'latitude']],
                                      'sample.KMdefault.longitude_deg': [['navigation', 'longitude']],
                                      'sample.KMdefault.ellipsoidHeight_m': [['navigation', 'altitude']]},
                              'MRZ': {'header.dgtime': [['ping', 'time']], 'cmnPart.pingCnt': [['ping', 'counter']],
                                      'cmnPart.rxTransducerInd': [['ping', 'rxid']],
                                      'pingInfo.soundSpeedAtTxDepth_mPerSec': [['ping', 'soundspeed']],
                                      'pingInfo.numTxSectors': [['ping', 'ntx']],
                                      'header.systemID': [['ping', 'serial_num']],
                                      'txSectorInfo.txSectorNumb': [['ping', 'txsectorid']],
                                      'txSectorInfo.tiltAngleReTx_deg': [['ping', 'tiltangle']],
                                      'txSectorInfo.sectorTransmitDelay_sec': [['ping', 'delay']],
                                      'txSectorInfo.centreFreq_Hz': [['ping', 'frequency']],
                                      'sounding.beamAngleReRx_deg': [['ping', 'beampointingangle']],
                                      'sounding.txSectorNumb': [['ping', 'txsector_beam']],
                                      'sounding.detectionType': [['ping', 'detectioninfo']],
                                      'sounding.qualityFactor': [['ping', 'qualityfactor_percent']],
                                      'sounding.twoWayTravelTime_sec': [['ping', 'traveltime']],
                                      'pingInfo.modeAndStabilisation': [['ping', 'yawpitchstab']],
                                      'pingInfo.pulseForm': [['ping', 'mode']],
                                      'pingInfo.depthMode': [['ping', 'modetwo']]},
                              'IIP': {'header.dgtime': [['installation_params', 'time']],
                                      'install_txt': [['installation_params', 'installation_settings']]},
                              'IOP': {'header.dgtime': [['runtime_params', 'time']],
                                      'runtime_txt': [['runtime_params', 'runtime_settings']]},
                              'SVP': {'time_sec': [['profile', 'time']], 'sensorData.depth_m': [['profile', 'depth']],
                                      'sensorData.soundVelocity_mPerSec': [['profile', 'soundspeed']]}}

recs_categories_result = {'attitude':  {'time': None, 'roll': None, 'pitch': None, 'heave': None, 'heading': None},
                          'installation_params': {'time': None, 'serial_one': None, 'serial_two': None,
                                                  'installation_settings': None},
                          'ping': {'time': None, 'counter': None, 'rxid': None, 'soundspeed': None, 'ntx': None,
                                   'serial_num': None, 'txsectorid': None, 'tiltangle': None, 'delay': None,
                                   'frequency': None, 'beampointingangle': None, 'txsector_beam': None,
                                   'detectioninfo': None, 'qualityfactor_percent': None, 'traveltime': None, 'mode': None,
                                   'modetwo': None, 'yawpitchstab': None},
                          'runtime_params': {'time': None, 'runtime_settings': None},
                          'profile': {'time': None, 'depth': None, 'soundspeed': None},
                          'navigation': {'time': None, 'latitude': None, 'longitude': None, 'altitude': None}}


class kmall():
    """ A class for reading a Kongsberg KMALL data file. """

    def __init__(self, filename=None):
        self.verbose = 0
        self.filename = filename
        self.FID = None
        self.file_size = None
        self.header_size = None
        self.Index = None

        self.pingDataCheck = None
        self.navDataCheck = None
        
        self.datagram_ident_search = self._build_startbytesearch()
        self.read_methods = [method_name for method_name in dir(self) if method_name[0:4] == 'read']
        
        self.datagram_ident = None
        self.datagram_data = None
        self.read_method = None
        self.eof = False

    def decode_datagram(self):
        """
        Assumes the file pointer is at the correct position to read the size of the dgram and the identifier
        
        Stores the datagram identifier and the read method as attributes.  read method is the name of the class
        method that we would use to read the datagram
        """
        self.datagram_ident = None
        self.read_method = None
        if self.FID is None:
            self.OpenFiletoRead()
        if self.file_size is None:  # need file size to determine end of file, init if not done already
            filelen = self._initialize_sequential_read(0, 0)

        num_bytes = self.FID.read(4)
        dgram = self.FID.read(4)
        if not self.FID.tell() == self.file_size:  # end of file
            self.FID.seek(-8, 1)
            is_valid_identifier = self.datagram_ident_search.search(dgram, 0)
            # dgram passes first check, starts with # and is 3 capital letters after
            if is_valid_identifier:
                # now compare dgram identifier with the last three letters of each read method to find the right one
                self.datagram_ident = dgram[-3:].decode()
                read_method = [rm for rm in self.read_methods if rm[-3:] == self.datagram_ident]
                if not len(read_method) > 1:
                    self.read_method = read_method[0]
                else:
                    raise ValueError('Found multiple valid read methods for {}: {}'.format(dgram, read_method))
            else:
                raise ValueError('Did not find valid datagram identifier: {}'.format(dgram))
        else:
            self.eof = True
    
    def read_datagram(self):
        """
        Reads the datagram data and stores the data in self.datagram_data
        Will always translate the installation parameters record (translate=True)
        
        To get the first record:
        
        km = kmall.kmall(r"C:\\Users\\zzzz\\Downloads\\0007_20190513_154724_ASVBEN.kmall")
        km.decode_datagram()
        km.read_datagram()
        
        Or to get the first MRZ record:
        
        km = kmall.kmall(r"C:\\Users\\zzzz\\Downloads\\0007_20190513_154724_ASVBEN.kmall")
        while not km.eof:
            km.decode_datagram()
            if km.datagram_ident != 'MRZ':
                km.skip_datagram()
            else:
                km.read_datagram()
                break
        
        """
        if self.read_method is not None:  # is None when decode fails or is at the end of file
            if self.read_method in ['read_EMdgmIIP', 'read_EMdgmIOP']:
                self.datagram_data = getattr(self, self.read_method)(translate=True)
            else:
                self.datagram_data = getattr(self, self.read_method)()

    def skip_datagram(self):
        """
        After decoding, use this to skip to the next datagram if you don't want to read this one
        """
        if self.read_method is not None:
            format_to_unpack = "1I"
            numbytes = struct.unpack(format_to_unpack, self.FID.read(struct.Struct(format_to_unpack).size))[0]
            self.FID.seek(numbytes - struct.Struct(format_to_unpack).size, 1)

    def read_first_datagram(self, datagram_identifier):
        """
        Uses read_datagram to quickly read the first instance of a datagram in a file

        datagram_identifier is a 3 letter string identifier, ex: 'IIP' or 'MRZ'
        """
        self.datagram_data = None
        self.eof = False
        
        if self.FID is None:
            self.OpenFiletoRead()
        else:
            self.FID.seek(0)

        while not self.eof:
            self.decode_datagram()
            if self.datagram_ident != datagram_identifier:
                self.skip_datagram()
            else:
                self.read_datagram()
                break
        if self.datagram_data is None:
            print('Unable to find {} in file'.format(datagram_identifier))
        return self.datagram_data

    def read_EMdgmHeader(self):
        """
        Read general datagram header.
        :return: A dictionary containing EMdgmHeader ('header').
        """
        # LMD tested.

        dg = {}
        format_to_unpack = "1I4s2B1H2I"
        self.header_size = struct.Struct(format_to_unpack).size
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

    def read_EMdgmIIP(self, translate=False):
        """
        Read #IIP - installation parameters and sensor format settings.

        If translate is True, the returned install_txt will be a dict with human readable key: value pairs.
        self.read_datagram will always use translate=True

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

        if translate:
            i_text = self.translate_installation_parameters_todict(i_text)
        dg['install_txt'] = i_text

        # remainder = total bytes - (header bytes + data bytes)
        expected_unknown_size = dg['header']['numBytesDgm'] - (self.header_size + dg['numBytesCmnPart'])

        # Skip unknown fields.
        self.FID.seek(expected_unknown_size, 1)

        return dg

    def read_EMdgmIOP(self, translate=False):
        """
        Read #IOP - runtime parameters, exactly as chosen by operator in K-Controller/SIS menus.

        If translate is True, the returned runtime_txt will be a dict with human readable key: value pairs.
        self.read_datagram will always use translate=True

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
        # print(rt_text)
        if translate:
            rt_text = self.translate_runtime_parameters_todict(rt_text)
        dg['runtime_txt'] = rt_text
        
        # remainder = total bytes - (header bytes + data bytes)
        expected_unknown_size = dg['header']['numBytesDgm'] - (self.header_size + dg['numBytesCmnPart'])

        # Skip unknown fields.
        self.FID.seek(expected_unknown_size, 1)

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
        # print(bist_text)
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
        dg['dgtime'] = dg['time_sec'] + dg['time_nanosec'] / 1.0E9
        dg['datetime'] = datetime.datetime.utcfromtimestamp(dg['dgtime'])
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
        # self.FID.seek(dg['numBytesDgm'] - struct.Struct(format_to_unpack).size, 1)

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
        # dg['dataFromSensor'] = ...

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
        # dg['dataFromSensor'] = ...

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
    # Writing datagrams
    ###########################################################
    def write_EMdgmMRZ(self, dg):
        ''' A method to write an MRZ datagram back to disk.'''

        # Force the header type to be MRZ, just in case
        # the datagram is converted from another type and
        # the old type is still set.
        dg['header']['dgmType'] = b'#MRZ'

        self.write_EMdgmHeader(dg['header'])
        self.write_EMdgmMpartition(dg['partition'])
        self.write_EMdgmMbody(dg['cmnPart'])
        self.write_EMdgmMRZ_pingInfo(dg['pingInfo'])

        for sector in range(dg['pingInfo']['numTxSectors']):
            self.write_EMdgmMRZ_txSectorInfo(dg['txSectorInfo'], sector)

        self.write_EMdgmMRZ_rxInfo(dg['rxInfo'])

        for detclass in range(dg['rxInfo']['numExtraDetectionClasses']):
            self.write_EMdgmMRZ_extraDetClassInfo(FID, dg['extraDetClassInfo'], detclass)

        Nseabedimage_samples = 0
        for record in range(dg['rxInfo']['numExtraDetections'] +
                            dg['rxInfo']['numSoundingsMaxMain']):
            self.write_EMdgmMRZ_sounding(dg['sounding'], record)
            Nseabedimage_samples += dg['sounding']['SInumSamples'][record]

        if Nseabedimage_samples > 0:
            if 'SIsample_desidB' not in dg:
                print(
                    "Warning, no Imagery data to write, although the field SInumSamples in the sounding datagram is non-zero.")
                print("This will produce an unreadable file.")
                # FIX: Should throw an error here.
            else:
                self.write_EMdgmMRZ_seabedImagery(dg, Nseabedimage_samples)

        self.FID.write(struct.pack("I", dg['header']['numBytesDgm']))

    def write_EMdgmMRZ_woImagery(self, dg):
        ''' A method to write an MRZ datagram back to disk, but omitting the imagery data.'''

        # First we need to see how much space the imagery data will take.
        Nseabedimage_samples = 0
        for record in range(dg['rxInfo']['numExtraDetections'] +
                            dg['rxInfo']['numSoundingsMaxMain']):
            Nseabedimage_samples += dg['sounding']['SInumSamples'][record]
        imageryBytes = Nseabedimage_samples * 2

        # Now we need to reset the total packet size.
        dg['header']['numBytesDgm'] -= imageryBytes

        # Now write the packet, just leave out the imagery
        # data and set Nsamples  to 0.
        self.write_EMdgmHeader(dg['header'])
        self.write_EMdgmMpartition(dg['partition'])
        self.write_EMdgmMbody(dg['cmnPart'])
        self.write_EMdgmMRZ_pingInfo(dg['pingInfo'])

        for sector in range(dg['pingInfo']['numTxSectors']):
            self.write_EMdgmMRZ_txSectorInfo(dg['txSectorInfo'], sector)

        write_EMdgmMRZ_rxInfo(dg['rxInfo'])

        for detclass in range(dg['rxInfo']['numExtraDetectionClasses']):
            self.write_EMdgmMRZ_extraDetClassInfo(dg['extraDetClassInfo'], detclass)

        Nseabedimage_samples = 0
        for record in range(dg['rxInfo']['numExtraDetections'] +
                            dg['rxInfo']['numSoundingsMaxMain']):
            # Zero out the number of imagery samples for each sounding.
            dg['sounding']['SInumSamples'][record] = 0
            self.write_EMdgmMRZ_sounding(dg['sounding'], record)
            Nseabedimage_samples += dg['sounding']['SInumSamples'][record]

        # Don't write the imagery data.
        # write_EMdgmMRZ_seabedImagery(FID, dg, Nseabedimage_samples)

        self.FID.write(struct.pack("I", dg['header']['numBytesDgm']))

    def write_EMdgmHeader(self, dg):
        ''' Method to write the datagram header.

        write_EMdgmHeader(FID, dg['header'])

        '''

        format_to_pack = "<1I4s2B1H2I"

        dg_seconds = int(dg['dgtime'])
        dg_nanoseconds = int((dg['dgtime'] - dg_seconds) * 1e9)

        self.FID.write(struct.pack(format_to_pack,
                                   dg['numBytesDgm'],
                                   dg['dgmType'],
                                   dg['dgmVersion'],
                                   dg['systemID'],
                                   dg['echoSounderID'],
                                   dg_seconds,
                                   dg_nanoseconds))

    def write_EMdgmMpartition(self, dg):
        ''' A method to write the Partition Information

        write_EMdgmMpartition(FID, dg['partition'])

        '''

        format_to_pack = "<2H"
        self.FID.write(struct.pack(format_to_pack,
                                   dg['numOfDgms'],
                                   dg['dgmNum']))

    def write_EMdgmMbody(self, dg):
        ''' A method to write the datagram body information

        write_EMdgmMbody(FID, dg['cmnPart'])

        '''

        format_to_pack = "<2H8B"
        self.FID.write(struct.pack(format_to_pack,
                                   dg['numBytesCmnPart'],
                                   dg['pingCnt'],
                                   dg['rxFansPerPing'],
                                   dg['rxFanIndex'],
                                   dg['swathsPerPing'],
                                   dg['swathAlongPosition'],
                                   dg['txTransducerInd'],
                                   dg['rxTransducerInd'],
                                   dg['numRxTransducers'],
                                   dg['algorithmType']))

    def write_EMdgmMRZ_pingInfo(self, dg):
        '''A method to write MRZ ping info.

        write_EMdgmMRZ_pingInfo(FID, dg['pinginfo'])

        '''

        format_to_pack_a = "<2H1f6B1H11f2h2B1H1I3f2H1f2H6f4B"
        self.FID.write(struct.pack(format_to_pack_a,
                                   dg['numBytesInfoData'],
                                   dg['padding0'],
                                   dg['pingRate_Hz'],
                                   dg['beamSpacing'],
                                   dg['depthMode'],
                                   dg['subDepthMode'],
                                   dg['distanceBtwSwath'],
                                   dg['detectionMode'],
                                   dg['pulseForm'],
                                   dg['padding1'],
                                   dg['frequencyMode_Hz'],
                                   dg['freqRangeLowLim_Hz'],
                                   dg['freqRangeHighLim_Hz'],
                                   dg['maxTotalTxPulseLength_sec'],
                                   dg['maxEffTxPulseLength_sec'],
                                   dg['maxEffTxBandWidth_Hz'],
                                   dg['absCoeff_dBPerkm'],
                                   dg['portSectorEdge_deg'],
                                   dg['starbSectorEdge_deg'],
                                   dg['portMeanCov_deg'],
                                   dg['stbdMeanCov_deg'],
                                   dg['portMeanCov_m'],
                                   dg['starbMeanCov_m'],
                                   dg['modeAndStabilisation'],
                                   dg['runtimeFilter1'],
                                   dg['runtimeFilter2'],
                                   dg['pipeTrackingStatus'],
                                   dg['transmitArraySizeUsed_deg'],
                                   dg['receiveArraySizeUsed_deg'],
                                   dg['transmitPower_dB'],
                                   dg['SLrampUpTimeRemaining'],
                                   dg['padding2'],
                                   dg['yawAngle_deg'],
                                   dg['numTxSectors'],
                                   dg['numBytesPerTxSector'],
                                   dg['headingVessel_deg'],
                                   dg['soundSpeedAtTxDepth_mPerSec'],
                                   dg['txTransducerDepth_m'],
                                   dg['z_waterLevelReRefPoint_m'],
                                   dg['x_kmallToall_m'],
                                   dg['y_kmallToall_m'],
                                   dg['latLongInfo'],
                                   dg['posSensorStatus'],
                                   dg['attitudeSensorStatus'],
                                   dg['padding3']))

        # For some reason, it doesn't work to do this all in one step, but it works broken up into two steps. *shrug*
        format_to_pack_b = "<2d1f"
        self.FID.write(struct.pack(format_to_pack_b,
                                   dg['latitude_deg'],
                                   dg['longitude_deg'],
                                   dg['ellipsoidHeightReRefPoint_m']))

    def write_EMdgmMRZ_txSectorInfo(self, dg, sector):
        ''' Write MRZ txSectorInfo for single index "sector".

        write_EMdgmMRZ_txSectorInfo(FID, dg['txSectorInfo'], sector)

        '''

        format_to_pack = "4B7f2B1H"
        self.FID.write(struct.pack(format_to_pack,
                                   dg['txSectorNumb'][sector],
                                   dg['txArrNumber'][sector],
                                   dg['txSubArray'][sector],
                                   dg['padding0'][sector],
                                   dg['sectorTransmitDelay_sec'][sector],
                                   dg['tiltAngleReTx_deg'][sector],
                                   dg['txNominalSourceLevel_dB'][sector],
                                   dg['txFocusRange_m'][sector],
                                   dg['centreFreq_Hz'][sector],
                                   dg['signalBandWidth_Hz'][sector],
                                   dg['totalSignalLength_sec'][sector],
                                   dg['pulseShading'][sector],
                                   dg['signalWaveForm'][sector],
                                   dg['padding1'][sector]))

    def write_EMdgmMRZ_rxInfo(self, dg):
        ''' Write MRZ rxInfo datagram.

            write_EMdgmMRZ_rxInfo(FID, dg['rxInfo'])

            '''

        format_to_pack = "4H4f4H"
        self.FID.write(struct.pack(format_to_pack,
                                   dg['numBytesRxInfo'],
                                   dg['numSoundingsMaxMain'],
                                   dg['numSoundingsValidMain'],
                                   dg['numBytesPerSounding'],
                                   dg['WCSampleRate'],
                                   dg['seabedImageSampleRate'],
                                   dg['BSnormal_dB'],
                                   dg['BSoblique_dB'],
                                   dg['extraDetectionAlarmFlag'],
                                   dg['numExtraDetections'],
                                   dg['numExtraDetectionClasses'],
                                   dg['numBytesPerClass']))

    def write_EMdgmMRZ_extraDetClassInfo(self, dg, detclass):
        ''' Write the MRZ sounding extra Detection Class information.

        write_EMdgmMRZ_extraDetClassInfo(FID,dg['extraDetClassInfo'],detclass)

        '''

        format_to_pack = "1H1b1B"
        self.FID.write(struct.pack(format_to_pack,
                                   dg['numExtraDetInClass'][detclass],
                                   dg['padding'][detclass],
                                   dg['alarmFlag'][detclass]))

    def write_EMdgmMRZ_sounding(self, dg, record):
        ''' Write MRZ soundings records.

        write_EMdgmMRZ_sounding(FID, dg['sounding'], record)

        '''

        format_to_pack = "1H8B1H6f2H18f4H"

        self.FID.write(struct.pack(format_to_pack,
                                   dg['soundingIndex'][record],
                                   dg['txSectorNumb'][record],
                                   dg['detectionType'][record],
                                   dg['detectionMethod'][record],
                                   dg['rejectionInfo1'][record],
                                   dg['rejectionInfo2'][record],
                                   dg['postProcessingInfo'][record],
                                   dg['detectionClass'][record],
                                   dg['detectionConfidenceLevel'][record],
                                   dg['padding'][record],
                                   dg['rangeFactor'][record],
                                   dg['qualityFactor'][record],
                                   dg['detectionUncertaintyVer_m'][record],
                                   dg['detectionUncertaintyHor_m'][record],
                                   dg['detectionWindowLength_sec'][record],
                                   dg['echoLength_sec'][record],
                                   dg['WCBeamNumb'][record],
                                   dg['WCrange_samples'][record],
                                   dg['WCNomBeamAngleAcross_deg'][record],
                                   dg['meanAbsCoeff_dbPerkm'][record],
                                   dg['reflectivity1_dB'][record],
                                   dg['reflectivity2_dB'][record],
                                   dg['receiverSensitivityApplied_dB'][record],
                                   dg['sourceLevelApplied_dB'][record],
                                   dg['BScalibration_dB'][record],
                                   dg['TVG_dB'][record],
                                   dg['beamAngleReRx_deg'][record],
                                   dg['beamAngleCorrection_deg'][record],
                                   dg['twoWayTravelTime_sec'][record],
                                   dg['twoWayTravelTimeCorrection_sec'][record],
                                   dg['deltaLatitude_deg'][record],
                                   dg['deltaLongitude_deg'][record],
                                   dg['z_reRefPoint_m'][record],
                                   dg['y_reRefPoint_m'][record],
                                   dg['x_reRefPoint_m'][record],
                                   dg['beamIncAngleAdj_deg'][record],
                                   dg['realTimeCleanInfo'][record],
                                   dg['SIstartRange_samples'][record],
                                   dg['SIcentreSample'][record],
                                   dg['SInumSamples'][record]))

    def write_EMdgmMRZ_seabedImagery(self, dg, Nseabedimage_samples):
        ''' Write the MRZ seabedImagery datagram

        write_EMdgmMRZ_seabedImagery(FID, dg['SIsample_desidB'])

        '''
        format_to_pack = str(Nseabedimage_samples) + "h"

        self.FID.write(struct.pack(format_to_pack,
                                   *dg['SIsample_desidB']))

    ###############################################################
    # Routines for writing and reading custom compressed packets
    ###############################################################

    def compressSoundings(self, dg):
        ''' A method to compress the soundings table by column rather than by row.'''
        record = len(dg['soundingIndex'])
        format_to_pack = "1H8B1H6f2H18f4H"

        buffer = struct.pack(str(record) + "H", *dg['soundingIndex'])

        buffer += struct.pack(str(record) + "B", *dg['txSectorNumb'])
        buffer += struct.pack(str(record) + "B", *dg['detectionType'])
        buffer += struct.pack(str(record) + "B", *dg['detectionMethod'])
        buffer += struct.pack(str(record) + "B", *dg['rejectionInfo1'])
        buffer += struct.pack(str(record) + "B", *dg['rejectionInfo2'])
        buffer += struct.pack(str(record) + "B", *dg['postProcessingInfo'])
        buffer += struct.pack(str(record) + "B", *dg['detectionClass'])
        buffer += struct.pack(str(record) + "B", *dg['detectionConfidenceLevel'])

        buffer += struct.pack(str(record) + "H", *dg['padding'])

        buffer += struct.pack(str(record) + "f", *dg['rangeFactor'])
        buffer += struct.pack(str(record) + "f", *dg['qualityFactor'])
        buffer += struct.pack(str(record) + "f", *dg['detectionUncertaintyVer_m'])
        buffer += struct.pack(str(record) + "f", *dg['detectionUncertaintyHor_m'])
        buffer += struct.pack(str(record) + "f", *dg['detectionWindowLength_sec'])
        buffer += struct.pack(str(record) + "f", *dg['echoLength_sec'])

        buffer += struct.pack(str(record) + "H", *dg['WCBeamNumb'])
        buffer += struct.pack(str(record) + "H", *dg['WCrange_samples'])

        buffer += struct.pack(str(record) + "f", *dg['WCNomBeamAngleAcross_deg'])
        buffer += struct.pack(str(record) + "f", *dg['meanAbsCoeff_dbPerkm'])
        buffer += struct.pack(str(record) + "f", *dg['reflectivity1_dB'])
        buffer += struct.pack(str(record) + "f", *dg['reflectivity2_dB'])
        buffer += struct.pack(str(record) + "f", *dg['receiverSensitivityApplied_dB'])
        buffer += struct.pack(str(record) + "f", *dg['sourceLevelApplied_dB'])
        buffer += struct.pack(str(record) + "f", *dg['BScalibration_dB'])
        buffer += struct.pack(str(record) + "f", *dg['TVG_dB'])
        buffer += struct.pack(str(record) + "f", *dg['beamAngleReRx_deg'])
        buffer += struct.pack(str(record) + "f", *dg['beamAngleCorrection_deg'])
        buffer += struct.pack(str(record) + "f", *dg['twoWayTravelTime_sec'])
        buffer += struct.pack(str(record) + "f", *dg['twoWayTravelTimeCorrection_sec'])
        buffer += struct.pack(str(record) + "f", *dg['deltaLatitude_deg'])
        buffer += struct.pack(str(record) + "f", *dg['deltaLongitude_deg'])
        buffer += struct.pack(str(record) + "f", *dg['z_reRefPoint_m'])
        buffer += struct.pack(str(record) + "f", *dg['y_reRefPoint_m'])
        buffer += struct.pack(str(record) + "f", *dg['x_reRefPoint_m'])
        buffer += struct.pack(str(record) + "f", *dg['beamIncAngleAdj_deg'])

        buffer += struct.pack(str(record) + "H", *dg['realTimeCleanInfo'])
        buffer += struct.pack(str(record) + "H", *dg['SIstartRange_samples'])
        buffer += struct.pack(str(record) + "H", *dg['SIcentreSample'])
        buffer += struct.pack(str(record) + "H", *dg['SInumSamples'])

        return bz2.compress(buffer)

    def encodeArrayIntoUintX(self, A, res):
        ''' Differential encoding of an array of values into a byte array
        A:   An array of values
        res: Desired resolution. This determines whether the encoding is
             in an 8-bit or 16-bit array. Details provided below.
        returns: bytes buffer containing packed values and metadata to unpack it.
        The data is differentially encoded, meaning that the difference
        in sequential values is calculated, then the minimum differential value
        is subtracted off the array before scaling each value by max_bits / (max-min).
        max_bits is 255 for uint8 encoding and 65535 for uint16 encoding. To
        determine the encoding, (max-min) / max_bits is compared to the desired
        resolution to ensure the minimum increment falls below it. uint8 is checked
        first, if it fails, uint16 is checked. If it also fails, uint32 is
        used and no actual compression is achieved.
        A buffer is created from the result containing everything needed to
        decipher it. Specifically:
        The first value of the original array as a 4-byte float
        Min difference values as 4-byte float.
        Max difference value as a 4-byte float.
        The number of bits used in the encoding (8 or 16) as a uint8.
        The number of difference values (len(A)-1) as an 4-byte unsigned int
        The array of scaled difference values cast to unsigned "max_bits" integers
        '''
        if isinstance(A, list):
            A = np.array(A)

        # There are two strategies taken here. Sometimes the
        # data varies smoothly but over a large range, and it
        # is more efficient to encode the data's sequential
        # differences, since they are small in amplitude.
        # But sometimes the data is very stochastic and the
        # first range of differences are large relative to
        # the maximum and minimum values in the data. For
        # example consider the sequence [0 2 0]. The range
        # of the values is 2, but the range of the first
        # differences is 4 (+2 - -2). In this case, it is
        # more efficient to encode the values themselves.

        valuesToEncode = np.diff(A.flatten())

        maxv = np.max(valuesToEncode)
        minv = np.min(valuesToEncode)

        maxA = np.max(A)
        minA = np.min(A)

        # print("maxvaluesToEncode:%f, minvaluesToEncode:%f" % (maxv,minv))
        # print("maxA:%f, minA:%f" % (maxA,minA))

        differentialEncode = True
        if (maxA - minA) < (maxv - minv):
            differentialEncode = False
            maxv = maxA
            minv = minA
            valuesToEncode = A[1:]

        # print("Encoding: %s" % differentialEncode)

        if ((maxv - minv) / 255.0) < res:
            bits = 8
        elif ((maxv - minv) / 65535.0) < res:
            bits = 16
        else:
            bits = 32

            # print("CANNOT Maintain Resolution - Loss of Data!")
            # print("max diff: %f, min diff: %f, res: %f" % (maxv, minv, res))
            # bits = 16
            # return None
        # print(bits)
        if maxv == minv:
            # Value is constant.
            scaleFactor = 1.0
        else:
            if bits == 8:
                scaleFactor = 255.0 / (maxv - minv)
            elif bits == 16:
                scaleFactor = 65535.0 / (maxv - minv)
            else:
                scaleFactor = 4294967295.0 / (maxv - minv)

        tmp = (((valuesToEncode - minv) * scaleFactor)).astype(int)

        # This bullshit gets around an apparant bug in the struct module.
        if isinstance(A[0], np.ndarray):
            tmp2 = A[0].tolist()
        else:
            tmp2 = A[0]

        if isinstance(tmp2, np.int64) or isinstance(tmp2, np.float64):
            buffer = struct.pack('f', tmp2)
        else:
            buffer = struct.pack('f', tmp2[0])
        # buffer = struct.pack('f',float(A[0][0]))

        N = len(tmp)
        buffer += struct.pack('f', minv)
        buffer += struct.pack('f', maxv)
        # Set a marker by recording the number of points
        # to encode as a negative number to indicate that
        # the fields have been differentially encoded.
        if differentialEncode:
            buffer += struct.pack('i', -N)
        else:
            buffer += struct.pack('i', N)
        buffer += struct.pack('B', bits)

        if bits == 8:
            buffer += struct.pack(str(N) + 'B', *tmp)
        if bits == 16:
            buffer += struct.pack(str(N) + 'H', *tmp)
        if bits == 32:
            buffer += struct.pack(str(N) + 'I', *tmp)

        return buffer

    def decodeUintXintoArray(self, buffer):
        ''' Decodes differential-encoded data from X-bit unsigned integers into a float array.
        See encodeArrayIntoUintX().

        '''

        fields = struct.unpack('fffiB', buffer[0:17])
        A0 = fields[0]
        minv = fields[1]
        maxv = fields[2]
        N = fields[3]
        differentialDecode = False
        if N < 0:
            differentialDecode = True
            N = -N

        bits = fields[4]

        if bits == 8:
            dA = struct.unpack(str(N) + 'B', buffer[17:(17 + N)])
            bytesDecoded = 17 + N
        elif bits == 16:
            dA = struct.unpack(str(N) + 'H', buffer[17:(17 + N * 2)])
            bytesDecoded = 17 + (N * 2)
        elif bits == 32:
            dA = struct.unpack(str(N) + 'I', buffer[17:(17 + N * 4)])
            bytesDecoded = 17 + (N * 4)

        if differentialDecode:
            if bits == 8:
                orig = np.cumsum(
                    [A0] + list((np.array([float(x) for x in dA]) * (maxv - minv) / 255.0) + minv)).tolist()
            elif bits == 16:
                orig = np.cumsum(
                    [A0] + list((np.array([float(x) for x in dA]) * (maxv - minv) / 65535.0) + minv)).tolist()
            else:
                orig = np.cumsum(
                    [A0] + list((np.array([float(x) for x in dA]) * (maxv - minv) / 4294967295.0) + minv)).tolist()
        else:
            if bits == 8:
                orig = [A0] + list((np.array([float(x) for x in dA]) * (maxv - minv) / 255.0) + minv)
            elif bits == 16:
                orig = [A0] + list((np.array([float(x) for x in dA]) * (maxv - minv) / 65535.0) + minv)
            else:
                orig = [A0] + list((np.array([float(x) for x in dA]) * (maxv - minv) / 4294967295.0) + minv)

                # print(A0)
            # print(minv)
            # print(maxv)
            # print(N)
            # print(bits)

        return (orig, bytesDecoded)

    def encodeAndCompressSoundings(self, dg):
        ''' A method to differential-encode and compress the soundings table.

        Float values are encoded in this way
        See encodeArrayIntoUintX() for details on how.
        Some attempt is made to minimize the impact of
        non-float fields in the original datagram too.

        A note about the "res" or resolution argument to
        encodeArrayIntoUintX(): This field attempts to be
        the maximum error one can expect between the original
        value and the final decoded value after encoding.
        But because it is the first difference in values that
        are actually encoded, errors accumulate in the
        decoding process as the decoded differences are cumulateively
        summed and the errors that result can be larger than the
        "res" value. Some experimentation is required to ensure
        sufficient bits are used to reduce the desired error.
        '''

        record = len(dg['soundingIndex'])

        buffer = struct.pack(str(record) + "H", *dg['soundingIndex'])

        ## The following optimization has almost no effect
        ## because of the compressoin applied to the
        ## sounding buffer:

        # Valid values for txSectorNumber are 0-7 (probably)
        # Valid values for detectionType are 0-2
        # Valid values for detectionMethod are 0-15.

        # But detectionMethod > 2 have been reserved for
        # future use as long as any one can remember. Under
        # the assumption that Kongsberg won't record more
        # than 9 detection methods or have more than 9
        # transmit sectors, these values can be packed
        # into a single 8-bit value.

        tmp = (np.array(dg['detectionType']) * 100. +
               np.array(dg['detectionMethod']) * 10. +
               np.array(dg['txSectorNumb'])).astype(int)
        buffer += struct.pack(str(record) + "B", *tmp)
        # I don't think there's any way to tell with no ambiguity
        # when decoding if they were packed or not. For example,
        # if there were just one tx sector, and only normal type
        # detections of using amplitude method, the values would
        # all be 1, which is a valid tx sector value. So I'll leave
        # these commented out.
        # else:
        # buffer += struct.pack(str(record)+"B", *dg['txSectorNumb'])
        # buffer += struct.pack(str(record)+"B", *dg['detectionType'])
        # buffer += struct.pack(str(record)+"B", *dg['detectionMethod'])

        buffer += struct.pack(str(record) + "B", *dg['rejectionInfo1'])
        buffer += struct.pack(str(record) + "B", *dg['rejectionInfo2'])
        buffer += struct.pack(str(record) + "B", *dg['postProcessingInfo'])
        buffer += struct.pack(str(record) + "B", *dg['detectionClass'])
        buffer += struct.pack(str(record) + "B", *dg['detectionConfidenceLevel'])

        # No point in carrying along the padding field. It's for byte alignment
        # but we've already reorganized the data. so we can omit it
        # and recreate it on the other side.

        buffer += self.encodeArrayIntoUintX(dg['rangeFactor'], 1)
        buffer += self.encodeArrayIntoUintX(dg['qualityFactor'], .01)
        buffer += self.encodeArrayIntoUintX(dg['detectionUncertaintyVer_m'], .01)
        buffer += self.encodeArrayIntoUintX(dg['detectionUncertaintyHor_m'], .1)
        buffer += self.encodeArrayIntoUintX(dg['detectionWindowLength_sec'], .001)
        buffer += self.encodeArrayIntoUintX(dg['echoLength_sec'], .001)

        buffer += struct.pack(str(record) + "H", *dg['WCBeamNumb'])
        buffer += struct.pack(str(record) + "H", *dg['WCrange_samples'])
        buffer += self.encodeArrayIntoUintX(dg['WCNomBeamAngleAcross_deg'], .001)

        # meanAbsCoeff_dbPerkm is a single value per transmit sector. No point in
        # encoding them all. This method first line gets a unique index for
        # each sector. These are used to capture a dbPkm for each.
        _, idx = np.unique(dg['txSectorNumb'], return_index=True)
        # Encoding as ushort's in .01's of a dB.
        vals = np.round(np.array(dg['meanAbsCoeff_dbPerkm'])[np.sort(idx)] * 100).astype(int)
        buffer += struct.pack(str(len(idx)) + "H", *vals)

        # Reflectivity1_dB values get -100 when the detect is invalid
        # and reflectivity2_dB get any of several values thare are
        # also non-sensical. Because they are never near the mean of
        # the valid data, the differential encoding scheme used
        # here becomes very inefficient. So we will set them to
        # the mode of the data to optimize the encoding and set them
        # back to their original values on decoding.

        # The values are rounded to 2 decimal places first because
        # they are floats and the chances that any two floats are
        # the same is quite small.
        dg['reflectivity1_dB'] = np.round(dg['reflectivity1_dB'], decimals=2)

        # This wizardry calculates the mode (most frequent value)
        # of the reflectivity values associated with valid detects.
        reflectivity_mode = stats.mode([y for x, y in
                                        zip(dg['detectionMethod'], dg['reflectivity1_dB'])
                                        if x != 0])[0][0]
        # Replace all the non-detects with the mode.
        dg['reflectivity1_dB'] = [y if x != 0 else reflectivity_mode
                                  for x, y in
                                  zip(dg['detectionMethod'], dg['reflectivity1_dB'])]

        # Do the same with reflectiivty2.
        dg['reflectivity2_dB'] = np.round(dg['reflectivity2_dB'], decimals=2)
        reflectivity_mode = stats.mode([y for x, y in
                                        zip(dg['detectionMethod'], dg['reflectivity2_dB'])
                                        if x != 0])[0][0]
        # Replace all the non-detects with the mode.
        dg['reflectivity2_dB'] = [y if x != 0 else reflectivity_mode
                                  for x, y in
                                  zip(dg['detectionMethod'], dg['reflectivity2_dB'])]

        buffer += self.encodeArrayIntoUintX(dg['reflectivity1_dB'], .1)
        buffer += self.encodeArrayIntoUintX(dg['reflectivity2_dB'], .001)
        buffer += self.encodeArrayIntoUintX(dg['receiverSensitivityApplied_dB'], .001)
        buffer += self.encodeArrayIntoUintX(dg['sourceLevelApplied_dB'], .001)
        buffer += self.encodeArrayIntoUintX(dg['BScalibration_dB'], .001)
        buffer += self.encodeArrayIntoUintX(dg['TVG_dB'], .001)
        buffer += self.encodeArrayIntoUintX(dg['beamAngleReRx_deg'], .001)
        buffer += self.encodeArrayIntoUintX(dg['beamAngleCorrection_deg'], .001)
        buffer += self.encodeArrayIntoUintX(dg['twoWayTravelTime_sec'], .000001)
        buffer += self.encodeArrayIntoUintX(dg['twoWayTravelTimeCorrection_sec'], .0000001)
        buffer += self.encodeArrayIntoUintX(dg['deltaLatitude_deg'], .0000001)
        buffer += self.encodeArrayIntoUintX(dg['deltaLongitude_deg'], .0000001)
        buffer += self.encodeArrayIntoUintX(dg['z_reRefPoint_m'], .001)
        buffer += self.encodeArrayIntoUintX(dg['y_reRefPoint_m'], .001)
        buffer += self.encodeArrayIntoUintX(dg['x_reRefPoint_m'], .001)
        buffer += self.encodeArrayIntoUintX(dg['beamIncAngleAdj_deg'], .001)

        # realTimeCleanInfo is for future use. So we can omit it for now.
        # buffer += struct.pack(str(record)+"H", *dg['realTimeCleanInfo'])

        buffer += struct.pack(str(record) + "H", *dg['SIstartRange_samples'])
        buffer += struct.pack(str(record) + "H", *dg['SIcentreSample'])
        buffer += struct.pack(str(record) + "H", *dg['SInumSamples'])

        return bz2.compress(buffer)

    def expandAndDecodeSoundings(self, buffer, records):
        ''' When the soundings datagram is differential-encoded and compressed, this method reverses it on reading.
        buffer:  bytes object containing the compressed data.
        records: Number of soundings encoded in the block.
        returns: dg['sounding'] containing dictionary of lists of sounding record fields.
        '''

        buffer = bz2.decompress(buffer)
        dg = {}
        ptr = 0
        dg['soundingIndex'] = struct.unpack(str(records) + "H", buffer[0:(records * 2)])
        ptr += (records * 2)

        tmp = np.array(struct.unpack(str(records) + "B", buffer[ptr:(ptr + records)]))
        ptr += records
        dg['detectionType'] = np.round(tmp / 100.).astype(int)
        dg['detectionMethod'] = np.round((tmp - dg['detectionType'] * 100) / 10.).astype(int)
        dg['txSectorNumb'] = np.round((tmp - dg['detectionType'] * 100 - dg['detectionMethod'] * 10)).astype(int)
        dg['detectionType'] = dg['detectionType'].tolist()
        dg['detectionMethod'] = dg['detectionMethod'].tolist()
        dg['txSectorNumb'] = dg['txSectorNumb'].tolist()
        # dg['txSectorNumb'] = struct.unpack(str(records) + "B", buffer[ptr:(ptr + records )])
        # ptr += records
        # dg['detectionType'] = struct.unpack(str(records) + "B", buffer[ptr:(ptr + records)])
        # ptr += records
        # dg['detectionMethod'] = struct.unpack(str(records) + "B", buffer[ptr:(ptr + records)])
        # ptr += records
        dg['rejectionInfo1'] = struct.unpack(str(records) + "B", buffer[ptr:(ptr + records)])
        ptr += records
        dg['rejectionInfo2'] = struct.unpack(str(records) + "B", buffer[ptr:(ptr + records)])
        ptr += records
        dg['postProcessingInfo'] = struct.unpack(str(records) + "B", buffer[ptr:(ptr + records)])
        ptr += records
        dg['detectionClass'] = struct.unpack(str(records) + "B", buffer[ptr:(ptr + records)])
        ptr += records
        dg['detectionConfidenceLevel'] = struct.unpack(str(records) + "B", buffer[ptr:(ptr + records)])
        ptr += records

        # The padding data is not encoded, so we just generate 0's for it here.
        dg['padding'] = list(np.zeros(shape=len(dg['soundingIndex'])).astype(int))

        dg['rangeFactor'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['qualityFactor'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['detectionUncertaintyVer_m'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['detectionUncertaintyHor_m'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['detectionWindowLength_sec'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['echoLength_sec'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded

        dg['WCBeamNumb'] = struct.unpack(str(records) + "H", buffer[ptr:(ptr + (records * 2))])
        ptr += (records * 2)
        dg['WCrange_samples'] = struct.unpack(str(records) + "H", buffer[ptr:(ptr + (records * 2))])
        ptr += (records * 2)

        dg['WCNomBeamAngleAcross_deg'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded

        # meanAbsCoeff_dbPerkm is a single value for each transmit sector.
        # And we've only encodeied one for each as ushorts in 0.01 dB.
        # So we extract these.
        Nsectors = len(np.unique(dg['txSectorNumb']))
        values = np.array(struct.unpack(str(Nsectors) + "H", buffer[ptr:(ptr + (Nsectors * 2))])) / 100.0
        ptr += (Nsectors * 2)
        # Then assign them to each sector.
        tmp = np.zeros(shape=len(dg['soundingIndex']))
        for sectoridx in np.unique(dg['txSectorNumb']):
            tmp[dg['txSectorNumb'] == sectoridx] = values[sectoridx]
        dg['meanAbsCoeff_dbPerkm'] = tmp.tolist()

        dg['reflectivity1_dB'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        # Reset values for no-detect values that were modified to
        # improve compression.
        dg['reflectivity1_dB'] = [-100. if x == 0 else y
                                  for x, y in
                                  zip(dg['detectionMethod'], dg['reflectivity1_dB'])]

        dg['reflectivity2_dB'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        # Reset values for no-detect values that were modified to
        # improve compression. Note this makes a suble if inconsequential
        # change to the file, as the values in reflectivity2_dB for
        # failed detections are not -100. They are not uniform in value
        # and so cannot be replaced exactly here. But since these
        # are for non-detects it should not matter to anyone. (I hope)
        dg['reflectivity2_dB'] = [-100. if x == 0 else y
                                  for x, y in
                                  zip(dg['detectionMethod'], dg['reflectivity2_dB'])]

        dg['receiverSensitivityApplied_dB'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['sourceLevelApplied_dB'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['BScalibration_dB'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['TVG_dB'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['beamAngleReRx_deg'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['beamAngleCorrection_deg'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['twoWayTravelTime_sec'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['twoWayTravelTimeCorrection_sec'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['deltaLatitude_deg'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['deltaLongitude_deg'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['z_reRefPoint_m'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['y_reRefPoint_m'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['x_reRefPoint_m'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded
        dg['beamIncAngleAdj_deg'], bytesDecoded = self.decodeUintXintoArray(buffer[ptr:])
        ptr += bytesDecoded

        # dg['realTimeCleanInfo'] = struct.unpack(str(records) + "H", buffer[ptr:(ptr + (records * 2))])
        # ptr += (records * 2)
        dg['realTimeCleanInfo'] = list(np.zeros(shape=len(dg['soundingIndex'])).astype(int))
        dg['SIstartRange_samples'] = struct.unpack(str(records) + "H", buffer[ptr:(ptr + (records * 2))])
        ptr += (records * 2)
        dg['SIcentreSample'] = struct.unpack(str(records) + "H", buffer[ptr:(ptr + (records * 2))])
        ptr += (records * 2)
        dg['SInumSamples'] = struct.unpack(str(records) + "H", buffer[ptr:(ptr + (records * 2))])
        ptr += (records * 2)

        return dg

    def write_EncodedCompressedSoundings(self, buffer):
        ''' Write MRZ soundings records.
        write_EMdgmMRZ_sounding(FID, dg['sounding'])
        '''
        self.FID.write(struct.pack('I', len(buffer)))
        self.FID.write(buffer)
        return

    def encodeAndCompressImagery(self, dg):
        ''' A method to encode and compress the imagery data.'''
        buffer = self.encodeArrayIntoUintX(np.array(dg['SIsample_desidB']), .1)
        return bz2.compress(buffer)

    def decodeAndDecompresssImagery(self, buffer, Nseabedimage_samples):
        format_to_unpack = str(Nseabedimage_samples) + "h"
        return self.decodeUintXintoArray(bz2.decompress(buffer))

    def write_EncodedCompressedImagery(self, buffer):
        ''' A method to write the encoded compressed imagery'''
        self.FID.write(struct.pack("I", len(buffer)))
        self.FID.write(buffer)

    def write_EMdgmCZ0(self, dg):
        ''' A method to write an MRZ datagram back to disk, but omitting the imagery data.'''

        # First we need to see how much space the imagery data will take.
        # And set the number of imagery samples per sounding field to zero.
        Nseabedimage_samples = 0
        for record in range(dg['rxInfo']['numExtraDetections'] +
                            dg['rxInfo']['numSoundingsMaxMain']):
            Nseabedimage_samples += dg['sounding']['SInumSamples'][record]
            # dg['sounding']['SInumSamples'][record] = 0
        imageryBytes = Nseabedimage_samples * 2

        # Now we need to reset the total packet size.
        # dg['header']['numBytesDgm'] -= imageryBytes

        # And we need to create a new MRZ packet type to hold compressed data.
        dg['header']['dgmType'] = b'#CZ0'

        imageryBuffer = self.encodeAndCompressImagery(dg)

        soundingsBuffer = self.encodeAndCompressSoundings(dg['sounding'])

        # Reduce the datagram size by the difference in size of the
        # original and compressed sounding data, including the size
        # of teh soundings buffer which is written as a 4-type int.
        Nsoundings = (dg['rxInfo']['numExtraDetections'] +
                      dg['rxInfo']['numSoundingsMaxMain'])
        dg['header']['numBytesDgm'] -= (Nsoundings * 120
                                        - (len(soundingsBuffer) + 4))

        # Reduce the datagram size by the difference in size of the
        # original and encoded, compressed imagery data.
        dg['header']['numBytesDgm'] -= (imageryBytes - (len(imageryBuffer) + 4))

        # Now write the packet, just leave out the imagery
        # data and set Nsamples  to 0.
        self.write_EMdgmHeader(dg['header'])
        self.write_EMdgmMpartition(dg['partition'])
        self.write_EMdgmMbody(dg['cmnPart'])
        self.write_EMdgmMRZ_pingInfo(dg['pingInfo'])

        for sector in range(dg['pingInfo']['numTxSectors']):
            self.write_EMdgmMRZ_txSectorInfo(dg['txSectorInfo'], sector)

        self.write_EMdgmMRZ_rxInfo(dg['rxInfo'])

        for detclass in range(dg['rxInfo']['numExtraDetectionClasses']):
            self.write_EMdgmMRZ_extraDetClassInfo(dg['extraDetClassInfo'], detclass)

        self.write_EncodedCompressedSoundings(soundingsBuffer)
        self.write_EncodedCompressedImagery(imageryBuffer)

        self.FID.write(struct.pack("I", dg['header']['numBytesDgm']))

    def write_EMdgmCZ1(self, dg):
        ''' A method to write a new datagram compressing teh soundings and
        omitting the imagery data.'''

        # First we need to see how much space the imagery data will take.
        # And set the number of imagery samples per sounding field to zero.
        Nseabedimage_samples = 0
        for record in range(dg['rxInfo']['numExtraDetections'] +
                            dg['rxInfo']['numSoundingsMaxMain']):
            Nseabedimage_samples += dg['sounding']['SInumSamples'][record]
            dg['sounding']['SInumSamples'][record] = 0
        imageryBytes = Nseabedimage_samples * 2

        # Now we need to reset the total packet size.
        dg['header']['numBytesDgm'] -= imageryBytes

        # And we need to create a new MRZ packet type to hold compressed data.
        dg['header']['dgmType'] = b'#CZ1'

        soundingsBuffer = self.encodeAndCompressSoundings(dg['sounding'])

        # Reduce the datagram size by the difference in size of the
        # original and compressed sounding data, including the size
        # of the soundings buffer which is also written, as a 4-type int.
        Nsoundings = (dg['rxInfo']['numExtraDetections'] +
                      dg['rxInfo']['numSoundingsMaxMain'])
        dg['header']['numBytesDgm'] -= (Nsoundings * 120
                                        - (len(soundingsBuffer) + 4))

        # Now write the packet, just leave out the imagery
        # data and set Nsamples  to 0.
        self.write_EMdgmHeader(dg['header'])
        self.write_EMdgmMpartition(dg['partition'])
        self.write_EMdgmMbody(dg['cmnPart'])
        self.write_EMdgmMRZ_pingInfo(dg['pingInfo'])

        for sector in range(dg['pingInfo']['numTxSectors']):
            self.write_EMdgmMRZ_txSectorInfo(dg['txSectorInfo'], sector)

        self.write_EMdgmMRZ_rxInfo(dg['rxInfo'])

        for detclass in range(dg['rxInfo']['numExtraDetectionClasses']):
            self.write_EMdgmMRZ_extraDetClassInfo(dg['extraDetClassInfo'], detclass)

        self.write_EncodedCompressedSoundings(soundingsBuffer)
        # write_EncodedCompressedImagery(FID,imageryBuffer)
        # Don't write the imagery data.
        # write_EMdgmMRZ_seabedImagery(FID, dg, Nseabedimage_samples)

        self.FID.write(struct.pack("I", dg['header']['numBytesDgm']))

    def read_EMdgmCZ0(self):
        """
        The #CR0 datagram is a custom datagram in which the sounding data
        and imagery data are encoded and compressed.

        The format of this datagram will evolve as better methods are devised.
        Therefore, files compressed in this way should only be used in a
        temporary way for passing data over telemetry links. Files left
        compressed are in danger of being unreadable in future releases.

        """

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
        Nseabedimage_samples = 0

        soundingsBuffer = self.read_EncodedCompressedSoundingsBlock()

        Nsoundings = (dg['rxInfo']['numExtraDetections'] +
                      dg['rxInfo']['numSoundingsMaxMain'])
        dg['sounding'] = self.expandAndDecodeSoundings(soundingsBuffer,
                                                       Nsoundings)

        for record in range(Nsoundings):
            Nseabedimage_samples += dg['sounding']['SInumSamples'][record]

        # Read the seabed imagery.
        # Seabed image sample amplitude, in 0.1 dB. Actual number of
        # seabed image samples (SIsample_desidB) to be found
        # by summing parameter SInumSamples in struct EMdgmMRZ_sounding_def
        # for all beams. Seabed image data are raw beam sample data
        # taken from the RX beams. The data samples are selected
        # based on the bottom detection ranges. First sample for
        # each beam is the one with the lowest range. The centre
        # sample from each beam is georeferenced (x, y, z data from
        # the detections). The BS corrections applied at the centre
        # sample are the same as used for reflectivity2_dB
        # (struct EMdgmMRZ_sounding_def).
        imageryBuffer = self.read_EncodedCompressedImageryBlock()
        dg['SIsample_desidB'], bytesDecoded = self.decodeAndDecompresssImagery(imageryBuffer,
                                                                               Nseabedimage_samples)
        dg['SIsample_desidB'] = np.array(dg['SIsample_desidB'], dtype=int)

        # Increase the reported size of the packet by the increase
        # in the size of the decoded soundings block. There are 120
        # bytes per sounding. And the size of the soundings buffer
        # is also recorded, as a 4-byte int.
        dg['header']['numBytesDgm'] += (Nsoundings * 120 -
                                        (len(soundingsBuffer) + 4))
        # Same for compressed imagery.
        dg['header']['numBytesDgm'] += (Nseabedimage_samples * 2 -
                                        (len(imageryBuffer) + 4))

        # Seek to end of the packet.
        self.FID.seek(start + dg['header']['numBytesDgm'], 0)

        return dg

    def read_EMdgmCZ1(self):
        """
        The #CR1 datagram is a custom datagram in which the sounding data
        are encoded and compressed and imagery is omitted.

        The format of this datagram will evolve as better methods are devised.
        Therefore, files compressed in this way should only be used in a
        temporary way for passing data over telemetry links. Files left
        compressed are in danger of being unreadable in future releases.

        """

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
        Nseabedimage_samples = 0

        soundingsBuffer = self.read_EncodedCompressedSoundingsBlock()
        Nsoundings = (dg['rxInfo']['numExtraDetections'] +
                      dg['rxInfo']['numSoundingsMaxMain'])
        dg['sounding'] = self.expandAndDecodeSoundings(soundingsBuffer, Nsoundings)

        # Increase the reported size of the packet by the increase
        # in the size of the decoded soundings block. There are 120
        # bytes per sounding. And the size of the soundings buffer
        # is also recorded, as a 4-byte int.
        dg['header']['numBytesDgm'] += (Nsoundings * 120 -
                                        (len(soundingsBuffer) + 4))
        # Skip the imagery data...

        # Seek to end of the packet.
        self.FID.seek(start + dg['header']['numBytesDgm'], 0)

        return dg

    def read_EncodedCompressedSoundingsBlock(self):
        ''' Read the compressed soundings block'''
        bytestoread = struct.unpack('I', self.FID.read(4))
        buffer = self.FID.read(bytestoread[0])
        return buffer

    def read_EncodedCompressedImageryBlock(self):
        ''' Read the compressed imagery block.'''
        bytestoread = struct.unpack('I', self.FID.read(4))
        buffer = self.FID.read(bytestoread[0])
        return buffer

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
            print("Opening: %s to read" % filetoopen)

        self.FID = open(filetoopen, "rb")

    def OpenFiletoWrite(self, inputfilename=None):
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
            print("Opening: %s to write" % filetoopen)

        self.FID = open(filetoopen, "wb")

    def closeFile(self):
        """ Close a file."""
        if self.FID is not None:
            self.FID.close()

    def print_datagram(self, dg):
        """ A utility function to print the fields of a parsed datagram. """
        print("\n")
        for k, v in dg.items():
            print("%s:\t\t\t%s\n" % (k, str(v)))

    def index_file(self):
        """ Index a KMALL file - message type, time, size, byte offset. """

        if self.FID is None:
            self.OpenFiletoRead()
        else:
            self.closeFile()  # forces flushing.
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
        ''' Extract navigation data.
        Only works when data is interpreted into the KMbinary record at the
        moment.'''
        self.extract_attitude()

    def extract_attitude(self):
        ''' Extract all raw attitude data from data file into self.att
        FIX: This method needs to be much more robust. It currently only
        handles our situation in which we are providing POS/MV Group 102
        messages, and these, it appears, are being interpreted into the
        KMbinary datagram. But it does not handle 1) multiple navigation
        inputs, 2) multiple navigation input types, 3) there are no checks to
        see that the data is valid. etc.
        '''

        if self.Index is None:
            self.index_file()

        if self.FID is None:
            self.OpenFiletoRead()

        # Get offsets for 'SKM' attitude datagrams.
        SKMOffsets = [x for x, y in zip(self.msgoffset, self.msgtype)
                      if y == "b'#SKM'"]

        attitudeDatagrams = list()
        for offset in SKMOffsets:
            self.FID.seek(offset, 0)
            dg = self.read_EMdgmSKM()
            attitudeDatagrams.append(dg['sample']['KMdefault'])

        # Convert list of dictionaries to dictionary of lists.
        self.att = self.listofdicts2dictoflists(attitudeDatagrams)

        self.FID.seek(0, 0)
        return

    def listofdicts2dictoflists(self, listofdicts):
        """ A utility  to convert a list of dicts to a dict of lists."""
        # dg = {}
        #
        # # This is done in two steps, handling both dictionary items that are
        # # lists and scalars separately. As long as no item combines both lists
        # # and scalars the method works.
        # #
        # # There is some mechanism to handle this in a single list
        # # comprehension statement, checking for types on the fly, but I cannot
        # # find any syntax that returns the proper result.
        # if len(listofdicts) == 0:
        #     return None
        #
        # for k, v in listofdicts[0].items():
        #     dg[k] = [item for dictitem in listofdicts
        #              if isinstance(dictitem[k], list)
        #              for item in dictitem[k]]
        #     scalartmp = [dictitem[k] for dictitem in listofdicts
        #                  if not isinstance(dictitem[k], list)]
        #     if len(dg[k]) == 0:
        #         dg[k] = scalartmp
        #
        # return dg
        if listofdicts:
            needs_flattening = [k for (k,v) in listofdicts[0].items() if isinstance(v, list)]
            d_of_l = {k: [dic[k] for dic in listofdicts] for k in listofdicts[0]}
            if needs_flattening:
                print('flattening {}'.format(needs_flattening))
                for nf in needs_flattening:
                    d_of_l[nf] = [item for sublist in d_of_l[nf] for item in sublist]
            return d_of_l
        else:
            return None

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

    def _initialize_sequential_read(self, start_ptr, end_ptr):
        """
        sequential_read_records gives you the ability to just read a chunk of a file, starting at start_ptr, ending
        at end_ptr.  This method sets up this functionality by figuring out the length of the chunk and the max length
        of the file.
        """
        self.eof = False
        if end_ptr:
            filelen = int(end_ptr - start_ptr)
        else:
            self.FID.seek(-start_ptr, 2)
            filelen = self.FID.tell()
        self.FID.seek(0, 2)
        self.file_size = self.FID.tell()
        self.FID.seek(start_ptr, 0)
        return filelen

    def _build_startbytesearch(self):
        """
        Build the regular expression we are going to use to find the next startbyte, if necessary.
        """
        # we search for the pound sign as a first step, use this compiled expression for the second tier, ensuring
        # the pound sign actually indicates the record identifier

        # went through and found the possible letters for all the records we care about
        # have to be explicit, as there are datagrams within datagrams, see read_EMdgmSKMinfo
        search_exp = b'#[CIMS][CDHIKOPRVWZ][CEILMOPTZ01]'
        compiled_expr = re.compile(search_exp)
        return compiled_expr

    def seek_next_startbyte(self, file_length, start_ptr=0):
        """
        Determines if current pointer is at the start of a record.  If not, finds the next valid one.
        """
        # check is to continue on until you find the pound sign, which might indicate the record identifier,
        #  can't just search for # though, have to use regex to ensure the 3 capital letter identifier comes after.
        at_the_right_byte = False
        while not at_the_right_byte:
            cur_ptr = self.FID.tell()
            if cur_ptr >= start_ptr + file_length:
                # at the end of file, return False to stop searching
                return False
            # consider start bytes right at the end of the given filelength as valid, even if they extend
            # over to the next chunk
            srchdat = self.FID.read(min(20, (start_ptr + file_length) - cur_ptr))
            stx_idx = srchdat.find(b'#')
            if stx_idx >= 0:
                possible_start = cur_ptr + stx_idx
                self.FID.seek(possible_start)
                datchk = self.FID.read(4)
                m = self.datagram_ident_search.search(datchk, 0)
                if m:
                    self.FID.seek(possible_start - 4)
                    return True

    def _divide_rec(self, rec):
        """
        MRZ comes in from sequential read by time/ping.  Each ping may have multiple sectors to it which we want
        to treat as separate pings.  Do this by generating a new record for each sector in the ping.  When rec is MRZ,
        the return is a list of rec split by sector.  Otherwise returns the original rec as the only element in a list
        returns: totalrecs, list of split rec
        """
        if self.datagram_ident != 'MRZ':
            return [rec]
        elif rec['pingInfo']['numTxSectors'] == 1:
            return [rec]
        else:
            totalrecs = []
            pingtime = rec['header']['dgtime']
            for sec in rec['txSectorInfo']['txSectorNumb']:
                split_rec = copy.copy(rec)
                split_rec['txSectorInfo'] = {k: v[sec] for (k,v) in rec['txSectorInfo'].items()}
                rx_index = np.where(np.array(rec['sounding']['txSectorNumb']) == sec)
                split_rec['sounding'] = {k: np.array(v)[rx_index] for (k,v) in rec['sounding'].items()}

                # ping time equals datagram time plus sector transmit delay
                split_rec['header']['dgtime'] = pingtime + split_rec['txSectorInfo']['sectorTransmitDelay_sec']

                totalrecs.append(split_rec)
            return totalrecs

    def _pad_to_dense(self, arr, padval=999.0, maxlen=500, override_type=None, detectioninfo=False):
        """
        Appends the minimal required amount of zeroes at the end of each array in the jagged array `M`, such that `M`
        loses its jaggedness.

        A required operation for our sector-wise read.  Each sector has a varying amount of beams over time, so the
        resulting number of values per ping (beam pointing angle for example) will differ between pings.  Here we make
        these ragged arrays square, by using the padval to fill in the holes.

        A padval of 999 is arbitrary, but we use that nodatavalue in kluster to reform pings and do processing, so
        leave at 999 for Kluster.  maxlen is the max number of expected beams per sector.
        returns: Z, square array padded with padval where arr is ragged
        """

        # override the dynamic length of beams across records by applying static length limit.
        # ideally this should cover all cases
        if override_type is not None:
            typ = override_type
        else:
            typ = arr[0].dtype

        Z = np.full((len(arr), maxlen), padval, dtype=typ)
        for enu, row in enumerate(arr):
            # some records being read have NaNs in them unexpectedly, like part of the record isn't being read
            row[np.isnan(row)] = 0
            if detectioninfo:
                Z[enu, :len(row)] = self.translate_detectioninfo(row)
            else:
                Z[enu, :len(row)] = row
        return Z

    def _finalize_records(self, recs_to_read, recs_count):
        """
        Take output from sequential_read_records and alter the type/size/translate as needed for Kluster to read and
        convert to xarray.  Major steps include
        - adding empty arrays so that concatenation later on will work
        - pad_to_dense to convert the ragged sector-wise arrays into square numpy arrays
        - translate the runtime parameters from integer/binary codes to string identifiers for easy reading (and to
             allow comparing results between different file types)
        returns: recs_to_read, dict of dicts finalized
        """
        # drop the delay array and txsector_beam array since we've already used it for adjusting ping time and building
        #    sector masks
        recs_to_read['ping'].pop('delay')
        recs_to_read['ping'].pop('txsector_beam')

        # need to force in the serial number, its not in the header anymore with these kmall files...
        if recs_to_read['installation_params']['installation_settings'] is not None:
            inst_params = recs_to_read['installation_params']['installation_settings'][0]
            if inst_params is not None:
                recs_to_read['installation_params']['serial_one'] = np.array([int(inst_params['pu_serial_number'])])
                # currently nothing in the record for identifying the second system in a dual head
                recs_to_read['installation_params']['serial_two'] = np.array([0])

        for rec in recs_to_read:
            for dgram in recs_to_read[rec]:
                if recs_count[rec] == 0:
                    if rec != 'runtime_params' or dgram == 'time':
                        # found no records, empty array
                        recs_to_read[rec][dgram] = np.zeros(0)
                    else:
                        # found no records, empty array of strings for the mode/stab records
                        recs_to_read[rec][dgram] = np.zeros(0, 'U2')
                elif rec == 'ping':
                    if dgram in ['beampointingangle', 'traveltime', 'qualityfactor_percent']:
                        # these datagrams can vary in number of beams, have to pad with 999 for 'jaggedness'
                        recs_to_read[rec][dgram] = self._pad_to_dense(recs_to_read[rec][dgram])
                    elif dgram in ['detectioninfo', 'qualityfactor']:
                        # same for detection info, but it also needs to be converted to something other than int8
                        recs_to_read[rec][dgram] = self._pad_to_dense(recs_to_read[rec][dgram], override_type=np.int)
                    elif dgram == 'yawandpitchstabilization':
                        recs_to_read[rec][dgram] = self.translate_yawpitch_tostring(np.array(recs_to_read[rec][dgram]))
                    elif dgram == 'mode':
                        recs_to_read[rec][dgram] = self.translate_mode_tostring(np.array(recs_to_read[rec][dgram]))
                    elif dgram == 'modetwo':
                        recs_to_read[rec][dgram] = self.translate_mode_two_tostring(np.array(recs_to_read[rec][dgram]))
                    else:
                        recs_to_read[rec][dgram] = np.array(recs_to_read[rec][dgram])
                elif rec in ['navigation', 'attitude']:  # these recs have time blocks of data in them, need to be concatenated
                    recs_to_read[rec][dgram] = np.concatenate(recs_to_read[rec][dgram])
                else:
                    recs_to_read[rec][dgram] = np.array(recs_to_read[rec][dgram])
        return recs_to_read

    def sequential_read_records(self, start_ptr=0, end_ptr=0, first_installation_rec=False):
        """
        Read the file and return a dict of the wanted records/fields according to recs_categories.  If start_ptr/end_ptr
        is provided, start and end at those byte offsets.

        returns: recs_to_read, dict of dicts for each desired record read sequentially, see recs_categories
        """
        wanted_records = list(recs_categories.keys())
        recs_to_read = copy.deepcopy(recs_categories_result)
        recs_count = dict([(k, 0) for k in recs_to_read])

        if self.FID is None:
            self.OpenFiletoRead()

        filelen = self._initialize_sequential_read(start_ptr, end_ptr)
        if start_ptr:
            self.seek_next_startbyte(filelen, start_ptr=start_ptr)

        while not self.eof:
            if self.FID.tell() >= start_ptr + filelen:
                self.eof = True
                break
            self.decode_datagram()
            if self.datagram_ident not in wanted_records:
                self.skip_datagram()
                continue
            self.read_datagram()
            for rec_ident in list(recs_categories_translator[self.datagram_ident].values())[0]:
                recs_count[rec_ident[0]] += 1

            rec = self.datagram_data
            recs = self._divide_rec(rec)  # split up the MRZ record for multiple sectors, otherwise just returns [rec]
            for rec in recs:
                for subrec in recs_categories[self.datagram_ident]:
                    #  override for nested recs, designated with periods in the recs_to_read dict
                    if subrec.find('.') > 0:
                        if len(subrec.split('.')) == 3:
                            rec_key = subrec.split('.')[2]
                            tmprec = rec[subrec.split('.')[0]][subrec.split('.')[1]][rec_key]
                        else:
                            rec_key = subrec.split('.')[1]
                            tmprec = rec[subrec.split('.')[0]][rec_key]
                    else:
                        rec_key = subrec
                        tmprec = rec[rec_key]

                    if subrec in ['install_txt', 'runtime_txt']:  # str, casting to list splits the string, dont want that
                        val = [tmprec]
                    else:
                        try:  # flow for array/list attribute
                            val = [np.array(tmprec)]
                        except TypeError:  # flow for float/int attribute
                            val = [tmprec]

                    # generate new list or append to list for each rec of that dgram type found
                    for translated in recs_categories_translator[self.datagram_ident][subrec]:
                        if recs_to_read[translated[0]][translated[1]] is None:
                            recs_to_read[translated[0]][translated[1]] = copy.copy(val)
                        else:
                            recs_to_read[translated[0]][translated[1]].extend(val)
            if self.datagram_ident == 'IIP' and first_installation_rec:
                self.eof = True
        recs_to_read = self._finalize_records(recs_to_read, recs_count)
        return recs_to_read

    def translate_yawpitch_tostring(self, arr):
        """
        Translate the binary code to a string identifier. Allows user to understand the mode
        without translating the integer code in their head.  Kluster will build plots using these string identifiers
        in the legend.

        'yawpitchstabilization' = 'Y' for Yaw stab, 'P' for pitch stab, 'PY' for both, 'N' for neither
        # xxxxxxx0 no pitch stab, xxxxxxx1 pitch stab
        # xxxxxx0x no yaw stab, xxxxxx1x yaw stab

        returns: rslt, numpy array of strings containing the translated yawpitch values
        """
        rslt = np.full(arr.shape, 'N', dtype='U2')
        first_bit_chk = np.bitwise_and(arr, (1 << 0)).astype(bool)
        sec_bit_chk = np.bitwise_and(arr, (1 << 1)).astype(bool)

        rslt[np.intersect1d(np.where(first_bit_chk), np.where(sec_bit_chk))] = 'PY'
        rslt[np.intersect1d(np.where(first_bit_chk), np.where(sec_bit_chk == False))] = 'P'
        rslt[np.intersect1d(np.where(first_bit_chk == False), np.where(sec_bit_chk))] = 'Y'
        return rslt

    def translate_mode_tostring(self, arr):
        """
        Translate the binary code to a string identifier (for MRZ pulseForm).  Allows user to understand the mode
        without translating the integer code in their head.  Kluster will build plots using these string identifiers
        in the legend.

        'mode' = 'CW' for continuous waveform, 'FM' for frequency modulated, 'MIX' for both
        0 for CW, 1 for MIX, 2 for FM

        returns: rslt, numpy array of strings containing the translated mode values
        """
        rslt = np.full(arr.shape, 'MIX', dtype='U3')

        rslt[np.where(arr == 0)] = 'CW'
        rslt[np.where(arr == 1)] = 'MIX'
        rslt[np.where(arr == 2)] = 'FM'

        return rslt

    def translate_mode_two_tostring(self, arr):
        """
        Translate the binary code to a string identifier (for MRZ depthMode).  Allows user to understand the mode
        without translating the integer code in their head.  Kluster will build plots using these string identifiers
        in the legend.

        0 = VS, 1 = SH, 2 = ME, 3 = DE, 4 = DR, 5 = VD, 6 = ED, 7 = XD

        if mode is manually selected, there will be an 'm' in front (ex: VSm)

        returns: rslt, numpy array of strings containing the translated mode_two values
        """
        rslt = np.zeros(arr.shape, dtype='U3')

        rslt[np.where(arr == 7)] = 'XD'
        rslt[np.where(arr == 6)] = 'ED'
        rslt[np.where(arr == 5)] = 'VD'
        rslt[np.where(arr == 4)] = 'DR'
        rslt[np.where(arr == 3)] = 'DE'
        rslt[np.where(arr == 2)] = 'ME'
        rslt[np.where(arr == 1)] = 'SH'
        rslt[np.where(arr == 0)] = 'VS'

        rslt[np.where(arr == 107)] = 'XDm'
        rslt[np.where(arr == 106)] = 'EDm'
        rslt[np.where(arr == 105)] = 'VDm'
        rslt[np.where(arr == 104)] = 'DRm'
        rslt[np.where(arr == 103)] = 'DEm'
        rslt[np.where(arr == 102)] = 'MEm'
        rslt[np.where(arr == 101)] = 'SHm'
        rslt[np.where(arr == 100)] = 'VSm'

        return rslt

    def translate_runtime_parameters_todict(self, r_text):
        """
        runtime parameters text comes from file as a string with carriage retuns between entries.

        ex: '"\\nSector coverage\\nMax angle Port:      70.0\\nMax angle Starboard: 70.0\\nMax coverage Port:  ..."'

        we want a dictionary of key: value pairs so we can save them as an xarray attribute and read them as a dict
        whenever we need to access.  Also, we translate the keys to something more human readable.  The translated
        key names will match up with .all files read with par module as well, so there is some cross compatibility (useful
        for Kluster multibeam processing)

        ex:

        returns: translated, dict of translated runtime parameters and values
        """
        translated = {}
        entries = r_text.split('\n')
        for entry in entries:
            if entry and (entry.find(':') != -1):  # valid entries look like 'key: value', the rest are headers or blank
                key, value = entry.split(':')
                translated[key] = value.lstrip().rstrip()
        return translated

    def translate_installation_parameters_todict(self, i_text):
        """
        installation parameters text comes from file as a comma delimited string with mix of = and ; separating the
        key/value pairs

        ex: 'SCV:Empty,EMXV:EM2040P,\nPU_0,\nSN=53011,\nIP=157.237.20.40:0xffff0000,\nUDP=1997,...'

        we want a dictionary of key: value pairs so we can save them as an xarray attribute and read them as a dict
        whenever we need to access.  Also, we translate the keys to something more human readable.  The translated
        key names will match up with .all files read with par module as well, so there is some cross compatibility (useful
        for Kluster multibeam processing)

        ex: {"operator_controller_version": "Empty", "multibeam_system": "EM2040P", "pu_id_type": "0",
             "pu_serial_number": "53011", "ip_address_subnet_mask": "157.237.20.40:0xffff0000",
             "command_tcpip_port": "1997",...}

        returns: translated, dict of translated installation parameters and values
        """
        translate_install = {'SCV:': 'operator_controller_version', 'EMXV:': 'sonar_model_number', 'PU_': 'pu_id_type',
                             'SN=': 'pu_serial_number', 'IP=': 'ip_address_subnet_mask', 'UDP=': 'command_tcpip_port',
                             'TYPE=': 'cpu_type', 'DCL:': 'dcl_version', 'KMALL:': 'kmall_version',
                             'SYSTEM:': 'system_description', 'EMXI:SWLZ=': 'waterline_vertical_location'}
        translate_versions = {'CPU:': 'cpu_software_version', 'VXW:': 'vxw_software_version',
                              'FILTER:': 'filter_software_version', 'CBMF:': 'cbmf_software_version',
                              'TX:': 'tx_software_version', 'RX:': 'rx_software_version'}
        translate_serial = {'TX:': 'tx_serial_number', 'RX:': 'rx_serial_number'}
        # device translator will use the device identifier plus the values here, ex: 'TRAI_HD1' + '_serial_number'
        translate_device_ident = {'ATTI_1': 'motion_sensor_1', 'ATTI_2': 'motion_sensor_2', 'ATTI_3': 'motion_sensor_3',
                                  'POSI_1': 'position_1', 'POSI_2': 'position_2', 'POSI_3': 'position_3',
                                  'CLCK': 'clock', 'SVPI': 'sound_velocity_1', 'TRAI_HD1': 'transducer_1'}
        translate_device = {'N=': '_serial_number', 'X=': '_along_location', 'Y=': '_athwart_location',
                            'Z=': '_vertical_location', 'R=': '_roll_angle', 'P=': '_pitch_angle',
                            'H=': '_heading_angle', 'S=': '_sounder_size_deg',
                            'V=': '_version', 'W=': '_system_description', 'IPX=': '_port_sector_forward',
                            'IPY=': '_port_sector_starboard', 'IPZ=': '_port_sector_down',
                            'ICX=': '_center_sector_forward', 'ICY=': '_center_sector_starboard',
                            'ICZ=': '_center_sector_down', 'ISX=': '_starboard_sector_forward',
                            'ISY=': '_starboard_sector_starboard', 'ISZ=': '_starboard_sector_down',
                            'ITX=': '_tx_forward', 'ITY=': '_tx_starboard', 'ITZ=': '_tx_down',
                            'IRX=': '_rx_forward', 'IRY=': '_rx_starboard', 'IRZ=': '_rx_down', 'D=': '_time_delay',
                            'G=': '_datum', 'T=': '_time_stamp', 'C=': '_motion_compensation', 'F=': '_data_format',
                            'Q=': '_quality_check', 'I=': '_input_source', 'U=': '_active_passive',
                            'M=': 'motion_reference', 'A=': '_1pps'}

        # split by comma delimited groups
        records = [i_text.split(',') for i_text in i_text.split('\n')]
        # subgroups are semicolon delimited
        # ex: TRAI_HD1:N=218;X=-0.293;Y=0.000;Z=0.861;R=0.496...
        records_flatten = [r.split(';') for rec in records for r in rec if r]

        translated = {}
        translate = translate_install
        for rec in records_flatten:
            # subgroups are parsed here, first rec contains the prefix
            # ex: ['ATTI_1:X=0.000', 'Y=0.000', 'Z=0.000', 'R=0.000', 'P=0.000', 'H=0.000', 'D=0.000'...
            if len(rec) > 1:
                prefix, first_rec = rec[0].split(':')
                try:
                    prefix = translate_device_ident[prefix]  # if its a prefix we haven't seen before, just pass it through
                except:
                    pass
                ky, data = first_rec.split('=')
                translated[prefix + translate_device[ky + '=']] = data
                for subrec in rec[1:]:
                    ky, data = subrec.split('=')
                    translated[prefix + translate_device[ky + '=']] = data
            # regular groups parsed here, use the headers to determine which translator to use
            # ex:  ['CBMF:1.11 18.02.20 ']
            else:
                if rec[0] == 'VERSIONS:':
                    translate = translate_versions
                    continue
                elif rec[0] == 'SERIALno:':
                    translate = translate_serial
                    continue
                elif rec[0] in ['VERSIONS-END', 'SERIALno-END']:
                    translate = translate_install
                    continue
                elif rec[0][-7:] == 'NOT_SET':
                    continue

                key = [trans_key for trans_key in translate if rec[0].find(trans_key) != -1]
                if len(key) == 0:
                    print('Unable to parse {}'.format(rec))
                elif len(key) == 1:
                    translated[translate[key[0]]] = rec[0][len(key[0]):].rstrip()
                else:
                    raise ValueError('Found multiple entries valid for record {}:{}'.format(rec, key))

        # plug in new keys for active position/motion sensor needed for kluster to identify the right sensor
        for mot_sens in ['motion_sensor_1_active_passive', 'motion_sensor_2_active_passive',
                         'motion_sensor_3_active_passive']:
            if mot_sens in translated:
                if translated[mot_sens] == 'ACTIVE':
                    translated['active_heading_sensor'] = 'motion_' + mot_sens[14]  # 'motion_1' in most cases
        for pos_sens in ['position_1_active_passive', 'position_2_active_passive', 'position_3_active_passive']:
            if pos_sens in translated:
                if translated[pos_sens] == 'ACTIVE':
                    translated['active_position_system_number'] = 'position_' + pos_sens[9]  # 'position_1'
        return translated


    def fast_read_start_end_time(self):
        """
        Get the start and end time for the file without mapping the file
        returns: list, [UTC start time in seconds, UTC end time in seconds]
        """
        self.datagram_data = None
        self.eof = False

        if self.FID is None:
            self.OpenFiletoRead()
        else:
            self.FID.seek(0)

        start_time = None
        end_time = None

        while not self.eof:
            self.decode_datagram()
            self.read_datagram()
            try:
                start_time = self.datagram_data['header']['dgtime']
                break
            except:
                continue

        # pick 10k of reading just to make sure you get some valid records, or the filelength if it is less than that
        self.FID.seek(0)
        chunksize = min(10 * 1024, self.FID.tell())
        self.FID.seek(-chunksize, 2)
        self.seek_next_startbyte(chunksize, self.FID.tell())
        while not self.eof:
            self.decode_datagram()
            self.read_datagram()
            try:
                end_time = self.datagram_data['header']['dgtime']
                break
            except:
                continue
        return [start_time, end_time]


if __name__ == '__main__':
    # Handle input arguments
    parser = argparse.ArgumentParser(description="A python script (and class) "
                                                 "for parsing Kongsberg KMALL "
                                                 "data files.")
    parser.add_argument('-f', action='store', dest='kmall_filename',
                        help="The path and filename to parse.")
    parser.add_argument('-d', action='store', dest='kmall_directory',
                        help="A directory containing kmall data files to parse.")
    parser.add_argument('-V', action='store_true', dest='verify',
                        default=False, help="Perform series of checks to verify the kmall file.")
    parser.add_argument('-z', action='store_true', dest='compress',
                        default=False, help="Create a compressed (somewhat lossy) version of the file. See -l")
    parser.add_argument('-l', action='store', type=int, dest='compressionLevel',
                        default=0, help=("Set the compression level (Default: 0).\n" +
                                         "\t 0: Somewhat lossy compression of soundings and imagery data.(Default)\n" +
                                         "\t 1: Somewhat lossy compression of soundings with imagery omitted."))
    parser.add_argument('-Z', action='store_true', dest='decompress',
                        default=False, help=("Decompress a file compressed with this library. " +
                                             "Files must end in .Lz, where L is an integer indicating " +
                                             "the compression level (set by -l when compresssing)"))

    parser.add_argument('-v', action='count', dest='verbose', default=0,
                        help="Increasingly verbose output (e.g. -v -vv -vvv),"
                             "for debugging use -vvv")
    args = parser.parse_args()

    verbose = args.verbose

    kmall_filename = args.kmall_filename
    kmall_directory = args.kmall_directory
    verify = args.verify
    compress = args.compress
    decompress = args.decompress
    compressionLevel = args.compressionLevel

    validCompressionLevels = [0, 1]
    if compressionLevel not in validCompressionLevels:
        print("Error: Compression level may be one of " + str(validCompressionLevels))
        sys.exit()

    suffix = "kmall"
    if decompress:
        suffix

    if kmall_directory:
        filestoprocess = []

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

        # Index file (check for index)
        K.index_file()

        ## Do packet verification if requested.
        pingcheckdata = []
        navcheckdata = []
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

        ## Do compression if desired, at the desired level.
        if compress:

            if compressionLevel == 0:

                print("Compressing soundings and imagery.")
                compressedFilename = K.filename + ".0z"

                # Modify filename if the file already exists
                idx = 1
                while os.path.exists(compressedFilename):
                    compressedFilename = ((K.filename + "_" + "%02d.0z") % idx)
                    idx += 1

                T = kmall(compressedFilename)
                K.index_file()
                T.OpenFiletoWrite()

                for offset, size, mtype in zip(K.Index['ByteOffset'],
                                               K.Index['MessageSize'],
                                               K.Index['MessageType']):
                    K.FID.seek(offset, 0)
                    if mtype == "b'#MRZ'":
                        dg = K.read_EMdgmMRZ()
                        T.write_EMdgmCZ0(dg)
                    else:
                        buffer = K.FID.read(size)
                        T.FID.write(buffer)

                K.closeFile()
                T.closeFile()

            if compressionLevel == 1:

                print("Compressing soundings, omitting imagery.")
                compressedFilename = K.filename + ".1z"

                # Modify filename if the file already exists
                idx = 1
                while os.path.exists(compressedFilename):
                    compressedFilename = compressedFilename + "_" + str(idx)

                T = kmall(compressedFilename)
                K.index_file()
                T.OpenFiletoWrite()

                for offset, size, mtype in zip(K.Index['ByteOffset'],
                                               K.Index['MessageSize'],
                                               K.Index['MessageType']):
                    K.FID.seek(offset, 0)
                    if mtype == "b'#MRZ'":
                        dg = K.read_EMdgmMRZ()
                        T.write_EMdgmCZ1(dg)
                    else:
                        buffer = K.FID.read(size)
                        T.FID.write(buffer)

                K.closeFile()
                T.closeFile()

        # Decompress the file is requested.
        if decompress:

            # Discern the compression level and base filename.
            regexp = '(?P<basename>.*\.kmall)\.(?P<level>\d+)z'
            tokens = re.search(regexp, K.filename)
            if tokens is None:
                print("Could not discern compression level.")
                print("Expecting xxxxx.kmall.\d+.z, where \d+ is 1 or more")
                print("integers indicating the compression level.")
                sys.exit()

            fileBasename = tokens['basename']
            compressionLevel = tokens['level']

            # Give some status.
            if compressionLevel == "0":
                print("Decompressing soundings and imagery.(Level: 0)")
            elif compressionLevel == "1":
                print("Decompessing soundings, imagery was omitted in this format. (Level: 1)")

            decompressedFilename = fileBasename
            # Check to see if decompressed filename exists and modify if necessary.
            idx = 1
            while os.path.exists(decompressedFilename):
                decompressedFilename = ((fileBasename[:-6] +
                                         "_" + "%02d" + '.kmall') % idx)
                idx += 1

            if verbose >= 1:
                print("Decompressing to: %s" % decompressedFilename)
                print("Decompressing from Level: %s" % compressionLevel)

            # Create kmall object for decompressed file and open it.
            T = kmall(filename=decompressedFilename)
            T.OpenFiletoWrite()

            # Loop through the file, decompressing datagrams
            # when necessary and just writing them when not.
            for offset, size, mtype in zip(K.Index['ByteOffset'],
                                           K.Index['MessageSize'],
                                           K.Index['MessageType']):
                K.FID.seek(offset, 0)
                if compressionLevel == "0":

                    if mtype == "b'#CZ0'":
                        dg = K.read_EMdgmCZ0()
                        T.write_EMdgmMRZ(dg)
                    else:
                        buffer = K.FID.read(size)
                        T.FID.write(buffer)

                if compressionLevel == "1":

                    if mtype == "b'#CZ1'":
                        dg = K.read_EMdgmCZ1()
                        T.write_EMdgmMRZ(dg)
                    else:
                        buffer = K.FID.read(size)
                        T.FID.write(buffer)

            T.closeFile()
            K.closeFile()
