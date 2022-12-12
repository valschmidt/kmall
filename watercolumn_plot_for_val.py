#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Extract and plot nadir watercolumn beams.
# When run as main, defaults to select 10% swath width for nadir curtain averaging.
# Can specify percent swath width, number of nadir beams, or across-track width to include in curtain averaging.

import copy
import getopt
import KMALL
import logging
import math
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import make_axes_locatable, axes_size
import numpy as np
import os
import pandas as pd
import sys

# sys.path.append("../PycharmProjects")

# __name__ is module's name
logger = logging.getLogger(__name__)


class WaterColumnPlot:

    def __init__(self, input_file_or_directory=None, write_directory=None, swath_width_percent=None, swath_width_m=None,
                 num_nadir_beams=None, along_track_swaths=None, bin_height_percent=None, bin_height_m=None,
                  max_nav_gap_sec=0.04, colorbar_min=None, colorbar_max=None):

        self.files = self.create_file_list(input_file_or_directory)
        self.write_directory = write_directory

        # Priority order for across-track averaging:
        if swath_width_percent is not None:
            self.swath_width_percent = swath_width_percent
            self.swath_width_m = None
            self.num_nadir_beams = None
        elif swath_width_m is not None:
            self.swath_width_percent = None
            self.swath_width_m = swath_width_m
            self.num_nadir_beams = None
        elif num_nadir_beams is not None:
            self.swath_width_percent = None
            self.swath_width_m = None
            self.num_nadir_beams = num_nadir_beams
        else:
            # TODO: Determine if 10% swath width is appropriate default.
            self.swath_width_percent = 10
            self.swath_width_m = None
            self.num_nadir_beams = None

        # Number of pings along-track to average (EX: Enter 4 to average 4 pings.)
        self.along_track_swaths = along_track_swaths

        # Priority order for binning:
        if bin_height_percent is not None:
            self.bin_height_percent = bin_height_percent
            self.bin_height_m = None
        elif bin_height_m is not None:
            self.bin_height_percent = None
            self.bin_height_m = bin_height_m
        else:
            # TODO: Determine if 1% swath width is appropriate default.
            self.bin_height_percent = 1
            self.bin_height_m = None

        # Maximum acceptable gap in navigation data to interpolate through. 0.04 seconds (25 Hz) by default.
        self.max_nav_gap_sec = max_nav_gap_sec

        # Custom colorbar range for plots:
        self.colorbar_min = colorbar_min
        self.colorbar_max = colorbar_max

    # Create file list if input path is directory
    def create_file_list(self, input_file):
        temp_list = []

        if os.path.isfile(input_file):
            if input_file.lower().endswith(('.kmall', '.kmwcd')):
                temp_list.append(input_file)
        elif os.path.isdir(input_file):
            for root, dirs, files in os.walk(input_file):
                for filename in files:
                    if filename.lower().endswith(('.kmall', '.kmwcd')):
                        temp_list.append(os.path.join(root, filename))

        if len(temp_list) > 0:
            return temp_list
        else:
            logger.warning("Invalid file path: %s" % input_file)
            sys.exit(1)

    # Similar to .index_file function of KMALL, this function indexes KMB (attitude) datagrams (which are housed within
    # SKM datagrams). Returns a pandas dataframe, indexed / sorted by timestamp and containing ByteOffset, MessageSize,
    # and MessageType fields for all KMB datagrams in a file.
    def index_KMB(self, k, SKM_offsets):
        time = []
        byte_offset = []
        message_size = []
        message_type = []

        for offset in SKM_offsets:
            # SKM datagram consists of:
            # - EMdgmHeader: header
            # - EMdgmSKMinfo: infoPart
            # - EMdgmSKMsample: sample[]
            # - KMbinary: KMdefault
            # - KMdelayedHeave: delayedHeave

            k.FID.seek(offset, 0)
            dg_SKM_header = k.read_EMdgmHeader()
            dg_SKM_infoPart = k.read_EMdgmSKMinfo()

            # Find position of file pointer:
            byte_offset_SKMsample = k.FID.tell()  # Start of EMdgmSKMsample

            dg_SKM_sample = k.read_EMdgmSKMsample(dg_SKM_infoPart)

            num_samples_array = dg_SKM_infoPart['numSamplesArray']
            num_bytes_per_sample = dg_SKM_infoPart['numBytesPerSample']

            for i in range(num_samples_array):
                time_KMB = (dg_SKM_sample['KMdefault']['time_sec'][i] +
                            (dg_SKM_sample['KMdefault']['time_nanosec'][i] / 1.0E9))
                offset_KMB = byte_offset_SKMsample + (i * num_bytes_per_sample)
                size_KMB = dg_SKM_sample['KMdefault']['numBytesDgm'][i]
                type_KMB = dg_SKM_sample['KMdefault']['dgmType'][i].strip("'b")

                time.append(time_KMB)
                byte_offset.append(offset_KMB)
                message_size.append(size_KMB)
                message_type.append(type_KMB)

        index_KMB = pd.DataFrame({'Time': time,
                                  'ByteOffset': byte_offset,
                                  'MessageSize': message_size,
                                  'MessageType': message_type})

        index_KMB.set_index('Time', inplace=True)
        index_KMB['MessageType'] = index_KMB.MessageType.astype('category')
        index_KMB.sort_index(inplace=True)

        return index_KMB

    def sort_dataframe_by_index(self, df, datagram_type=None):
        """
        Sort pandas dataframe by index value; optionally filter by datagram type.
        :param df: Pandas dataframe.
        :param datagram_type: Kongsberg datagram type. EX: #MRZ, #MWC...
        :return: Pandas dataframe sorted by index value; optionally filtered by datagram type.
        """
        if datagram_type is None:
            return df.sort_index()
        else:
            filtered_df = df[df['MessageType'].map(lambda x: x.strip("'b")) == datagram_type]
            return filtered_df.sort_index()

    def interpolate_position_attitude(self, k, sorted_df_KMB, time_MWC):
        """
        Interpolates (linear) position and attitude at time of MWC datagram.
        :param time_MWC: UTC (epoch 1970-01-01) time in seconds.
        :param sorted_df_KMB: Pandas dataframe containing only KMB datagrams, sorted by timestamp (index).
        :param: k: instance of class kmall with indexed file
        :return: returns None if navigation time gap exceeds maximum allowable value; otherwise returns
        Python dictionary with interpolated position and attitude data
        """
        # First, ensure that time_MWC does not exceed greatest timestamp value of KMB datagrams:
        if time_MWC > sorted_df_KMB.index[-1]:
            print("WARNING: MWC (water column) timestamp exceeds KMB (attitude) timestamp!")
            return None

        # Find corresponding SKM / KMB datagrams and interpolate position based on MWC timestamp:
        # NOTE: searchsorted returns index of element that should be *after* argument:
        # [..., [(index - 1)], (argument), [(index)], ...]
        index = sorted_df_KMB.index.searchsorted(time_MWC)

        byte_offset1 = sorted_df_KMB.iloc[(index - 1), 0]
        byte_offset2 = sorted_df_KMB.iloc[index, 0]

        k.FID.seek(byte_offset1, 0)  # Find start of 1st KMB datagram
        dg_KMB1 = k.read_KMbinary()  # Read 1st KMB datagram

        k.FID.seek(byte_offset2, 0)  # Find start of 2nd KMB datagram
        dg_KMB2 = k.read_KMbinary()  # Read 2nd KMB datagram

        time1 = dg_KMB1['dgtime']
        time2 = dg_KMB2['dgtime']

        # Check that interval between KMB records does not exceed maximum allowable gap for interpolation.
        if (time2 - time1) > self.max_nav_gap_sec:
            print("ERROR: Navigation time step too large to interpolate through!")
            return None


        # Interpolate position:
        lat1 = dg_KMB1['latitude_deg']
        lat2 = dg_KMB2['latitude_deg']
        lat3_interp = self.__linear_interpolate(time1, lat1, time2, lat2, time_MWC)

        lon1 = dg_KMB1['longitude_deg']
        lon2 = dg_KMB2['longitude_deg']
        lon3_interp = self.__linear_interpolate(time1, lon1, time2, lon2, time_MWC)

        elip_height1 = dg_KMB1['ellipsoidHeight_m']
        elip_height2 = dg_KMB2['ellipsoidHeight_m']
        elip_height3_interp = self.__linear_interpolate(time1, elip_height1, time2, elip_height2, time_MWC)

        position = {"latitude_deg": lat3_interp,
                    "longitude_deg": lon3_interp,
                    "ellipsoidHeight_m": elip_height3_interp}

        # Interpolate attitude:
        roll1 = dg_KMB1['roll_deg']
        roll2 = dg_KMB2['roll_deg']
        roll3_interp = self.__linear_interpolate(time1, roll1, time2, roll2, time_MWC)

        pitch1 = dg_KMB1['pitch_deg']
        pitch2 = dg_KMB2['pitch_deg']
        pitch3_interp = self.__linear_interpolate(time1, pitch1, time2, pitch2, time_MWC)

        heading1 = dg_KMB1['heading_deg']
        heading2 = dg_KMB2['heading_deg']
        heading3_interp = self.__linear_interpolate(time1, heading1, time2, heading2, time_MWC)

        heave1 = dg_KMB1['heave_m']
        heave2 = dg_KMB2['heave_m']
        heave3_interp = self.__linear_interpolate(time1, heave1, time2, heave2, time_MWC)

        attitude = {"roll_deg": roll3_interp,
                    "pitch_deg": pitch3_interp,
                    "heading_deg": heading3_interp,
                    "heave_m": heave3_interp}

        position_and_attitude = {"postion": position,
                                 "attitude": attitude}

        return position_and_attitude

    def __linear_interpolate(self, x1, y1, x2, y2, x3):
        """
        Linear interpolation. x values usually position or attitude; y values usually time.
        """
        y3 = y1 + ((x3 - x1) / (x2 - x1)) * (y2 - y1)
        return y3

    def calculate_bin_height_m(self, avg_nadir_depth):
        if self.bin_height_percent is not None:
            return (self.bin_height_percent / 100) * avg_nadir_depth
        return None

    def calculate_swath_width_m(self, swath_width):
        if self.swath_width_percent is not None:
            return (self.swath_width_percent / 100) * swath_width
        return None

    def bin_samples(self, dg_MWC, interp_position_attitude):
        # TODO: Find a better way to do this...
        # Matrices to be initialized at 120% of the number of expected bins based on bin_height_percent
        # Matrix to hold sample amplitude data for each watercolumn data point; binned by depth
        binned_sample_array = [[] for value in range(int(120 / self.bin_height_percent))]
        # For testing / debugging: Keep track of number of data points in each bin.
        binned_count_array = [0 for value in range(int(120 / self.bin_height_percent))]

        # For every beam in ping:
        for beam in range(dg_MWC['rxInfo']['numBeams']):  # 0 to self.numBeams - 1
            # Across-track angle:
            beam_point_angle_re_vertical = dg_MWC['beamData']['beamPointAngReVertical_deg'][beam]
            # Along-track angle:
            tilt_angle_re_tx_deg = dg_MWC['sectorData']['tiltAngleReTx_deg']  # List; one entry per sector
            tilt_angle_re_vertical_deg = (tilt_angle_re_tx_deg[dg_MWC['beamData']['beamTxSectorNum'][beam]] -
                                         interp_position_attitude['attitude']['pitch_deg'])

            # Index in sampleAmplitude05dB array where bottom detected
            detected_range = dg_MWC['beamData']['detectedRangeInSamples'][beam]

            # For each watercolumn data point in a single beam:
            for i in range(detected_range + 1):  # 0 to detected_range
                range_to_wc_data_point = ((dg_MWC['rxInfo']['soundVelocity_mPerSec'] * i) /
                                          (dg_MWC['rxInfo']['sampleFreq_Hz'] * 2))

                #       |         / \
                #       |       /     \
                #     y |     /         \
                #       |   /   (swath)   \
                #       |_/_________________\ _
                #                  x
                # TODO: Double-check this math:
                # Across-track distance
                x = (range_to_wc_data_point * math.sin(math.radians(beam_point_angle_re_vertical))
                     * math.cos((math.radians(tilt_angle_re_vertical_deg))))
                # Depth
                # y = (range_to_wc_data_point * math.cos(math.radians(beam_point_angle_re_vertical))
                #      * math.cos(math.radians(tilt_angle_re_vertical_deg))) - self.heave

                # If across-track distance falls within specified curtain width:
                if (-self.swath_width_m / 2) < x < (self.swath_width_m / 2):
                    # TODO: Double-check this math:
                    # Calculate depth
                    y = (range_to_wc_data_point * math.cos(math.radians(beam_point_angle_re_vertical))
                         * math.cos(math.radians(tilt_angle_re_vertical_deg))) - dg_MWC['txInfo']['heave_m']

                    # Determine bin index based on depth:
                    bin_index = max(0, (math.floor(y / self.bin_height_m) - 1))

                    # Place watercolumn data point in appropriate bin:
                    # NOTE: Kongsberg: "sampleAmplitude05dB_p: Sample amplitudes in 0.5 dB resolution."
                    # NOTE: Kongsberg: "TVGoffset_dB: Time Varying Gain offset used (OFS), unit dB.
                    # X log R + 2 Alpha R + OFS + C, where X and C is documented in #MWC datagram.
                    # OFS is gain offset to compensate for TX source level, receiver sensitivity etc."
                    binned_sample_array[bin_index].append((dg_MWC['beamData']['sampleAmplitude05dB_p'][beam][i] * 0.5)
                                                          - dg_MWC['rxInfo']['TVGoffset_dB'])
                    # For testing / debugging: Keep track of number of data points in each bin.
                    binned_count_array[bin_index] += 1

        return binned_sample_array, binned_count_array

    def extract_and_plot_wc_from_file(self):
        # TODO: Handle .kmwcd files!

        for fp in self.files:
            k = KMALL.kmall(fp)
            k.index_file()

            # Extract only #MWC and #SKM datagrams from k.Index and sort by timestamp:
            sorted_df_MWC = self.sort_dataframe_by_index(k.Index, "#MWC")
            sorted_df_SKM = self.sort_dataframe_by_index(k.Index, "#SKM")
            sorted_df_IIP = self.sort_dataframe_by_index(k.Index, "#IIP")

            # Extract #MWC and #SKM file offsets:
            MWCOffsets = sorted_df_MWC['ByteOffset'].tolist()
            SKMOffsets = sorted_df_SKM['ByteOffset'].tolist()
            IIPOffsets = sorted_df_IIP['ByteOffset'].tolist()

            sorted_df_KMB = self.index_KMB(k, SKMOffsets)

            # If file does not contain MWC data, continue to next:
            if len(MWCOffsets) is 0:
                continue

            z_offset_sonar = None
            if len(IIPOffsets) > 0:
                k.FID.seek(IIPOffsets[-1], 0)
                dg_IIP = k.read_EMdgmIIP(True)
                z_offset_sonar = float(dg_IIP['install_txt']['transducer_1_vertical_location'])

            # TODO: For future use:
            # counter_one_sec = None
            # avg_watercolumn_array_one_sec = []
            # samples_per_bin_array_one_sec = []

            avg_watercolumn_array_full = []
            samples_per_bin_array_full = []


            # TODO: FOR TESTING: (Use only a subset of MWC records:)
            # for offset in MWCOffsets[0:4]:  # Only two pings to evaluate dual swath
            # for offset in MWCOffsets[0:(int((len(MWCOffsets) / 2)))]:  # Each MWCOffset corresponds to a ping

            for offset in MWCOffsets:  # For each MWC datagram:
                k.FID.seek(offset, 0)  # Find start of MWC datagram
                self.dg_MWC = k.read_EMdgmMWC()  # Read MWC datagram

                time_MWC = self.dg_MWC['header']['dgtime']

                # TODO: For future use:
                # if counter_one_sec is None:
                #     counter_one_sec = time_MWC

                # Find corresponding SKM / KMB datagrams and interpolate position based on MWC timestamp:
                interpolated_position_attitude = self.interpolate_position_attitude(k, sorted_df_KMB, time_MWC)

                if interpolated_position_attitude is None:
                    # Navigation gap is too large to interpolate through
                    # Skip this ping and continue
                    continue

                basic_swath_stats = self.basic_swath_stats(self.dg_MWC, interpolated_position_attitude)

                # TODO: This method of determining swath_width_m and bin_height_m will use only the first ping of the
                #  file to calculate the value. Maybe determine a way to make this variable if swath width / depth
                #  changes dramatically?
                # Calculate swath_width_m if not already given
                # if self.swath_width_m is None:
                #     self.swath_width_m = self.calculate_swath_width_m(basic_swath_stats['swath_width'])

                # Calculate bin height based on percent of average nadir depth; this will be bin height for entire file.
                if self.bin_height_m is None:
                    self.bin_height_m = self.calculate_bin_height_m(basic_swath_stats['avg_nadir_depth'])

                binned_sample_array = []
                binned_count_array = []

                # Priority for across-track curtain width:
                # 1. Percentage of total swath width  # NOTE: This can be variable per ping!
                # 2. Across-track width (meters) # NOTE: Constant width per ping.
                # 3. Number of nadir beams  # NOTE: Constant value per ping.
                if self.swath_width_percent is not None:
                    # TODO: This method re-calculates swath_width_m for each ping.
                    self.swath_width_m = self.calculate_swath_width_m(basic_swath_stats['swath_width'])

                    binned_sample_array, binned_count_array = self.bin_samples(self.dg_MWC,
                                                                               interpolated_position_attitude)

                elif self.swath_width_m is not None:
                    binned_sample_array, binned_count_array = self.bin_samples(self.dg_MWC,
                                                                               interpolated_position_attitude)

                elif self.num_nadir_beams is not None:
                    # TODO!
                    pass

                else:
                    print("ERROR: User must specify watercolumn curtain width by: "
                          "\n1. Percent swath width."
                          "\n2. Meters."
                          "\n3. Number nadir beams.")
                    sys.exit(1)

                # After every watercolumn data point for every beam in a ping:
                for i in range(len(binned_sample_array)):
                    if len(binned_sample_array[i]) > 0:
                        # Average bins:
                        binned_sample_array[i] = sum(binned_sample_array[i]) / len(binned_sample_array[i])
                    else:
                        # If there are no values in a bin, enter NaN:
                        binned_sample_array[i] = np.NaN

                avg_watercolumn_array_full.append(binned_sample_array)
                samples_per_bin_array_full.append(binned_count_array)

                # # TODO: Timer implementation--send to plotter every 1 sec.
                #  # Implement this with... threads, I guess?
                # if (time_MWC - counter_one_sec) < 1:
                #     avg_watercolumn_array_one_sec.append(binned_sample_array)
                #     samples_per_bin_array_one_sec.append(binned_count_array)
                # else:
                #     counter_one_sec = time_MWC
                #     # TODO: Publish arrays before clearing.
                #     # Clear arrays before appending
                #     avg_watercolumn_array_one_sec.clear()
                #     samples_per_bin_array_one_sec.clear()
                #     avg_watercolumn_array_one_sec.append(binned_sample_array)
                #     samples_per_bin_array_one_sec.append(binned_count_array)


            # TODO: Temporary. Plotting should occur in different function:
            np_avg_watercolumn_array_full = np.transpose(np.array(avg_watercolumn_array_full))
            np_samples_per_bin_array_full = np.transpose(np.array(samples_per_bin_array_full))

            path, file = os.path.split(fp)

            if z_offset_sonar is not None:
                x = [round(((i * self.bin_height_m) + z_offset_sonar), 3) for i in range(120 + 1)]
            else:
                x = [round((i * self.bin_height_m), 3) for i in range(120 + 1)]

            y = [i for i in range(len(avg_watercolumn_array_full) + 1)]

            if self.colorbar_min is None:
                self.colorbar_min = -65 - self.dg_MWC['rxInfo']['TVGoffset_dB']
            if self.colorbar_max is None:
                self.colorbar_max = 65 - self.dg_MWC['rxInfo']['TVGoffset_dB']

            # Plot all pings:
            plot_file_name = os.path.splitext(file)[0] + "_WC_Plot_All.png"
            fig1a = plt.figure(figsize=(11, 8.5), dpi=150)
            plt.pcolormesh(y, x, np_avg_watercolumn_array_full, cmap='jet')
            plt.ylim(max(plt.ylim()), min(plt.ylim()))
            fig1a.suptitle('Watercolumn (Dual Swath, All)\n' + file)
            plt.xlabel("Ping Number")
            #plt.ylabel("Bin Number\n (Bin Size = %.3f m)" % self.bin_height_m)
            plt.ylabel("Depth (m)")
            clb = plt.colorbar()
            clb.ax.set_xlabel("dB")
            plt.clim(vmin=self.colorbar_min, vmax=self.colorbar_max)
            # figure = plt.gcf()
            # figure.set_size_inches(11, 8.5)
            plt.show()
            if self.write_directory is not None:
                fig1a.savefig(os.path.join(self.write_directory, plot_file_name), dpi=150, bbox_inches='tight')

            # Plot first ping of dual swath:
            plot_file_name = os.path.splitext(file)[0] + "_WC_Plot_OddPing.png"
            fig1b = plt.figure(figsize=(11, 8.5), dpi=150)
            plt.pcolormesh(np_avg_watercolumn_array_full[:, ::2])
            plt.ylim(max(plt.ylim()), min(plt.ylim()))
            fig1b.suptitle('Watercolumn (Dual Swath, Odd Ping)\n' + file)
            plt.xlabel("Ping Number")
            plt.ylabel("Bin Number\n (Bin Size = %.3f m)" % self.bin_height_m)
            clb = plt.colorbar()
            clb.ax.set_xlabel("dB")
            plt.clim(vmin=self.colorbar_min, vmax=self.colorbar_max)
            # figure = plt.gcf()
            # figure.set_size_inches(11, 8.5)
            plt.show()
            if self.write_directory is not None:
                fig1b.savefig(os.path.join(self.write_directory, plot_file_name), dpi=150, bbox_inches='tight')

            # Plot second ping of dual swath:
            plot_file_name = os.path.splitext(file)[0] + "_WC_Plot_EvenPing.png"
            fig1c = plt.figure(figsize=(11, 8.5), dpi=150)
            plt.pcolormesh(np_avg_watercolumn_array_full[:, 1::2])
            plt.ylim(max(plt.ylim()), min(plt.ylim()))
            fig1c.suptitle('Watercolumn (Dual Swath, Even Ping)\n' + file)
            plt.xlabel("Ping Number")
            plt.ylabel("Bin Number\n (Bin Size = %.3f m)" % self.bin_height_m)
            clb = plt.colorbar()
            clb.ax.set_xlabel("dB")
            plt.clim(vmin=self.colorbar_min, vmax=self.colorbar_max)
            # figure = plt.gcf()
            # figure.set_size_inches(11, 8.5)
            plt.show()
            if self.write_directory is not None:
                fig1c.savefig(os.path.join(self.write_directory, plot_file_name), dpi=150, bbox_inches='tight')


            # Plot averaged first and second ping of dual swath:
            plot_file_name = os.path.splitext(file)[0] + "_WC_Plot_Average.png"
            fig1d = plt.figure(figsize=(11, 8.5), dpi=150)
            # Compare sizes of sliced arrays and discard odd entry if necessary:
            if len(np_avg_watercolumn_array_full[:, ::2][0]) > len(np_avg_watercolumn_array_full[:, 1::2][0]):
                # Omit final entry of first array (so that slices are of equal size):
                plt.pcolormesh((np_avg_watercolumn_array_full[:, :-1:2] + np_avg_watercolumn_array_full[:, 1::2]) / 2)
            else:
                plt.pcolormesh((np_avg_watercolumn_array_full[:, ::2] + np_avg_watercolumn_array_full[:, 1::2]) / 2)
            plt.ylim(max(plt.ylim()), min(plt.ylim()))
            fig1d.suptitle('Watercolumn (Dual Swath, Averaged)\n' + file)
            plt.xlabel("Ping Number")
            plt.ylabel("Bin Number\n (Bin Size = %.3f m)" % self.bin_height_m)
            clb = plt.colorbar()
            clb.ax.set_xlabel("dB")
            plt.clim(vmin=self.colorbar_min, vmax=self.colorbar_max)
            # figure = plt.gcf()
            # figure.set_size_inches(11, 8.5)
            plt.show()
            if self.write_directory is not None:
                fig1d.savefig(os.path.join(self.write_directory, plot_file_name), dpi=150, bbox_inches='tight')

    def basic_swath_stats(self, dg_MWC, interp_position_attitude):
        # Determine swath width and min, max, and avg depth:
        #       |         / \
        #       |       /     \
        #     y |     /         \
        #       |   /   (swath)   \
        #       |_/_________________\ _
        #                  x
        x_across_track = []
        y_depth = []
        nadir_indices = []

        for beam in range(dg_MWC['rxInfo']['numBeams']):  # 0 to self.numBeams - 1

            # Across-track angle:
            beam_point_angle_re_vertical1 = dg_MWC['beamData']['beamPointAngReVertical_deg'][beam]
            # Along-track angle:
            tilt_angle_re_tx_deg = dg_MWC['sectorData']['tiltAngleReTx_deg']  # List; one entry per sector
            tilt_angle_re_vertical_deg = tilt_angle_re_tx_deg[dg_MWC['beamData']['beamTxSectorNum'][beam]] - \
                                         interp_position_attitude['attitude']['pitch_deg']

            # Find indices for 10 nadir beams:
            if (len(nadir_indices) is 0) and (beam < dg_MWC['rxInfo']['numBeams'] - 2):
                beam_point_angle_re_vertical2 = dg_MWC['beamData']['beamPointAngReVertical_deg'][beam + 1]
                if (beam_point_angle_re_vertical1 >= 0 > beam_point_angle_re_vertical2) or \
                        (beam_point_angle_re_vertical1 <= 0 < beam_point_angle_re_vertical2):
                    nadir_indices = list(range((beam - 4), (beam + 6)))

            # Index in sampleAmplitude05dB array where bottom detected:
            # Kongsberg: "Two way range in samples. Approximation to calculated distance from tx to bottom
            # detection [meters] = soundVelocity_mPerSec * detectedRangeInSamples / (sampleFreq_Hz * 2).
            # The detected range is set to zero when the beam has no bottom detection.
            # Replaced by detectedRangeInSamplesHighResolution for higher precision."
            detected_range = dg_MWC['beamData']['detectedRangeInSamples'][beam]

            range_to_wc_data_point = ((dg_MWC['rxInfo']['soundVelocity_mPerSec'] * detected_range) /
                                      (dg_MWC['rxInfo']['sampleFreq_Hz'] * 2))

            # TODO: Double check this math:
            # Across-track distance
            x = (range_to_wc_data_point * math.sin(math.radians(beam_point_angle_re_vertical1))
                 * math.cos(math.radians(tilt_angle_re_vertical_deg)))
            # Depth
            y = (range_to_wc_data_point * math.cos(math.radians(beam_point_angle_re_vertical1))
                 * math.cos(math.radians(tilt_angle_re_vertical_deg)))

            x_across_track.append(x)
            y_depth.append(y)

        # Slice y_depth array to include only 10 nadir beams:
        nadir_depths = y_depth[nadir_indices[0]:nadir_indices[-1]]

        swath_width = abs(min(x_across_track)) + abs(max(x_across_track))
        min_depth = min(y_depth)
        max_depth = max(y_depth)
        avg_depth = sum(y_depth) / len(y_depth)
        avg_nadir_depth = sum(nadir_depths) / len(nadir_depths)

        # TODO: Maybe make a PingStats or SwathStats class?
        return {'swath_width': swath_width, 'min_depth': min_depth, 'max_depth': max_depth, 'avg_depth': avg_depth,
                'avg_nadir_depth': avg_nadir_depth}

    def run(self):
        print("Running!")
        self.extract_and_plot_wc_from_file()


if __name__ == '__main__':

    # Defaults:
    _input_file_or_directory = None
    _write_directory = None

    _swath_width_percent = None  # 10
    _swath_width_m = None
    _num_nadir_beams = None

    _along_track_swaths = None
    #_dual_swath = False

    _bin_height_percent = None  # 1
    _bin_height_m = None

    _max_nav_gap_sec = 0.04  # Default

    _colorbar_min = None
    _colorbar_max = None

    # Read command line args for files/directory, num_nadir_beams:
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hf:w:p:m:n:s:d:b:a:g:c:C:",
                                   ["read_file=", "write_dir=", "width_percent=", "width_m=", "num_beams=", "swaths=",
                                    "dual_swath=", "bin_percent=", "bin_m=", "nav_gap=", "color_min=", "color_max="])
    except getopt.GetoptError:
        print("kongsberg_watercolumnplot.py")
        # File or directory containing .kmall or .kmwcd file extensions
        print("-f <read_file_or_directory>")
        # Write directory
        print("-w <write_directory>")

        # Three options for selecting across-track width to include in watercolumn curtain:
        # 1. A percent of overall swath width, centered at nadir beam.
        # (Example: 10% of overall swath width.)
        print("-p <across_track_width_percent>")
        # 2. A specific number of meters, centered at nadir beam.
        # (Example: 10 meters gives 5 meters left and 5 meters right of nadir beam.)
        print("-m <across_track_width_m>")
        # 3. A specific number of beams, centered at nadir beam.
        # (Example: 20 beams.)
        print("-n <number_nadir_beams>")

        # Two options for selecting along-track width to include in watercolumn curtain:
        # 1. A fixed number of pings.
        print("-s <number_along_track_swaths>")
        # 2. If in dual swath mode, average dual swath pings.
        #print("-d <dual_swath>")

        # Two options for selecting vertical bin height:
        # 1. Vertical bin height as a percent of average nadir depth of first ping. 1% by default.
        print("-b <bin_height_percent_depth>")
        # 2. Fixed, absolute vertical bin height. (EX: 0.25 m)
        print("-a <bin_height_m>")

        # Maximum acceptable gap in navigation to interpolate (linear) through.
        print("-g <max_nav_gap_seconds>")

        # Plot colorbar range
        print("-c <minimum_colorbar_value>")
        print("-C <maximum_colorbar_value>")


    for opt, arg in opts:
        if opt == "-h":
            print("watercolumnplot.py\n")
            print("-f <read_file_or_directory>")
            print("-w <write_directory>")
            print("\nDetermine across-track curtain width based on one of the following: ")
            print("-p <across_track_width_percent>")
            print("-m <across_track_width_m>")
            print("-n <number_nadir_beams>")
            print("\nDetermine along-track curtain width based on one of the following: ")
            print("-s <number_along_track_swaths>")
            # Is dual swath Kongsberg-specific? If so, maybe this should go elsewhere?
            # print("-d <dual_swath>")
            print("\nDetermine vertical bin size based on one of the following: ")
            print("-b <bin_height_percent_depth>")
            print("-a <bin_height_m>")
            print("\n-g <max_nav_gap_seconds>")
            print("-c <minimum_colorbar_value>")
            print("-C <maximum_colorbar_value>")
            sys.exit()
        elif opt in ("-f", "--read_file"):
            if not os.path.exists(arg):
                print("ERROR: Invalid read file or directory: %s"%arg)
                sys.exit()
            else:
                _input_file_or_directory = arg
        elif opt in ("-w", "--write_dir"):
            if not os.path.exists(arg):
                print("ERROR: Invalid write directory: %s"%arg)
                sys.exit()
            else:
                _write_directory = arg
        elif opt in ("-p", "--width_percent"):
            _swath_width_percent = int(arg)
        elif opt in ("-m, --width_m"):
            _swath_width_m = int(arg)
        elif opt in ("-n", "--num_beams"):
            _num_nadir_beams = int(arg)
        elif opt in ("-s", "--swaths"):
            _along_track_swaths = int(arg)
        # elif opt in ("-d", "--dual_swath"):
        #     _dual_swath = True
        elif opt in ("-b, --bin_percent"):
            _bin_height_percent = int(arg)
        elif opt in ("-a, --bin_m"):
            _bin_height_m = int(arg)
        elif opt in ("g", "--nav_gap"):
            _max_nav_gap_sec = int(arg)
        elif opt in ("c", "--color_min"):
            _colorbar_min = arg
        elif opt in ("C", "--color_max"):
            _colorbar_max = arg

    if _input_file_or_directory is None:
        print("Must enter file or directory: watercolumnplot.py -f <file_or_directory>")
        sys.exit()

    wc_plotter = WaterColumnPlot(_input_file_or_directory, _write_directory, _swath_width_percent, _swath_width_m,
                                        _num_nadir_beams, _along_track_swaths, _bin_height_percent, _bin_height_m,
                                         _max_nav_gap_sec, _colorbar_min, _colorbar_max)

    wc_plotter.run()
