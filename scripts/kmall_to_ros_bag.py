#!/bin/env python3

# Converts xyz points from kmall files into
# ROS PointCloud2 messages and saves them into one
# or more ROS bag files.

import KMALL
import argparse
import datetime
import struct

try:
    import rospy
    import rosbag
    from sensor_msgs.msg import PointCloud2, PointField
    from tf2_msgs.msg import TFMessage
    from geometry_msgs.msg import TransformStamped
except ModuleNotFoundError as e:
    print('\nThis is script is meant to run on a system running the Robotic Operating System (ROS).\n')
    raise
    


parser = argparse.ArgumentParser(description="Convert soundings from kmall files to ROS Pointcloud2 messages and saves to ROS bag files.")

parser.add_argument("inputs", metavar="input.kmall", nargs='+', help="Input kmall file.")

parser.add_argument("-o", "--output", help="Output bag file. If omitted, a bag file for each input file will be created by adding '.bag' to each input filename. When an output bag file is spcified, the data from all input files will be saved to that single bag file.")

parser.add_argument("-t", "--topic", default="mbes/pings", help="Topic where PointCloud2 messages will appear. (default: %(default)s)")

parser.add_argument("-f", "--frame_id", default="mbes", help="The Frame ID used in the PointCloud2 messages. (default: %(default)s)")

tf_parser = parser.add_argument_group("Transform messages", "Optional transform messages from a parent frame to frame_id can be saved to the bag file(s). The default orientation flips the sensor upside down to match the typical mounting on a surface survey ship.")

tf_parser.add_argument("-tf", "--tf", help="Enable saving of transform messages in topic /tf. (default: %(default)s)", action="store_true")

tf_parser.add_argument("-p", "--parent_frame_id", default="base_link_level", help="Parent frame for transform messages. (default: %(default)s)")

tf_parser.add_argument("-tx", "--translate_x", type=float, default=0.0, help="X translate component (default: %(default)s)")

tf_parser.add_argument("-ty", "--translate_y", type=float, default=0.0, help="Y translate component (default: %(default)s)")

tf_parser.add_argument("-tz", "--translate_z", type=float, default=0.0, help="Z translate component (default: %(default)s)")

tf_parser.add_argument("-qx", "--quaternion_x", type=float, default=1.0, help="X quaternion rotation component (default: %(default)s)")

tf_parser.add_argument("-qy", "--quaternion_y", type=float, default=0.0, help="Y quaternion rotation component (default: %(default)s)")

tf_parser.add_argument("-qz", "--quaternion_z", type=float, default=0.0, help="Z quaternion rotation component (default: %(default)s)")

tf_parser.add_argument("-qw", "--quaternion_w", type=float, default=0.0, help="W quaternion rotation component (default: %(default)s)")

args = parser.parse_args()

# A single bag file is created if output filename is provided.
if args.output is not None:
    bag = rosbag.Bag(args.output, 'w')

if args.tf:
    transform = TransformStamped()
    transform.header.frame_id = args.parent_frame_id
    transform.child_frame_id = args.frame_id
    transform.transform.translation.x = args.translate_x
    transform.transform.translation.y = args.translate_y
    transform.transform.translation.z = args.translate_z
    transform.transform.rotation.x = args.quaternion_x
    transform.transform.rotation.y = args.quaternion_y
    transform.transform.rotation.z = args.quaternion_z
    transform.transform.rotation.w = args.quaternion_w

for kfile in args.inputs:
    # Create a bag file per input file if output is ommited
    if args.output is None:
        bag = rosbag.Bag(kfile+'.bag', 'w')

    k = KMALL.kmall(kfile)
    while not k.eof:
        k.decode_datagram()
        if k.datagram_ident == 'MRZ':
            k.read_datagram()
            mrz = k.datagram_data
            
            pc = PointCloud2()
            pc.header.frame_id = args.frame_id
            pc.header.stamp = rospy.Time.from_sec(mrz['header']['dgdatetime'].replace(tzinfo=datetime.timezone.utc).timestamp())

            pc.fields.append(PointField(name="x", offset=0, datatype=PointField.FLOAT32, count=1))
            pc.fields.append(PointField(name="y", offset=4, datatype=PointField.FLOAT32, count=1))
            pc.fields.append(PointField(name="z", offset=8, datatype=PointField.FLOAT32, count=1))
            pc.fields.append(PointField(name="backscatter", offset=12, datatype=PointField.FLOAT32, count=1))

            pc.height = 1
            pc.width = len(mrz['sounding']['x_reRefPoint_m'])

            pc.is_bigendian = False
            pc.point_step = 16 # four x four byte floats
            pc.row_step = pc.width*pc.point_step
            pc.is_dense = True

            for i in range(pc.width):
                pc.data += struct.pack('<ffff', mrz['sounding']['x_reRefPoint_m'][i], 
                                       mrz['sounding']['y_reRefPoint_m'][i],
                                       mrz['sounding']['z_reRefPoint_m'][i],
                                       mrz['sounding']['reflectivity1_dB'][i])

            if args.tf:
                transform.header.stamp = pc.header.stamp
                tfm = TFMessage()
                tfm.transforms.append(transform)
                bag.write('/tf', tfm, transform.header.stamp)

            bag.write(args.topic, pc, pc.header.stamp)
        else:
            k.skip_datagram()

    # only close bag file if using per input bags
    if args.output is None:
        bag.close()

# Only close if using a single bag for output.
# per input bags should be closed already by now.
if args.output is not None:
    bag.close()
