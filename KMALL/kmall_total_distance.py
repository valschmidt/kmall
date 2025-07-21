#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import subprocess
import concurrent.futures
import re
import argparse

def find_kmall_files(root_dir):
    kmall_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            if fname.endswith('.kmall'):
                kmall_files.append(os.path.join(dirpath, fname))
    return kmall_files

def get_distance_from_file(kmall_py, kmall_file):
    try:
        result = subprocess.run(
            ['python3', kmall_py, '-f', kmall_file, '-S'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True
        )
        # Extract distance from output
        match = re.search(
            r'Total distance travelled:\s*([0-9.]+)\s*nautical miles\s*\(([0-9.]+)\s*meters\)', result.stdout)
        if match:
            nm = float(match.group(1))
            m = float(match.group(2))
            return nm, m
    except Exception as e:
        print(f"Error processing {kmall_file}: {e}")
    return 0.0, 0.0

def main():
    parser = argparse.ArgumentParser(description="Sum total distance from all .kmall files in a directory tree.")
    parser.add_argument("root_dir", help="Root directory to search for .kmall files")
    parser.add_argument("--kmall_py", default="/Users/vschmidt/gitsrc/kmall/KMALL/kmall.py", help="Path to kmall.py script")
    args = parser.parse_args()
    
    files = find_kmall_files(args.root_dir)
    total_nm = 0.0
    total_m = 0.0

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_file = {executor.submit(get_distance_from_file, args.kmall_py, f): f for f in files}
        completed = 0
        for future in concurrent.futures.as_completed(future_to_file):
            nm, m = future.result()
            total_nm += nm
            total_m += m
            completed += 1
            print(f"[{completed}/{len(files)}] Processed: {os.path.basename(future_to_file[future])} | Distance: {nm:.3f} NM ({m:.3f} m)")

    print("TOTAL distance travelled: %.3f nautical miles (%.3f meters)" % (total_nm, total_m))

if __name__ == '__main__':
    main()