#!/usr/bin/env python3

import os
import pandas as pd
import argparse
import functools
from multiprocessing import Pool

from common import conditionalMkdir


''' Converts XPT files to CSV files '''
def XPT2CSV(os_walk_path, input_prefix, output_prefix):
    read_dir, _, f_names = os_walk_path
    # Filter out non XPT files
    f_names = [f_name for f_name in f_names if f_name.endswith('.XPT')]
    for f_name in f_names:
        # Define read and write paths
        read_path = os.path.join(read_dir, f_name)
        write_dir = os.path.join(output_prefix, read_dir.lstrip(input_prefix))
        write_path = os.path.join(write_dir, f_name.replace('.XPT', '.csv'))

        # Make sure directory exists
        conditionalMkdir(write_dir)

        # Print progress
        print('Converting file: %s' % read_path)

        # Read XPT (SAS) and write CSV
        data = pd.read_sas(read_path)
        data.to_csv(write_path, index=False)


def main():
    # Get arguments from user
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--multithread', action='store_true',\
            help='invoke multiprocessing python to parallelize conversion')
    parser.add_argument('-i', '--input_dir', default='./data/raw_data/',\
            type=str, help='Input directory containing raw .XPT files from\
            NHANES dataset')
    parser.add_argument('-o', '--output_dir', default='./data/csv_data/',\
            type=str, help='Output directory for .XPT data files converted to\
            .CSV files')
    args = parser.parse_args()

    # Read in each XPT data file and write out as CSV
    os_walk_paths = os.walk(args.input_dir)
    if args.multithread:
        parallelXPT2CSV = functools.partial(XPT2CSV,\
                input_prefix=args.input_dir, output_prefix=args.output_dir)
        pool = Pool(processes=os.cpu_count())
        pool.map(parallelXPT2CSV, os_walk_paths)
    else:
        for os_walk_path in os_walk_paths:
            XPT2CSV(os_walk_path, args.input_dir, args.output_dir)


if __name__ == '__main__':
    main()
