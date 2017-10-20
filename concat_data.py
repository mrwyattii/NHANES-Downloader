#!/usr/bin/env python3

## Copyright (c)
##    2017 by The University of Delaware
##    Contributors: Michael Wyatt
##    Affiliation: Global Computing Laboratory, Michela Taufer PI
##    Url: http://gcl.cis.udel.edu/, https://github.com/TauferLab
##
## All rights reserved.
##
## Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions are met:
##
##    1. Redistributions of source code must retain the above copyright notice,
##    this list of conditions and the following disclaimer.
##
##    2. Redistributions in binary form must reproduce the above copyright
##    notice, this list of conditions and the following disclaimer in the
##    documentation and/or other materials provided with the distribution.
##
##    3. If this code is used to create a published work, one or both of the
##    following papers must be cited.
##
##            M. Wyatt, T. Johnston, M. Papas, and M. Taufer.  Development of a
##            Scalable Method for Creating Food Groups Using the NHANES Dataset
##            and MapReduce.  In Proceedings of the ACM Bioinformatics and
##            Computational Biology Conference (BCB), pp. 1 - 10. Seattle, WA,
##            USA. October 2 - 4, 2016.
##
##    4.  Permission of the PI must be obtained before this software is used
##    for commercial purposes.  (Contact: taufer@acm.org)
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
## AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
## IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
## ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
## LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
## CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
## SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
## INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
## CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
## ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
## POSSIBILITY OF SUCH DAMAGE.

import os
import numpy as np
import pandas as pd
import argparse
import functools
from functools import reduce
from multiprocessing import Pool

from common import conditionalMkdir


def appendToColumns(data, append_string, exception):
    columns = [col if col==exception else '_'.join((append_string, col))\
            for col in data.columns]
    data.columns = columns
    return data


def readYearData(year_dir, concat_on):
    # Create empty list to hold file paths
    f_paths = []

    # Get exact paths to each file
    for read_dir, _, f_names in os.walk(year_dir):
        # Filter any non CSV files
        f_names = [f_name for f_name in f_names if f_name.endswith('.csv')]
        f_paths += [os.path.join(read_dir, f_name) for f_name in f_names]

    # Read data into Pandas dataframes
    data = []
    for f_path in f_paths:
        tmp = pd.read_csv(f_path)
        append_string = '_'.join(f_path.split('/')[-2:]).rstrip('.csv')
        tmp = appendToColumns(tmp, append_string, concat_on)
        data.append(tmp)

    return data


def processYearColumns(data, concat_on):
    # Remove data which doesn't have concat_on column
    data = [val for val in data if concat_on in val.columns]

    # Remove data which has multiple entries for same concat_on column
    # Note: We do this for performance reasons - Might remove important data
    data = [val for val in data if len(val[concat_on].unique())==len(val)]

    return data


def concatYearData(year_dir, write_dir, concat_on):
    # Print progress
    print('Concating: %s' % year_dir)

    # Read data into dictionary
    data = readYearData(year_dir, concat_on)

    # Process column names to make all unique (except concat_on)
    # Additionally, remove any data without concat_on column
    data = processYearColumns(data, concat_on)

    # Make sure write directory exists
    conditionalMkdir(write_dir)

    # Define write path
    write_name = os.path.basename(year_dir) + '.pd'
    write_path = os.path.join(write_dir, write_name)

    # Concatenate the data with concat_on column
    data = reduce(lambda l,r: pd.merge(l, r, on=concat_on, how='outer'), data)

    # Save the data
    data.to_pickle(write_path)


def main():
    # Get arguments from user
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_dir', default='./data/csv_data/',\
            type=str, help='Input directory with csv data from raw_to_csv.py\
            script')
    parser.add_argument('-o', '--output_dir', default='./data/concat_data/',\
            type=str, help='Output directory for concatenated data')
    parser.add_argument('-c', '--concat_on', type=str,\
            default='Respondent sequence number', help='Column name from CSV\
            files which is used\ to concat data')
    parser.add_argument('-m', '--multithread', action='store_true',\
            help='invoke multprocessing python to parallelize concating')
    args = parser.parse_args()

    # Get directories for each year of NHANES data
    year_dirs = [os.path.join(args.input_dir, year_dir) for year_dir in\
            os.listdir(args.input_dir)]

    # Read data, concat, and output to CSV
    if args.multithread:
        parallelConcatYearData = functools.partial(concatYearData,\
                write_dir=args.output_dir, concat_on=args.concat_on)
        pool = Pool(processes=os.cpu_count())
        pool.map(parallelConcatYearData, year_dirs)
    else:
        for year_dir in year_dirs:
            concatYearData(year_dir, args.output_dir, args.concat_on)


if __name__ == '__main__':
    main()
