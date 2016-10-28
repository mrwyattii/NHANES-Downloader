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
import argparse
import urllib.request
import re
import functools
from multiprocessing import Pool

from common import conditionalMkdir

try:
        from BeautifulSoup import BeautifulSoup
except ImportError:
        from bs4 import BeautifulSoup


''' Finds all links to XPT files in source HTML '''
def parsePageXPT(html_source):
    # Parse HTML source code with BeautifulSoup library
    soup = BeautifulSoup(html_source, 'html.parser')

    # Get all <a>...</a> with .XPT extensions
    xpt_urls = soup.findAll('a', href=re.compile('\.XPT$'))
    xpt_urls = [url['href'] for url in xpt_urls]
    return xpt_urls


''' Creates directory for file and downloads file from provided URL '''
def getFile(file_dir, file_url, file_type):
    # Print progress
    print('Getting file: %s' % file_url)

    # Get year associated with file
    year = re.search('\/(\d+-\d+)\/', file_url)
    if year:
        year = year.group(1)
    else:
        year = 'Other'

    # Compile file location
    file_dir = '%s/%s/%s/' % (file_dir.rstrip('/'), year, file_type)

    # Make directory for file if necessary
    conditionalMkdir(file_dir)

    # Get name for file
    file_name = file_url.split('/')[-1]
    file_loc = file_dir + file_name

    # Check that file does not already exist
    if not os.path.isfile(file_loc):
        # Download the file and write to local
        urllib.request.urlretrieve(file_url, file_loc)


''' Reads HTML source from provided URLs, parses HTML for XPT files, and saves files '''
def parseWebSite(url, output_dir):
    # Get base URL for appending to relative file URLs
    base_url = 'http://' + url.lstrip('http://').split('/')[0]

    # Get file type for this URL
    file_type = re.search('Component=([a-zA-Z]+)', url)
    if file_type:
        file_type = file_type.group(1)
    else:
        file_type = 'Other'

    # Open the website and download source html
    with urllib.request.urlopen(url) as page:
        html_source = page.read()

    # Parse the website for .XPT file links
    file_urls = parsePageXPT(html_source)
    file_urls = [base_url + file_url for file_url in file_urls]

    # Download each file and store locally
    for file_url in file_urls:
        getFile(output_dir, file_url, file_type)


def main():
    # Get text file with list of URLs for NHANES data
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output_dir', type=str,\
            default='./data/raw_data/', help='Location for writing files')
    parser.add_argument('-m', '--multithread', action='store_true',\
            help='invoke multiprocessing python to parallelize downloads')
    parser.add_argument('url_list', type=str, default='./NHANES_URLS.txt',\
            nargs='?', help='text document containing URLs to NHANES\
            website listing data files')
    args = parser.parse_args()

    # Make output directory if necessary
    conditionalMkdir(args.output_dir)

    # Get list of URLs
    with open(args.url_list, 'r') as f:
        urls = f.readlines()

    # Parse each webpage
    if args.multithread:
        parallelParseWebSite = functools.partial(parseWebSite,\
                output_dir=args.output_dir)
        pool = Pool(processes=os.cpu_count())
        pool.map(parallelParseWebSite, urls)
    else:
        for url in urls:
            parseWebSite(url, args.output_dir)



if __name__ == '__main__':
    main()
