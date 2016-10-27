#!/usr/bin/env python3

import os
import argparse
import urllib.request
import re
import functools
from multiprocessing import Pool

try:
        from BeautifulSoup import BeautifulSoup
except ImportError:
        from bs4 import BeautifulSoup


def conditionalMkdir(directory):
    if not os.path.exists(directory):
        os.makedir(directory)


def parsePageXPT(html_source):
    # Parse HTML source code with BeautifulSoup library
    soup = BeautifulSoup(html_source, 'html.parser')

    # Get all <a>...</a> with .XPT extensions
    xpt_urls = soup.findAll('a', href=re.compile('\.XPT$'))
    xpt_urls = [url['href'] for url in xpt_urls]
    return xpt_urls


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

    # Download the file and write to local
    urllib.request.urlretrieve(file_url, file_loc)


def parseWebSite(url, output):
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
        getFile(output, file_url, file_type)


def main():
    # Get text file with list of URLs for NHANES data
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', type=str, default='./data/raw_data/',
            help='Location for writing NHANES files')
    parser.add_argument('-m', '--multithread', action='store_true',\
            help='invoke multiprocessing python to parallelize downloads')
    parser.add_argument('url_list', type=str, default='./NHANES_URLS.txt',\
            nargs='?', help='text document containing URLs to NHANES\
            website listing data files')
    args = parser.parse_args()

    # Make output directory if necessary
    conditionalMkdir(args.output)

    # Get list of URLs
    with open(args.url_list, 'r') as f:
        urls = f.readlines()

    # Parse each webpage
    if args.multithread:
        parallelParseWebSite = functools.partial(parseWebSite,\
                output=args.output)
        print(os.cpu_count())
        pool = Pool(processes=os.cpu_count())
        pool.map(parallelParseWebSite, urls)
    else:
        for url in urls:
            parseWebSite(url, args.output)


if __name__ == '__main__':
    main()
