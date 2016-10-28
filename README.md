# NHANES-Downloader
Python script to download the entire NHANES dataset from the CDC website.

# Requrements
* Python 3
* BeautifulSoup

The easiest way to ensure that these requirements are met is to install
Anaconda 3.  You can download that from here:
https://www.continuum.io/downloads

If you already have python 3 installed, then BeautifulSoup can be installed
with pip using `pip3 install --user beautifulsoup4`.


# Description
This is a simple python script which you can use to download the entire NHANES
dataset from the CDC website.  The script will load the websites at the URLs
provided in `NHANES_URLS.txt` and parse each page for links to .XPT files.  It
will then download these files and store then in a local `./data/` directory.

# Usage
Running the script is easy.  Just navigate to the directory of the script and
then:
```
$ ./get_data.py
```

# Arguments
There are a few command line arguments for this script (but default values have
been set).  `-o` will specify the directory to save the NHANES data. `-m` can
be used to invoke a multiprocess version of the script which utilizes the
python `multiprocessing.pool` method.  Last, you can specify a different file
containing URLs.  For additionaly documentation, try:
```
$ ./get_data.py -h
```
