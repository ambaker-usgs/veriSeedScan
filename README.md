veriSeedScan
============

Verify dataless and various seedscan functions as well as data differences

veriA0.py
=========

This program compares A0 frequencies are correctly stated

veriAvail.py
============

This program verifies the sample rate to the channel name

veriGaps.py
============

This program verifies the gaps are consistent between different computations

chkavail.py
============

This program checks the availability of data at different sources

Usage
-----

usage: chkavail.py [-h] -n NET [-s STA] -y YEAR -sd SDAY -ed EDAY [-d] [-NEIC]
                   [-ASL] [-q QUALITY]

Code to compare data availability

optional arguments:
  -h, --help   show this help message and exit
  -n NET       Network to check: NN
  -s STA       Station to check: SSSS
  -y YEAR      Year to check: YYYY
  -sd SDAY     Start day: DDD
  -ed EDAY     End day: DDD
  -d, --debug  Run in debug mode
  -NEIC        Check the NEIC CWB
  -ASL         Check the ASL CWB
  -q QUALITY   Data quality type: D,Q,...


Example
-------

chkavail.py -n IU -y 2014 -s RAR -sd 100 -ed 103 -q Q

