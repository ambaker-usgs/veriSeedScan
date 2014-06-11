#!/usr/bin/env python
import glob
import urllib2
import os
import psycopg2

from multiprocessing import Pool

from obspy.core import Stream, read, UTCDateTime
from math import isnan

###################################################################################################
#veriGaps.py
#By Adam Ringler
#
#This program verifies gaps for obspy, Mustang, DCC, and Seedscan
###################################################################################################

debug = True

#This is the main program which is called as a function to allow for multi-threading
def veriGaps(string):
	string = string.split(",")
	net = string[0]
	sta = string[1]
	chan = string[2]
	loc = string[3]
	year = string[4]
	day = string[5]
	if net in set(["IU","IC","CU"]):
		dataloc = '/xs0/seed/'
	else:
		dataloc = '/xs1/seed/'
	dataloc += net + '_' + sta + '/' + year + '/' + year + '_' + day + '_' + net + '_' + sta + '/'
	datalocSeed = dataloc + loc + '_' + chan + '*.seed'


	#Here we check the gaps from obspy
	try:
		gaps = float('nan')
		if debug:
			print(datalocSeed)
		datalocSeed = glob.glob(datalocSeed)
		st = Stream()
		for datafile in datalocSeed:
			st += read(datafile)
			if debug:
				print(st)
			gaps = st.getGaps()
			gaps = len(gaps)
			if debug:
				print 'Here are the gaps:' + str(gaps)
	except:
		gaps = float('nan')


	#Here we check the gaps from the DCC
	DCCgaps = float('nan')
	try:
		f = open(dataloc + "data_avail.txt")
		lines = f.readlines()
		f.close()
		for line in lines:
			line = ' '.join(line.split())
			line = line.split(' ')
			if line[0] == loc and line[1] == chan:
				DCCgaps = int(line[3])
				if debug:
					print DCCgaps
	except:
		DCCgaps = float('nan')
	

	#Here we will check the gaps from mustang
	try:	
		mustangGapsTotal = float('nan')
		datevalStart = UTCDateTime(year + "-" + day + "T00:00:00.0")
		datevalEnd = datevalStart + 24*60*60
		urlMustang = "http://service.iris.edu/mustangbeta/measurements/1/query?metric=num_gaps&"
		urlMustang += "net=" + net + "&sta=" + sta + "&loc=" + loc + "&cha=" + chan + "&output=csv" + \
			"&timewindow=" + datevalStart.formatIRISWebService() + "," + datevalEnd.formatIRISWebService()
		if debug:
			print(urlMustang)
		mustangGaps = urllib2.urlopen(urlMustang)
		if debug:
			print(urlMustang)
	
		mustangGapsTotal = 0 
		for index,line in enumerate(mustangGaps.readlines()):
			if index > 1:
				if debug:
					print 'Here is the mustang line ' + line
				line = line.split(",")
				line = line[0].replace('"','')
				mustangGapsTotal += int(line)
		if index <= 1:
			mustangGapsTotal = float('nan')
			
	except:
		mustangGapsTotal = float('nan')

	#Here we will check the gaps from seedscan
	try:
		connString = open('db.config', 'r').readline()
		if debug: 
			print "Connecting to "+connString
		host, user, pwd, db, port = connString.split(',')
		conn = psycopg2.connect(host=host, user=user, password=pwd, database=db, port=port)

		queryString = """
SELECT "tblGroup".name, tblStation.name, tblChannel.name, tblMetric.name, value, tblDate.date
  FROM tblstation
  JOIN tblSensor on fkStationID = pkStationID
  JOIN tblChannel on fkSensorID = pksensorID
  JOIN tblMetricData on fkChannelID = pkChannelID
  JOIN tblMetric on fkMetricID = pkMetricID
  JOIN "tblGroup" on fkNetworkID = pkGroupID
  JOIN tblDate on tblMetricData.date = pkdateid
  WHERE tblStation.name = %s
  AND tblChannel.name = %s
  AND tblMetric.name = 'GapCountMetric'
  AND tblDate.date = %s
  AND tblSensor.location = %s
  LIMIT 100
"""
		date = year + "-" + str(datevalStart.month).zfill(2) + "-" + \
			str(datevalStart.day).zfill(2)
		cursor = conn.cursor()
		cursor.execute(queryString,(sta,chan,date,loc))
	
		seedScanGaps = int((cursor.fetchone())[4])
		conn.close()	
	except:
		seedScanGaps = float('nan')
	if DCCgaps != gaps or gaps != mustangGapsTotal or gaps != seedScanGaps:
		print year + " " + day + " " + sta + " " + loc + " " + chan + " DCC gaps=" + \
			str(DCCgaps) + " Obspy gaps=" + str(gaps) + " Mustang gaps=" + \
			str(mustangGapsTotal) + " Seedscan gaps=" + str(seedScanGaps)	
	return



getStaList = glob.glob('/xs0/seed/IU*')
staList = []
for sta in getStaList:
	staList.append(sta.replace('/xs0/seed/IU_',''))
net = 'IU'
year = '2014'
loc = '00'
chan = 'BHZ'
sendToGaps=[]
for day in xrange(1,158,1):
	for item in staList:
		sendToGaps.append(net + ',' + item + ',' + chan + ',' + loc + ',' + year + ',' + str(day).zfill(3))
	pool = Pool()
	pool.map(veriGaps,sendToGaps)
	print "Finished day: " + str(day)
	sendToGaps=[]






