#!/usr/bin/env python

import glob
import os
import argparse

from obspy import UTCDateTime, read
from time import gmtime, strftime
from obspy.fdsn import Client
from obspy.neic import Client as ClientGCWB
from obspy.neic import Client as ClientPCWB
from multiprocessing import Pool

###################################################################################################
#chkavail.py
#
#This program checks the availability of data from various sources
#
#Methods
#args()
#checkAvail()
#
###################################################################################################

print 'Scan started on', strftime('%Y-%m-%d %H:%M:%S', gmtime()), 'UTC'

client = Client("IRIS")
chan = '*[BL]H*'
decimalPlaces = 2



def args():
#This function parses the command line arguments
	parser = argparse.ArgumentParser(description='Code to compare data availability')

#Sets flag for the network
	parser.add_argument('-n', action = "store",dest="net", \
		default = "*", help="Network to check: NN", type = str, required = True)

#Sets flag for the station (optional)
	parser.add_argument('-s', action = "store",dest="sta", \
		default = "*", help="Station to check: SSSS", type = str, required = False)

#Sets flag for the year in range
	parser.add_argument('-y', action = "store",dest="year", \
		default = 2014, help="Year to check: YYYY", type = int, required = True)

#Sets flag for the first day in range
	parser.add_argument('-sd', action = "store",dest="sday", \
		default = 1, help="Start day: DDD", type = int, required = True)

#Sets flag for the last day in range (optional)
	parser.add_argument('-ed', action = "store",dest="eday", \
		default = 1, help="End day: DDD", type = int)

#Sets flag for verbose mode
	parser.add_argument('-d','--debug',action = "store_true",dest="debug", \
		default = False, help="Run in debug mode")

#Sets flag for forcefully checking IRIS
	parser.add_argument('-IRIS',action = "store_true",dest="iris", \
		default = False, help="Indiscriminately check IRIS without regard to /xsX")

#Sets flag for the NEIC CWB
	parser.add_argument('-GCWB',action = "store_true",dest="gcwb", \
		default = False, help="Check the NEIC CWB (GCWB)")

#Sets flag for the PCWB
	parser.add_argument('-PCWB',action = "store_true",dest="pcwb", \
		default = False, help="Check the NEIC internal CWB (PCWB)")

#Sets flag for the ASL CWB
	parser.add_argument('-ASL',action = "store_true",dest="asl", \
		default = False, help="Check the ASL CWB (ASLCWB)")

#Sets flag for IRIS data quality type for Tyler
	parser.add_argument('-q',action = "store",dest = "quality", \
		default = "M", help = "Data quality type: D,Q,...")

	parserval = parser.parse_args()
	return parserval


def checkDate(date):
#Here is the heart of the program where the availability gets checked
	allAvailString=[]

#Here we parse the function arguments
	year, day = date.split(",")
	print 'On day ' + str(jday).zfill(3) + ' ' + str(year)

	startTime = UTCDateTime(year + day.zfill(3) + 'T00:00:00.000')
	endTime = startTime + 24*60*60

#Setup the /tr1 string and glob it
	if net in ['IU','IC','CU']:
		netPath = '/xs0'
	else:
		netPath = '/xs1'
	globString = netPath + '/seed/' + net + '_' + sta +'/' + year + '/' + year + '_' \
		+ day.zfill(3) + '_' + net + '_' + sta + '/' + chan
	dataOnXSX = glob.glob(globString)
	
	if debug:
		print(globString)
		for item in dataOnXSX:
			print item


	for tracePath in dataOnXSX:
		#Parses through the different channels and locations for a given station
		#Below sets up the availability string as laid out in the header
		currentAvailability = identifyChanLoc(tracePath)
		#if True:
		try:
			if debug:
				#To visually separate the channel location scans from one another
				print '\n' + '*' * 80
			#Checks the availability for given sources
			availXSX        = checkAvailability('xsX', tracePath, startTime, endTime)
			availTR1        = checkAvailability('tr1', tracePath, startTime, endTime)
			if availXSX < 100.0 or parserval.iris:
				availIRIS   = checkAvailability('IRIS', tracePath, startTime, endTime)
			else:
				if debug:
					print '\nIRIS not queried due to', netPath, 'availability percentage'
				availIRIS   = 'XX'
			if parserval.gcwb:
				availGCWB   = checkAvailability('GCWB', tracePath, startTime, endTime)
			if parserval.pcwb:
				availPCWB   = checkAvailability('PCWB', tracePath, startTime, endTime)
			if parserval.asl:
				availASLCWB = checkAvailability('ASLCWB', tracePath, startTime, endTime)
			#Appends the availability percentages for chan/loc for given station
			currentAvailability.append(str(availXSX))
			currentAvailability.append(str(availTR1))
			currentAvailability.append(str(availIRIS))
			if parserval.gcwb:
				currentAvailability.append(str(availGCWB))
			if parserval.pcwb:
				currentAvailability.append(str(availPCWB))
			if parserval.asl:
				currentAvailability.append(str(availASLCWB))
			if availXSX < 100.0:
				#If /xs0 or /xs1 availability is not 100%
				if availXSX != availIRIS:
					#And if /xs0 or /xs1 availability does not equal IRIS
					allAvailability.append(','.join(currentAvailability))
			if debug:
				#Prints the station/chan/loc availability
				print '\n', header[:-1]
				print ' '.join(currentAvailability)
		except:
			print 'Problem with:', tracePath
	f = open(outputFilename(),'a')
	for curavail in allAvailability:
		#Writes the availability to file
		print 'Writing to file'
		f.write(curavail + '\n')
	f.close()
	return

def checkAvailability(IDstring, tracePath, startTime, endTime):
	#Checks the availability for the desired source
	try:
		if   IDstring == 'xsX':
			#Checks for local data on /xs0 or /xs1 accordingly
			traces = read(tracePath)
		elif IDstring == 'tr1':
			#Checks for local data on /tr1
			#Note: data on /tr1 is only for the most current 100 day period
			traces = read(convertXSXtoTR1(tracePath))
		else:
			#Checks CWBs
			traces = getWaveform(IDstring, read(tracePath), startTime, endTime)
		availability = 0
		#Merges with method 0 to appropriately handle data overlaps
		#Mostly an issue with IRIS data
		traces.merge(method = -1)
		if debug:
			nptsSamples   = 0
			sampleRate    = 0
		for trace in traces:
			#Sometimes merges have multiple traces and as such
			#it is necessary to step through them one by one
			availability += trace.stats.npts / (24*60*60 * trace.stats.sampling_rate)
			if debug:
				nptsSamples  += trace.stats.npts
				sampleRate    = trace.stats.sampling_rate
		if debug:
			print ''
			print IDstring, 'points:', nptsSamples, 'out of', sampleRate * 24 * 60 * 60
			print IDstring, 'availability:', str(availability * 100) + '%'
	except:
		availability = 0
		if debug:
			print '\nProblem with ' + IDstring + ', availability set to 0%'
			if IDstring == 'tr1' and not os.path.isfile(convertXSXtoTR1(tracePath)):
				#Checks to see if the data exists on /tr1
				print 'Data does not exist on /tr1 for this day'
	return round(availability, decimalPlaces + 2) * 100

def getWaveform(IDstring, trace, startTime, endTime):
	#Gets the traces from CWBs
	if   IDstring == 'IRIS':
		#Incorporated Research Institutions for Seismology
		trace = client.get_waveforms(net, trace[0].stats.station, trace[0].stats.location,\
			trace[0].stats.channel, startTime, endTime, quality = qualval)
	elif IDstring == 'GCWB':
		#NEIC's (Golden's) public-facing CWB
		trace = clientGCWB.getWaveform(net, trace[0].stats.station,\
			trace[0].stats.location, trace[0].stats.channel, startTime, endTime)
	elif IDstring == 'PCWB':
		#NEIC's (internal) private-facing CWB
		trace = clientPCWB.getWaveform(net, trace[0].stats.station,\
			trace[0].stats.location, trace[0].stats.channel, startTime, endTime)
	elif IDstring == 'ASLCWB':
		#Albuquerque Seismological Laboratory's CWB
		trace = clientASL.getWaveform(net, trace[0].stats.station,\
			trace[0].stats.location, trace[0].stats.channel, startTime, endTime)
	return trace

def convertXSXtoTR1(tracePath):
	#Converts the file paths from /xs0 or /xs1 to /tr1
	tracePath    = tracePath.split('/')
	netSta       = tracePath[3]
	year         = tracePath[4]
	yearDay      = tracePath[5][:8]
	fileName     = tracePath[-1]
	tr1TracePath = '/tr1/telemetry_days/' + netSta + '/' + year + '/' + yearDay + \
		'/' + fileName
	return tr1TracePath

def identifyChanLoc(tracePath):
	#Returns the preamble for station/loc/chan availability
	tracePath = tracePath.split('/')
	sta       = tracePath[3].split('_')[-1]
	year      = tracePath[4]
	day       = tracePath[5][5:8]
	fileName  = tracePath[-1]
	chan      = tracePath[-1][3:6]
	loc       = tracePath[-1][:2]
	return [sta, loc, chan, year, day]

#Here are the parser value arguments
#We make the global to get them in the function
parserval = args()
net = parserval.net
year = parserval.year
sta = parserval.sta
debug = parserval.debug
sday = parserval.sday

if parserval.eday != 1:
	#If eday flag was parsed, set eday
	eday = parserval.eday
else:
	#If eday has default value, set eday equal to sday
	#This allows for one-day scans
	eday = sday
qualval = parserval.quality

if parserval.iris:
	if debug:
		print 'IRIS selected, will be indiscriminately queried'
		
if parserval.gcwb:
	#Sets up GCWB client to read the waveforms
	if debug:
		print 'GCWB selected', parserval.gcwb
	clientGCWB = ClientGCWB()

if parserval.asl:
	#Copies GCWB's client and redirects it to ASL's CWB
	if debug:
		print 'ASLCWB selected', parserval.asl
	clientASL = ClientGCWB(host='136.177.121.27')

if parserval.pcwb:
	#Copies GCWB's client and redirects it to their internal CWB
	if debug:
		print 'PCWB selected', parserval.pcwb
	clientPCWB = ClientGCWB(host='136.177.24.70')

def outputFilename():
	#Forms the name of the output file
	#Avail + year + jday + net (+ sta) .csv
	outputFilename = 'avail' + str(year) + str(sday).zfill(3)
	if net != '*':
		outputFilename += net
		if sta != '*':
			outputFilename += sta
	outputFilename += '.csv'
	return outputFilename

f = open(outputFilename(),'w')
header = 'Sta,Loc,Chan,Year,Day,xsX,tr1,IRIS'
if parserval.pcwb:
	header += ',PCWB'
if parserval.gcwb:
	header += ',GCWB'
if parserval.asl:
	header += ',ASLCWB'
header += '\n'
f.write(header)
f.close()

#Here we loop over the days
dateRange = []
for day in xrange(sday, eday + 1, 1):
	dateRange.append(str(year) + ',' + str(day).zfill(3))

#Hwere we run everything as a multi-process
pool = Pool()
pool.map(checkDate,dateRange)

print 'Scan ended on', strftime('%Y-%m-%d %H:%M:%S', gmtime()), 'UTC'
print 'Output saved to', outputFilename()