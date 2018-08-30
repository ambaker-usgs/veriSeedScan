#!/usr/bin/env python

#Importing packages
from obspy.io.xseed import Parser	#Dataless seed reader
from obspy.core import UTCDateTime	#Timing conventions
from time import gmtime, strftime	#Timing conventions
from multiprocessing import Pool	#Multiprocessing

datalessLocation = '/APPS/metadata/SEED/'	#Location of the dataless

startYear = 1995	#Year to begin review
endYear   = 2014	#Year to end review

debug = False	#For additional verbose output

network = 'US'		#Network to run (CU, IC, IU, US, NE, IW)

def main(parsedDataless):
	#Begins the review, sets blank variables
	for station in parsedDataless.stations:
		processStation(station)
	return 'DONE'

def processStation(station):
	#Processes station data to generate verbose report
	stationCallLetters = ''
	channelIdentifier  = ''
	locationIdentifier = ''
	startDate          = ''
	endDate            = ''
	a0frequency        = ''
	gainFrequency1     = ''
	gainFrequency0     = ''
	stageSequenceNum   = ''
	for blockette in station:
		#Begins setting the variables that we want to check for analysis later on
		if   blockette.id == 50:
			stationCallLetters = blockette.station_call_letters
		elif blockette.id == 52:
			locationIdentifier = blockette.location_identifier
			channelIdentifier  = blockette.channel_identifier
			startDate          = blockette.start_date
			endDate            = blockette.end_date
			if endDate == '':
				endDate = 'present'
		elif blockette.id == 53:
			a0frequency        = blockette.normalization_frequency

		elif blockette.id == 58:
			if   blockette.stage_sequence_number == 1:
				gainFrequency1 = blockette.frequency
			elif blockette.stage_sequence_number == 0:
				gainFrequency0 = blockette.frequency
				#Begin analysis
				if not a0frequency == gainFrequency1 == gainFrequency0:
					warning = 'Frequency Deviation: ' + stationCallLetters + ' '
					warning += cleanLocationIdentifier(str(locationIdentifier)) + ' '
					warning += channelIdentifier + ' ' + cleanDate(startDate, endDate)
					warning += ' A0HZ: ' + str(a0frequency) + ', S1GHZ:' + str(gainFrequency1)
					warning += ', S0GHZ: ' + str(gainFrequency0)
					print warning

def cleanLocationIdentifier(lID):
	#If there is no location ID, it writes spaces equaling a valid location ID
	if len(lID) == 0:
		return '  '
	return lID

def cleanDate(startDate, endDate):
	#Turns the UTCDateTime into human readable, showing date and time
	sdate = str(startDate)
	sdate = sdate[:4]+ sdate[5:7] + sdate[8:10] + ' ' + sdate[11:19]
	if type(endDate) is str:
		edate = ' to present          '
	else:
		edate = str(endDate)
		edate = ' to ' + edate[:4]+ edate[5:7] + edate[8:10] + ' ' + edate[11:19]
	return sdate + edate


def parseDataless(datalessLocation):
	#Reads the dataless seed with optional debug verbosity
	if debug:
		print 'Reading dataless for the ' + network + 'network'
	parsedDataless = Parser(datalessLocation + network + '.dataless')
	if debug:
		print 'Done reading dataless for the ' + network + 'network'
	return parsedDataless

main(parseDataless(datalessLocation))
