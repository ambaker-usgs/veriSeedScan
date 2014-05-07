#!/usr/bin/env python

#Importing packages

#Dataless seed reader
from obspy.xseed import Parser
#Timing conventions
from obspy.core import UTCDateTime
from time import gmtime, strftime

#Multiprocessing
from multiprocessing import Pool

#location of the dataless
datalessLoc = '/APPS/metadata/SEED/'

#For additional output
debug = False

#Network we are running this on
net = 'CU'
		

#Read in the dataless
sp = Parser(datalessLoc + net + '.dataless')

#Gets a list of channels for a station, network, year, and day
def getConChan(sp,net,sta,year,day):
	listOfChans =[]
	#Turn day string into a UTCtime type
	evetime = UTCDateTime(str(year) + str(day).zfill(3) + "T00:00:00.0")
	for cursta in sp.stations:
		for blkt in cursta:
#Checking if we are at the correct station
			if blkt.id == 50:
				stacall = blkt.station_call_letters.strip()
			if stacall == sta:
#Check if we have a channel blockette
				if blkt.id == 52:
#Checking for the time of the epoch					
					if type(blkt.end_date) is str:
						curdoy = strftime("%j",gmtime())
						curyear = strftime("%Y",gmtime())
						curtime = UTCDateTime(curyear + "-" + curdoy + "T00:00:00.0") 
						if blkt.start_date <= evetime:
							listOfChans.append(blkt.location_identifier + ' ' + blkt.channel_identifier + \
							' ' + blkt.channel_flags + ' ' + str(blkt.sample_rate))
					elif blkt.start_date <= evetime and blkt.end_date >= evetime:
						listOfChans.append(blkt.location_identifier + ' ' + blkt.channel_identifier + \
						' ' + blkt.channel_flags + ' ' + str(blkt.sample_rate))




	return listOfChans



def checkSta(sta):
#Here we are checking the sample rates to the station channel names
	days = list(xrange(1,366,1))
	for day in days:
		listOfChans = getConChan(sp,net,sta,year,day)
		for chan in listOfChans:
			chan = chan.split(' ')
			chanName = chan[1]
			if chanName[0] == 'L' and float(chan[3]) != 1:
				print 'Sample rate problem: ' + sta + ' ' + str(year) + str(day).zfill(3) + ' ' + chan[0] + ' ' + chan[1]
			if chanName[0] == 'V' and float(chan[3]) != .1:
				print 'Sample rate problem: ' + sta + ' ' + str(year) + str(day).zfill(3) + ' ' + chan[0] + ' ' + chan[1]
			if chanName[0] == 'B' and float(chan[3]) < 20:
				print 'Sample rate problem: ' + sta + ' ' + str(year) + str(day).zfill(3) + ' ' + chan[0] + ' ' + chan[1]
	return	


def getstalist(sp,etime,curnet):
#A function to get a station list
	stations = []
	for cursta in sp.stations:
#As we scan through blockettes we need to find blockettes 50 
		for blkt in cursta:
			if blkt.id == 50:
#Pull the station info for blockette 50
				stacall = blkt.station_call_letters.strip()
				if debug:
					print "Here is a station in the dataless" + stacall
				if type(blkt.end_effective_date) is str:
					curdoy = strftime("%j",gmtime())
					curyear = strftime("%Y",gmtime())
					curtime = UTCDateTime(curyear + "-" + curdoy + "T00:00:00.0") 

					if blkt.start_effective_date <= etime:
						stations.append(blkt.station_call_letters.strip())
				elif blkt.start_effective_date <= etime and blkt.end_effective_date >= etime:
					stations.append(blkt.station_call_letters.strip())	
	return stations	
		
#Here we actually run the code for a number of different years			
years = list(xrange(1995,2014,1))
for year in years:
	#Print out status of the year
	print str(year)
	#Setup defualt processor pool
	pool = Pool()
	staList = getstalist(sp,UTCDateTime(str(year) + str(1).zfill(3) + "T00:00:00.0"),'IU')
	#Here are the processors we are running
	pool.map(checkSta,staList)






	
