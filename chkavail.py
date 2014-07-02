#!/usr/bin/env python

import glob
import os
import argparse

from obspy import UTCDateTime, read
from obspy.fdsn import Client
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


client = Client("IRIS")
chan = '*[BL]H*'



def args():
#This function parses the command line arguments
	parser = argparse.ArgumentParser(description='Code to compare data availability')

	parser.add_argument('-n', action = "store",dest="net", \
		default = "*", help="Network to check: NN", type = str, required = True)

	parser.add_argument('-s', action = "store",dest="sta", \
		default = "*", help="Station to check: SSSS", type = str, required = False)

	parser.add_argument('-y', action = "store",dest="year", \
		default = 2014, help="Year to check: YYYY", type = int, required = True)

	parser.add_argument('-sd', action = "store",dest="sday", \
		default = 1, help="Start day: DDD", type = int, required = True)

	parser.add_argument('-ed', action = "store",dest="eday", \
		default = 1, help="End day: DDD", type = int, required = True)

#Here is verbose mode
	parser.add_argument('-d','--debug',action = "store_true",dest="debug", \
		default = False, help="Run in debug mode")

#Here is the quality flag for Tyler
	parser.add_argument('-q',action = "store",dest = "quality", \
		default = "M", help = "Data quality type: D,Q,...")

	parserval = parser.parse_args()
	return parserval


def checkAvail(string):
#Here is the heart of the program where the availability gets checked

#Setup a path to decide if it is on /xs1 or /xs0	
	if net in set(["IU","IC","CU"]):
		netpath = "xs0"
	else:
		netpath = "xs1"

	allAvailString=[]

#Here we parse the function arguments
	string = string.split(",")
	year = int(string[0])
	jday = int(string[1])


	startTime = UTCDateTime(str(year) + str(jday).zfill(3) + "T00:00:00.000")
	endTime = startTime + 24*60*60

#Setup the /tr1 string and glob it
	globString = '/tr1/telemetry_days/' + net + '_' + sta +'/' + str(year) + '/' + \
		str(year) + '_' + str(jday).zfill(3) + '/' + chan
	dataOnTr1 = glob.glob(globString)
	
	if debug:
		print(globString)
		print(dataOnTr1)
	

	for index, dataTrace in enumerate(dataOnTr1):

		try:
#tr1 availability
			trtr1 = read(dataTrace)
			trtr1.merge()
			availtr1 = 0
			for tr in trtr1:
				availtr1 += tr.stats.npts / (24*60*60*tr.stats.sampling_rate)
		
#Here is the xs0 availability
			trxs0 = dataTrace.replace('/tr1/telemetry_days','/' + netpath + '/seed')
			trxs0 = trxs0.replace(str(year) + '_' + str(jday).zfill(3), \
				str(year) + '_' + str(jday).zfill(3) + '_' + net + '_' + sta)
			trxs0 = read(trxs0)
			trxs0.merge()
			availxs0 = 0
			for tr in trxs0:
				availxs0 += tr.stats.npts / (24*60*60*tr.stats.sampling_rate)		
			if debug:
				print netpath + ' avail: ' + str(availxs0*100) + '%'

#Here is the IRIS availability
			if availxs0 < 0.999 or availtr1 < 0.999:
				trIRIS = client.get_waveforms(net,trxs0[0].stats.station, \
					trxs0[0].stats.location, trxs0[0].stats.channel, \
					startTime,endTime,quality=qualval)
				trIRIS.merge()
				availIRIS = 0
				for tr in trIRIS:
					availIRIS += tr.stats.npts / (24*60*60*tr.stats.sampling_rate)
				if debug:
					print 'IRIS avail: ' + str(availIRIS*100) + '%'		
				availIRIS = round(availIRIS,3)
				availxs0 = round(availxs0,3)
				availtr1 = round(availtr1,3)
				if availIRIS != availxs0 or availIRIS != availtr1:
					availString = trIRIS[0].stats.station + ' ' + trIRIS[0].stats.location + \
					' ' + trIRIS[0].stats.channel + ' ' + str(year) + ' ' + \
					str(jday).zfill(3) + ' IRIS: ' + str(availIRIS*100) + \
					' ' + netpath + ': ' + str(availxs0*100) + ' tr1: ' + str(availtr1*100)
					allAvailString.append(availString)
					print availString
				
		except:
			print 'Problem with: ' + dataTrace
	f = open("avail" + str(year) + net,"a")
	for curavail in allAvailString:	
		print 'Writing to file'
		f.write(curavail + "\n")
	f.close()	
	
	return	





#Here are the parser value arguments
#We make the global to get them in the function
parserval = args()
net = parserval.net
year = parserval.year
sta = parserval.sta
debug = parserval.debug
sday = parserval.sday
eday = parserval.eday
qualval = parserval.quality




if os.path.isfile("avail" + str(year) + net):
	os.remove("avail" + str(year) + net)

#Here we loop over the days
sendToavail=[]
for day in xrange(sday,eday,1):
	sendToavail.append(str(year) + "," + str(day).zfill(3))
	
#Hwere we run everything as a multi-process
pool = Pool()
pool.map(checkAvail,sendToavail)
















