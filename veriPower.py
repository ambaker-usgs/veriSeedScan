#!/usr/bin/env python

###################################################################################################
#
#veriPower.py
#
#This program compares DQA power levels to those computed by obspy
#
#We use the following functions
#
#getstations()
#querydatabase()
#computePSD()
#getPAZ2()
#runmulti()
#
###################################################################################################

import urllib2
import os
import psycopg2
import glob
import numpy

from obspy.core import read, UTCDateTime, Stream
from obspy.signal import pazToFreqResp
from obspy.signal.spectral_estimation import get_NLNM
from obspy.xseed import Parser
from time import gmtime, strftime
from matplotlib.mlab import psd
from math import pi
from multiprocessing import Pool	

def getstations(sp,dateval):
	#A function to get a station list along with channels and locations
	stations = []
	for cursta in sp.stations:
		#As we scan through blockettes we need to find blockettes 50 
		for blkt in cursta:
			if blkt.id == 50:
				#Pull the station info for blockette 50
				stacall = blkt.station_call_letters.strip()
				if debug:
					print 'Here is a station in the dataless: ' + stacall
			if blkt.id == 52:
				if blkt.channel_identifier[:2] == 'LH':
					if blkt.start_date <= dateval:
						if len(str(blkt.end_date)) == 0 or dateval <= blkt.end_date:
							stations.append(' '.join([stacall, blkt.location_identifier, blkt.channel_identifier]))
	print stations, len(stations)
	return stations



def querydatabase(sta,chan,loc,dateval):
	#Here is the database query
	metric1 = 'NLNMDeviationMetric:4-8'
	metric2 = 'NLNMDeviationMetric:18-22'
	metric3 = 'NLNMDeviationMetric:90-110'
	metric4 = 'NLNMDeviationMetric:200-500'
	#Here we query the database
	connString = open('db.config', 'r').readline()
	if debug: 
		print "Connecting to", connString.split(',')[0]
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
  AND tblMetric.name = %s
  AND tblDate.date = %s
  AND tblSensor.location = %s
  LIMIT 100
"""
	date = str(dateval.year) + "-" + str(dateval.month).zfill(2) + "-" + \
		str(dateval.day).zfill(2)
	cursor = conn.cursor()
	cursor.execute(queryString,(sta,chan,metric1,date,loc))
	psdvalue1 = round(float(cursor.fetchone()[4]),2)
	cursor.execute(queryString,(sta,chan,metric2,date,loc))
	psdvalue2 = round(float(cursor.fetchone()[4]),2)
	cursor.execute(queryString,(sta,chan,metric3,date,loc))
	psdvalue3 = round(float(cursor.fetchone()[4]),2)
	cursor.execute(queryString,(sta,chan,metric4,date,loc))
	psdvalue4 = round(float(cursor.fetchone()[4]),2)
	if debug:
		print 'Here is our spectra value1: ' + str(psdvalue1) 
		print 'Here is our spectra value2: ' + str(psdvalue2)
		print 'Here is our spectra value3: ' + str(psdvalue3)
		print 'Here is our spectra value4: ' + str(psdvalue4)
	conn.close()
	return psdvalue1, psdvalue2, psdvalue3, psdvalue4

def computePSD(sp,net,sta,loc,chan,dateval):
	#Here we compute the PSD
	lenfft = 5000
	lenol = 2500
	#Here are the different period bands.  These could be done as a dicitionary
	#if Adam B. wants to clean this up using a dicitionary that is fine with me
	permin1 = 4
	permax1 = 8
	permin2 = 18
	permax2 = 22
	permin3 = 90
	permax3 = 110
	permin4 = 200
	permax4 = 500

	#Get the response and compute amplitude response	
	paz=getPAZ2(sp,net,sta,loc,chan,dateval)
	respval = pazToFreqResp(paz['poles'],paz['zeros'],paz['sensitivity']*paz['gain'], \
		t_samp = 1, nfft = lenfft,freq=False)[1:]
	respval = numpy.absolute(respval*numpy.conjugate(respval))

	#Get the data to compute the PSD
	readDataString = '/xs0/seed/' + net + '_' + sta + '/' + str(dateval.year) + '/' + \
		str(dateval.year) + '_' + str(dateval.julday).zfill(3) + '_' + net + '_' + \
		sta + '/' + loc + '_' + chan + '*.seed'
	datafiles = glob.glob(readDataString)
	st = Stream()
	for datafile in datafiles:
		st += read(datafile)
	st.merge(method=-1)

	#Compute the PSD
	cpval,fre = psd(st[0].data,NFFT=lenfft,Fs=1,noverlap=lenol,scale_by_freq=True)
	per = 1/fre[1:]
	cpval = 10*numpy.log10(((2*pi*fre[1:])**2)*cpval[1:]/respval)
	perminind1 = numpy.abs(per-permin1).argmin()
	permaxind1 = numpy.abs(per-permax1).argmin()
	perminind2 = numpy.abs(per-permin2).argmin()
	permaxind2 = numpy.abs(per-permax2).argmin()
	perminind3 = numpy.abs(per-permin3).argmin()
	permaxind3 = numpy.abs(per-permax3).argmin()
	perminind4 = numpy.abs(per-permin4).argmin()
	permaxind4 = numpy.abs(per-permax4).argmin()
	perNLNM,NLNM = get_NLNM()
	perNLNMminind1 = numpy.abs(perNLNM-permin1).argmin()
	perNLNMmaxind1 = numpy.abs(perNLNM-permax1).argmin()
	perNLNMminind2 = numpy.abs(perNLNM-permin2).argmin()
	perNLNMmaxind2 = numpy.abs(perNLNM-permax2).argmin()
	perNLNMminind3 = numpy.abs(perNLNM-permin3).argmin()
	perNLNMmaxind3 = numpy.abs(perNLNM-permax3).argmin()
	perNLNMminind4 = numpy.abs(perNLNM-permin4).argmin()
	perNLNMmaxind4 = numpy.abs(perNLNM-permax4).argmin()

	cpval1 = round(numpy.average(cpval[permaxind1:perminind1]) - \
		numpy.average(NLNM[perNLNMmaxind1:perNLNMminind1]),2)
	cpval2 = round(numpy.average(cpval[permaxind2:perminind2]) - \
		numpy.average(NLNM[perNLNMmaxind2:perNLNMminind2]),2)
	cpval3 = round(numpy.average(cpval[permaxind3:perminind3]) - \
		numpy.average(NLNM[perNLNMmaxind3:perNLNMminind3]),2)
	cpval4 = round(numpy.average(cpval[permaxind4:perminind4]) - \
		numpy.average(NLNM[perNLNMmaxind4:perNLNMminind4]),2)
	return cpval1, cpval2, cpval3,cpval4


def getPAZ2(sp,net,sta,loc,chan,dateval):
	#Here we get the response information 
	debuggetPAZ2 = False
        data = {}
	station_flag = False
	channel_flag = False
	for statemp in sp.stations:
		for blockette in statemp:
			if blockette.id == 50:
				station_flag = False
				if net == blockette.network_code and sta == blockette.station_call_letters:
					station_flag = True
					if debuggetPAZ2:
						print 'We found the station blockettes'
			elif blockette.id == 52 and station_flag:
				channel_flag = False
				if blockette.location_identifier == loc and blockette.channel_identifier == chan:
					if debuggetPAZ2:
						print 'We are in the location and channel blockette'
						print 'End date: ' + str(blockette.end_date)
						print 'Start date: ' + str(blockette.start_date)
					if type(blockette.end_date) is str:
						curdoy = strftime("%j",gmtime())
						curyear = strftime("%Y",gmtime())
						curtime = UTCDateTime(curyear + "-" + curdoy + "T00:00:00.0") 
						if blockette.start_date <= dateval:
							channel_flag = True
							if debuggetPAZ2:
								print 'We found the channel blockette'
					elif blockette.start_date <= dateval and blockette.end_date >= dateval:
						channel_flag = True
						if debuggetPAZ2:
							print 'We found the channel blockette'
			elif blockette.id == 58 and channel_flag and station_flag:
				if blockette.stage_sequence_number == 0:
					data['sensitivity'] = blockette.sensitivity_gain
				elif blockette.stage_sequence_number == 1:
					data['seismometer_gain'] = blockette.sensitivity_gain
				elif blockette.stage_sequence_number == 2:
					data['digitizer_gain'] = blockette.sensitivity_gain
				elif blockette.stage_sequence_number == 3:
					if not 'digitizer_gain' in data.keys():
						data['digitizer_gain'] = blockette.sensitivity_gain
					else:
						data['digitizer_gain'] *= blockette.sensitivity_gain
			elif blockette.id == 53 and channel_flag and station_flag:
				if not 'gain' in data.keys():
					data['gain'] = blockette.A0_normalization_factor
				else:
					data['gain'] *= blockette.A0_normalization_factor
				if debuggetPAZ2:
					print 'Here is the gain: ' + str(blockette.A0_normalization_factor)
				if not 'poles' in data.keys():
					data['poles'] = []
				if not blockette.transfer_function_types in set(['A','B']):
					msg = 'Only supporting Laplace transform response ' + \
					'type. Skipping other response information.'
					warnings.warn(msg, UserWarning)
					continue

				if blockette.transfer_function_types == 'B':
					schan = 1
				else:
					schan = 1
				if debuggetPAZ2:
					print 'Here are the number of poles: ' + str(blockette.number_of_complex_poles)
					print 'Here are the number of zeros: ' + str(blockette.number_of_complex_zeros)
				for i in range(blockette.number_of_complex_poles):
					p = complex(schan*blockette.real_pole[i], schan*blockette.imaginary_pole[i])
					data['poles'].append(p)
				if not 'zeros' in data.keys():
					data['zeros'] = []
				for i in range(blockette.number_of_complex_zeros):
					z = complex(schan*blockette.real_zero[i], schan*blockette.imaginary_zero[i])
					data['zeros'].append(z)
        return data

def runmulti(slc):
	slc = slc.split()
	sta = slc[0]
	loc = slc[1]
	chan = slc[2]
	#Here we try to get the database values as well as compute the values
	try:
		dbres1, dbres2, dbres3, dbres4 = querydatabase(sta, chan, loc, dateval)
		ppsdvalue1, ppsdvalue2, ppsdvalue3, ppsdvalue4 =computePSD(sp,net,sta,loc,chan,dateval)
		#Now we write the results
		f = open(net + 'results.csv','a')
		f.write(sta + ',' + loc + ',' + chan + ',' + str(dateval.year) + ',' + str(dateval.julday).zfill(3) + ',' + \
			str(dbres1) + ',' + str(ppsdvalue1) + ',' + \
			str(dbres2) + ',' + str(ppsdvalue2) + ',' + \
			str(dbres3) + ',' + str(ppsdvalue3) + ',' + \
			str(dbres4) + ',' + str(ppsdvalue4) + '\n')
		f.close()
	except:
		print 'Problem with: ' + sta + ' ' + loc + ' ' + chan + ' ' + str(dateval.year) + \
			' ' + str(dateval.julday).zfill(3)
	return

print 'Scan started on', strftime('%Y-%m-%d %H:%M:%S', gmtime()), 'UTC'

if __name__ == '__main__':
	#Main function

	debug = True

	#Here is the network we read the dataless from
	net ='US'
	datalessloc = '/APPS/metadata/SEED/'
	try:
		sp = Parser(datalessloc + net + ".dataless")
	except:
		print "Can not read the dataless."
		exit(0)
	
	#The start day we are going to get power values from
	datevalstart = UTCDateTime("2014-150T00:00:00.0")
	#How many days we plan to run through
	daystogo = 1
	for ind in range(daystogo):
		dateval = datevalstart + ind*24*60*60
		#Get station list with location and channels for the current day
		stationList = getstations(sp,dateval)
		pool = Pool(12)
		#Run the function for that day
		# for station in stationList:
		# 	runmulti(station)
		pool.map(runmulti,stationList)

print 'Scan terminated on', strftime('%Y-%m-%d %H:%M:%S', gmtime()), 'UTC'