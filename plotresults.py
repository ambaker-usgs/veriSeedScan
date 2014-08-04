#!/usr/bin/env python

#Load in the plotting package
from matplotlib.pyplot import (figure,plot,xlabel,ylabel,title,legend,savefig,xlim,xticks)
from multiprocessing import Pool

#Read in the data file
network   = 'US'
filename  = network + 'results.csv'
outputDir = 'pics/'

debug     = True	


def main():
	stage    = stageSLC(contents)
	for index in range(len(stage)):
		plotContents(stage[index])
	# pool     = Pool(1)
	# pool.map(plotContents,stage)

def readFile():
	fob = open(filename, 'r')
	contents = fob.read().strip().split('\n')
	fob.close()
	return contents

def stageSLC(contents):
	stations  = []
	locations = []
	channels  = []
	stage     = []
	for index in range(len(contents)):
		line = contents[index].split(',')
		if line[0] not in stations:
			stations.append(line[0])
		if line[1] not in locations:
			locations.append(line[1])
		if line[2] not in channels:
			channels.append(line[2])
	for station in stations:
		for location in locations:
			for channel in channels:
				stage.append(' '.join([station,location,channel]))
	return stage

def plotContents(station):
	station = station.split(' ')
	sta     = station[0]
	loc     = station[1]
	chan    = station[2]
	power1  = []
	power2  = []
	power3  = []
	power4  = []
	date    = []
	for index in range(len(contents)):
		line = contents[index].split(',')
		if line[0] == sta and line[1] == loc and line[2] == chan:
			power1.append(float(line[5])  - float(line[6]))
			power2.append(float(line[7])  - float(line[8]))
			power3.append(float(line[9])  - float(line[10]))
			power4.append(float(line[11]) - float(line[12]))
			date.append(round(float(line[3]) + float(line[4])/365,6))
	print date
	#Now it is time to plot
	if len(date) > 0:
		if debug:
			print 'Plotting', ' '.join(station)
		plotme = figure()
		plot(date,power1, label='4 to 8 s. period')
		plot(date,power2, label='18 to 22 s. period')
		plot(date,power3, label='90 to 110 s. period')
		plot(date,power4, label='200 to 500 s. period')
		#Here we fix up the lengend and the labels
		legend(prop={'size':12})
		xlabel('Time (year)')
		ylabel('DQA - Computed Diff (dB)')
		xlim((min(date),max(date)))
		#Here we change the ticks to a more readable format
		xx,locs = xticks()
		ll = ['%.1f' % a for a in xx]
		xticks(xx,ll)
		title(network + ' ' + sta + ' ' + loc + ' ' + chan)
		savefig(outputDir + network + sta + loc + chan + '.png',format='png')
	else:
		print 'No date range for', ' '.join(station)
		


contents = readFile()

main()

# #Here is what we want to plot out of it
# sta = 'GTBY'
# loc = '00'
# chan = 'LHZ'
#
# fob = open(filename,'r')
# contents = fob.read().split('\n')
# fob.close()
#
# power1a = []
# power2a = []
# power3a = []
# power4a = []
# datea   = []
#
#
#
# #Get these values from the data file
# for line in contents:
# 	line = line.split(',')
# 	if line[0] == sta and line[1] == loc:
# 		comps = line[2]
# 		years = line[3]
# 		days  = line[4]
# 		power1 = float(line[5])  - float(line[6])
# 		power2 = float(line[7])  - float(line[8])
# 		power3 = float(line[9])  - float(line[10])
# 		power4 = float(line[11]) - float(line[12])
# 		#Append if they match the correct channel
# 		if comps == chan:
# 			datea.append(round(float(years) + float(days)/365,2))
# 			power1a.append(power1)
# 			power2a.append(power2)
# 			power3a.append(power3)
# 			power4a.append(power4)
#
#
# #Now it is time to plot
# plotme = figure(1)
# plot(datea,power1a,label='4 to 8 s. period')
# plot(datea,power2a,label='18 to 22 s. period')
# plot(datea,power3a,label='90 to 110 s. period')
# plot(datea,power4a,label='200 to 500 s. period')
# #Here we fix up the lengend and the labels
# legend(prop={'size':12})
# xlabel('Time (year)')
# ylabel('Diff (dB)')
# xlim((min(datea),max(datea)))
# #Here we change the ticks to a more readable format
# xx,locs = xticks()
# ll = ['%.1f' % a for a in xx]
# xticks(xx,ll)
# title(network + ' ' + sta + ' ' + loc + ' ' + chan)
# savefig(network + sta + loc + chan + '.jpg',format='jpeg')