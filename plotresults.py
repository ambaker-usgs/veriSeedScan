#!/usr/bin/env python

#Load in the plotting package
from matplotlib.pyplot import (figure,plot,xlabel,ylabel,title,legend,savefig,xlim,xticks)

#Read in the data file
filetoplot='CUresults.csv'

#Here is what we want to plot out of it
sta = 'GTBY'
loc = '00'
chan = 'LHZ'

f=open(filetoplot,'r')

power1a = []
power2a = []
power3a = []
power4a = []
datea = []

#Get these values from the data file
for line in f:
	line = line.split(',')
	if line[0] == sta and line[1] == loc:
		comps = line[2]
		years = line[3]
		days = line[4]
		power1 = float(line[5]) - float(line[6])
		power2 = float(line[7]) - float(line[8])
		power3 = float(line[8]) - float(line[9])
		power4 = float(line[10]) - float(line[11])
		#Append if they match the correct channel
		if comps == chan:
			datea.append(round(float(years) + float(days)/365,2))
			power1a.append(power1)
			power2a.append(power2)
			power3a.append(power3)
			power4a.append(power4)
		
		
#Now it is time to plot		
plotme = figure(1)
plot(datea,power1a,label='4 to 8 s. period')
plot(datea,power2a,label='18 to 22 s. period')
plot(datea,power3a,label='90 to 110 s. period')
plot(datea,power4a,label='200 to 500 s. period')
#Here we fix up the lengend and the labels
legend(prop={'size':12})
xlabel('Time (year)')
ylabel('Diff (dB)')
xlim((min(datea),max(datea)))
#Here we change the ticks to a more readable format
xx,locs = xticks()
ll = ['%.1f' % a for a in xx]
xticks(xx,ll)
title(sta + ' ' + loc + ' ' + chan)
savefig(sta + loc + chan + '.jpg',format='jpeg')



