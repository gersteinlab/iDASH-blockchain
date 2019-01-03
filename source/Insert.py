'''
zInsert.py
Loads tab-separated text file data onto an existing data stream on an existing multichain


Usage: $ python zInsert.py <tab-separated-data>.txt <chainName> <streamName>
Example: python zInsert.py training1.txt testChain testStream

modified by GG on November 4th, 2018

'''


import sys
import subprocess
import time

#	Set variables, open file, split on new lines

lines = open(sys.argv[1]).read().splitlines()
chainName=sys.argv[2]
streamName=sys.argv[3]

#	Replace tabs with '\t' character (necessary for multichain--no spaces allowed on multichain keys)

def removeTabs(lines):
	newlines=[]
	for line in range(0, len(lines)):
		str=lines[line]
		str=" ".join(str.split())
		str=str.replace(' ', '\\t')
		newlines.append(str)
	return newlines

#newlines=removeTabs(lines)

#	Use subprocess to load lines of text to chain

for line in range(0, len(lines)):
        ss=lines[line]
        ss=" ".join(ss.split())
        ss=ss.replace(' ','\\t')
        newlines=ss
        subprocess.call(['multichain-cli', str('{}'.format(chainName)), 'publish',str('{}'.format(streamName)), str('{}'.format(newlines)),str('{}'.format(str(line).encode('hex')))])


import os
import psutil
process = psutil.Process(os.getpid())
print('\n\n Total memory in bytes:\n\n')
print(process.memory_info().rss)
