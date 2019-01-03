'''
testing100Queries.py


Takes file containing 100 random queries (100queries.txt) and uses string concatenation to run zQuery.py on them

Usage: python testing100Queries.py <chainName> <streamName> <100klinesRandomQueries.txt>

Example usage: python testing100Queries.py 100k 100k 100klinesRandomQueries.txt
Example usage: python testing100Queries.py 200k 200k 100klinesRandomQueries.txt


'''


import subprocess
import sys
import time

chainName=sys.argv[1]

streamName=sys.argv[2]

fp=sys.argv[3]
rows=open(fp).read().splitlines()

rows=[eval(row) for row in rows]

commands=[]
for i in xrange(0,len(rows)):
	command='python zQuery.py -cn {} -sn {}'.format(chainName,streamName)
	for j in xrange(0, len(rows[i])):
		command +=' --{} {}'.format(rows[i][j][0],rows[i][j][1])
	commands.append(command)

commands=[i.split(' ') for i in commands]


for i in commands:
	print('\n##################### \n Query: {}\n'.format(i[6:]))
	start_time=time.time()
	myout=subprocess.check_output(i)
	print myout
	print("\n%s seconds\n " % (time.time() - start_time))	
	print ('\n\n#####################\n ')

