'''
zQuery.py
Queries an existing stream from a loaded multichain chain.
Written in python 2.7

SYNTAX:
On command line (OCL): python zQuery.py -cn/--chainName [chain name] -sn/--streamName [stream name] [--queryflag query] 

OCL example: python zFunctionalQuery.py --chainName testingChain -streamName testingStream -resource mod_flybase
OCL example2:python zFunctionalQuery.py -cn testingChain -sn testingStream -user 1 -activity file_access
OCL example3:python zFunctionalQuery.py -cn testingChain -sn testingStream -u 1 -a file_access -ob asc
OCL example4:python zFunctionalQuery.py -cn testingChain -sn testingStream -u 1 -a file_access -orderBy ascending

queryflag options (can use short or long flag)
-cn or --chainName
-sn or --streamName
-st or --startTime
-et or --endTime
-n or --node
-i or --id
-rd or --refid
-u or --user
-a  or --activity
-r or --resource
-sb or --sortBy
-ob or --orderBy
ts or --timestamp


Interactive Mode (IM): 
To get to interactive mode from shell/terminal, first run python -i zQuery.py -cn [chain name] -sn [stream name]

***Once in interactive mode, can use only long flags without '-' ***

Once in interactive mode: rows.query('queryflag query') 

IM example: rows.query('user 1')
IM example2:rows.query('refid 9')
IM example3:rows.query('node 1 activity req_resource')  


Help: 
python zFunctionalQuery.py -h 
	OR
python zFunctionalQuery.py --help
'''


import argparse
import subprocess
import json
import pandas as pd

	################### Parses user-supplied arguments ###################
parser=argparse.ArgumentParser()
parser.add_argument('-cn','--chainName', dest='chainName', required=True, help='The name of the chain to be queried')
parser.add_argument('-sn','--streamName', dest='streamName', required=True, help='The name of the data stream to be queried')
parser.add_argument('-st','--startTime'.upper(), dest='startTime',default=None, help='Unix timestamp start time of query')
parser.add_argument('-et','--endTime'.upper(), dest='endTime',default=None, help='Unix timestamp end time of query')
parser.add_argument('-n','--node'.upper(),'--node', dest='node',default=None,help='Node')
parser.add_argument('-i','--id'.upper(),'--id',dest='id',default=None,help='id')
parser.add_argument('-rd','--refid'.upper(),'--refid',dest='refid',default=None,help='refid')
parser.add_argument('-u','--user'.upper(),'--user',dest='user',default=None, help='user')
parser.add_argument('-a','--activity'.upper(),'--activity',dest='activity',default=None,help='activity')
parser.add_argument('-r','--resource'.upper(), '--resource',dest='resource',default=None,help='resource')
parser.add_argument('-sb','--sortBy',dest='sortBy',default=None,help='Sort data by a specific column')
parser.add_argument('-ob','--orderBy',dest='orderBy',default=None,help='Sort data in ascending or descending order')
parser.add_argument('-ts','--timestamp'.upper(), dest='timestamp', required=False, help='A single timestamp')


o=parser.parse_args()


class Chain():

        def __init__(self, o):
	###################Initialize the Chain object###################
                self.chainName=o.chainName
                self.streamName=o.streamName
		self.startTime=o.startTime
		self.endTime=o.endTime
		self.node=o.node
                self.id=o.id
		self.refid=o.refid
                self.user=o.user
                self.activity=o.activity
                self.resource=o.resource
		self.timestamp=o.timestamp
		self.dict={k:v for k,v in o._get_kwargs() if v != None}
		self.noneDict={k:v for k,v in o._get_kwargs()}
		if o.sortBy==None:
			self.sortBy='TIMESTAMP'
		else:
			self.sortBy=o.sortBy.upper()	
		if o.orderBy==None or o.orderBy.upper() in [True, 'ASC','ASCENDING']:
			self.orderBy=True
		else:
			self.orderBy=False
	################### Create the dataframe ###################
		columns=['TIMESTAMP','NODE','ID','REFID','USER','ACTIVITY','RESOURCE']
		array=[]
		string='multichain-cli {} liststreamkeys {}'.format(self.chainName, self.streamName)
		rawItems=rawItems=subprocess.check_output([i for i in string.split()])
		#rawItems=subprocess.check_output(['multichain-cli',str('{}'.format(self.chainName)),'liststreamitems',str('{}'.format(self.streamName)), 'false','10000000'])	
		items=json.loads(str(rawItems))
		for item in range(0, len(items)):
			array.append(items[item]['key'])
		df=pd.DataFrame([sub.split('\t') for sub in array])
		df.columns=columns
		df=df.applymap(lambda x: x.upper())
		for key in ['TIMESTAMP']:
			df[key]=map(lambda x: int(x), df[key])
		self.df=df
		
	################### Examples to help user with syntax ###################	
	def examples(self):
		print('\n Field options for querying: {}\n'.format(o.__dict__.keys()))
		print('\n Syntax: rows.query(\'example string\')\n ')
		print('\n Example: rows.query(\'user 1 node 1\')\n ')
			
	################### Called by program (does not require interactive mode) ###################
	def firstQuery(self):
		inputDict=self.dict
		unQueriables=['chainName','streamName','sortBy','orderBy']
		queryDict=self.dict
		for k in unQueriables:
			if k in queryDict.keys():
				del queryDict[k]
		if queryDict=={}:
			print('\n No matches (or no field queried). Returning truncated full chain. If in interactive mode, please use rows.examples() \n')
			return self.df.sort_values(by='{}'.format(self.sortBy.upper()), ascending=self.orderBy)
		newValues=['{0}=={1}{2}{1}'.format(key.upper(),'\\\'',value.upper()) for key,value in queryDict.items()]
		newDict=dict(zip(queryDict.keys(),newValues))
		if 'startTime' in queryDict.keys() and 'endTime' in queryDict.keys():
			newDict['TIMESTAMP']='{0}<=TIMESTAMP<={1}'.format(queryDict['startTime'],queryDict['endTime'])
			del newDict['startTime']
			del newDict['endTime']
		if 'startTime' in queryDict.keys() and 'endTime' not in queryDict.keys():
			newDict['TIMESTAMP']='{0}<=TIMESTAMP<={1}'.format(queryDict['startTime'],queryDict['startTime'])
			del newDict['startTime']
		if 'endTime' in queryDict.keys() and 'startTime' not in queryDict.keys():
			newDict['TIMESTAMP']='{0}<=TIMESTAMP<={1}'.format(queryDict['endTime'],queryDict['endTime'])
			del newDict['endTime']
		array=['{}'.format(newDict.values()[i]) for i in range(0, len(newDict.values()))]
		seperator=' and '
		queryStr=seperator.join(array)
		queryStr='\'{}\''.format(queryStr)
		result=self.df.query('{}'.format(eval(queryStr)))
		#print('Check it out with interactive mode, multiple queries can be performed on this instance of the chain. Please use rows.examples() in interactive mode to see examples.')
		return result.sort_values(by='{}'.format(self.sortBy), ascending=self.orderBy)


	################### For interactive mode queries ###################	
	def query(self,str):
		queriables=['startTime','endTime','node','id','refid','user','activity','resource']
		sortables={'*timestamp':'TIMESTAMP','*node':'NODE','*id':'ID','*refid':'REFID','*user':'USER','*activity':'ACTIVITY','*resource':'RESOURCE'}
		orderables=['-d','--descending','des','DES','DESCENDING','descending']
		queryOptionsDict={k:None for k in queriables}
		k=[x for x in str.split() if x in queryOptionsDict.keys()]
		v=[y for y in str.split() if y not in queryOptionsDict.keys()]
		d=dict(zip(k,v)) #Our query dictionary
		sortByArray=[x for x in str.split() if x in sortables.keys()]
		#print sortByArray
		orderBy=[x for x in str.split() if x in orderables]
		if orderBy!=[]:
			orderBy=False
		else:
			orderBy=True
		if sortByArray!=[]:
			sortBy=sortables[sortByArray[0]]
		else:
			sortBy='TIMESTAMP'
		for k in d.keys():
			queryOptionsDict[k]=d[k]
                queryDict=queryOptionsDict
		for k in queryDict.keys():
			if queryDict[k]==None or k not in queriables:
				del queryDict[k]
                if queryDict=={}:
                        print('\n Full chain. If in interactive mode, please use rows.examples() \n')
                        print sortBy
			print orderBy
			return self.df.sort_values(by='{}'.format(sortBy), ascending=orderBy)
                newValues=['{0}=={1}{2}{1}'.format(key.upper(),'\\\'',value.upper()) for key,value in queryDict.items()]
                newDict=dict(zip(queryDict.keys(),newValues))
                if 'startTime' in queryDict.keys() and 'endTime' in queryDict.keys():
                        newDict['TIMESTAMP']='{0}<=TIMESTAMP<={1}'.format(queryDict['startTime'],queryDict['endTime'])
                        del newDict['startTime']
                        del newDict['endTime']
                array=['{}'.format(newDict.values()[i]) for i in range(0, len(newDict.values()))]
                queryStr=' and '.join(array)
                queryStr='\'{}\''.format(queryStr)
                result=self.df.query('{}'.format(eval(queryStr)))
		#print sortBy
		#print orderBy
                return result.sort_values(by='{}'.format(sortBy), ascending=orderBy)


if __name__=='__main__':
        rows=Chain(o)


#print (rows.firstQuery().to_string()) # If you want to show all rows and columns (not advised if you are querying something that appears in thousands of rows, i.e.; Node)
print (rows.firstQuery())

#	Memory Cost
import os
import psutil
process = psutil.Process(os.getpid())
print('\n\n Total memory in bytes:\n\n')
print(process.memory_info().rss)

