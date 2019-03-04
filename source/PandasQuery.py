
'''
to do:
- handle no input? 
'''

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
import sys
import os
import subprocess
import json
import pandas as pd

columns=['TIMESTAMP','NODE','ID','REFID','USER','ACTIVITY','RESOURCE']

	################### Parses user-supplied arguments ###################
parser=argparse.ArgumentParser()
# required multichain options
parser.add_argument('-fn','--filename', dest='fileName', required=True, help='The name of the chain to be queried')

# timestamp filtering
parser.add_argument('-st','--startTime', type=int, dest='startTime',default=-sys.maxint, help='Unix timestamp start time of query')
parser.add_argument('-et','--endTime', type=int, dest='endTime',default=sys.maxint, help='Unix timestamp end time of query')

# things to query on
parser.add_argument('-n','--node'.upper(),'--node', dest='node',default=None,help='Node')
parser.add_argument('-i','--id'.upper(),'--id',dest='id',default=None,help='id')
parser.add_argument('-f','--refid'.upper(),'--refid',dest='refid',default=None,help='refid')
parser.add_argument('-u','--user'.upper(),'--user',dest='user',default=None, help='user')
parser.add_argument('-a','--activity'.upper(),'--activity',dest='activity',default=None,help='activity')
parser.add_argument('-r','--resource'.upper(), '--resource',dest='resource',default=None,help='resource')
parser.add_argument('-t','--timestamp'.upper(), dest='timestamp', required=False, help='A single timestamp')

# output ordering
parser.add_argument('-sb','--sortBy',dest='sortBy',default=None,help='Sort data by a specific column')
parser.add_argument('-ob','--orderBy',dest='orderBy',default='asc',help='Sort data in ascending or descending order')

def Query(qstr, df):
        subqueries=[]
        o=parser.parse_args(qstr)
        if o.timestamp: subqueries.append('TIMESTAMP == {}'.format(o.timestamp))
        if o.node:      subqueries.append('NODE == {}'.format(o.node))
        if o.id:        subqueries.append('ID == {}'.format(o.id))
        if o.refid:     subqueries.append('REFID == {}'.format(o.refid))
        if o.user:      subqueries.append('USER == {}'.format(o.user))
        if o.activity:  subqueries.append('ACTIVITY == "{}"'.format(o.activity))
        if o.resource:  subqueries.append('RESOURCE == "{}"'.format(o.resource))

        if o.startTime: subqueries.append('TIMESTAMP >= {}'.format(o.startTime))
        if o.endTime:   subqueries.append('TIMESTAMP <= {}'.format(o.endTime))

        if not subqueries:
                print("Error: you must provide at least one query element")
                sys.exit(1)

        query = ' and '.join(subqueries)
        print(query)
        res=df.query(query)
        if o.sortBy:
                res.sort_values(by='{}'.format(o.sortBy), ascending=(o.orderBy == 'asc'), inplace=True)
        print(res)
        print("\n{} rows".format(len(res)))

if __name__=='__main__':
        # this is an initial test parse; we'll reparse later if all is good
        
        o=parser.parse_args()
        df=pd.read_csv(o.fileName, sep='\t', names=columns)
        
        if len(sys.argv)>3: # non-interactive mode
                print(Query(sys.argv[1:], df))
        else:
                while True:
                        q=raw_input('> ')
                        print(q)
                        # prepend the chain and stream args from the original invocation
                        print(Query(sys.argv[1:]+q.split(), df))
                        

