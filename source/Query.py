
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
parser.add_argument('-cn','--chainName', dest='chainName', required=True, help='The name of the chain to be queried')
parser.add_argument('-sn','--streamName', dest='streamName', required=True, help='The name of the data stream to be queried')

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

def Query(qstr):
        subqueries=[]
        o=parser.parse_args(qstr)
        if o.timestamp: subqueries.append('T:{}'.format(o.timestamp))
        if o.node:      subqueries.append('N:{}'.format(o.node))
        if o.id:        subqueries.append('I:{}'.format(o.id))
        if o.refid:     subqueries.append('F:{}'.format(o.refid))
        if o.user:      subqueries.append('U:{}'.format(o.user))
        if o.activity:  subqueries.append('A:{}'.format(o.activity))
        if o.resource:  subqueries.append('R:{}'.format(o.resource))

        if not subqueries:
                print("Error: you must provide at least one query element")
                sys.exit(1)

        logf=open("Query.log", "w+")
        txids=[]
        for sq in subqueries:
                cmd='multichain-cli {} liststreamkeyitems {} {} false 100000'.format(o.chainName, o.streamName, sq)
                recs=json.loads(subprocess.check_output(cmd, shell=True, stderr=logf))
                # txids is a list of sets, each holding all the txids found for that query, e.g. -u 9
                txids.append(set([r['data'] for r in recs]))
        matching_txids=reduce(set.intersection, txids)
        results=[]
        for txid in matching_txids:
                cmd='multichain-cli {} getstreamitem {} {}'.format(o.chainName, o.streamName, txid)
                rec=json.loads(subprocess.check_output(cmd, shell=True, stderr=logf))
                flds=rec['data'].decode("hex").split(":")
                timestamp=int(flds[0])
                if o.startTime <= timestamp and timestamp <= o.endTime:
                        results.append(flds)

        df=pd.DataFrame(results, columns=columns)
        if o.sortBy: df.sort_values(by=o.sortBy, ascending=(o.orderBy=='asc'), inplace=True)
        return df

if __name__=='__main__':
        # this is an initial test parse; we'll reparse later if all is good
        o=parser.parse_args()
        
        if len(sys.argv)>5: # non-interactive mode
                df=Query(sys.argv[1:])
                print(df)
                print("\n{} rows".format(len(df)))
                
        else:
                while True:
                        q=raw_input('> ')
                        # prepend the chain and stream args from the original invocation
                        df=Query(sys.argv[1:]+q.split())
                        print(df)
                        print("\n{} rows".format(len(df)))
                        
                        
#res=Query("-cn chain1 -sn stream1 -u 6 -sb TIMESTAMP")
#print(res)
#Query("-cn chain1 -sn stream1 -u 6 -i 5")
