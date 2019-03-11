
'''
to do:
- handle no filters?
- handle just -st -et?
'''

'''
Query.py
Queries an existing stream from a loaded multichain chain.
Written in python 2.7

Multichain chain and stream name are supplied via environment variables. They default to chain1, stream1.
export CHAIN=chain1
export STREAM=stream1

SYNTAX:
On command line (OCL): python Query.py -cn/--chainName [chain name] -sn/--streamName [stream name] [--queryflag query] 

OCL example: python Query.py --resource mod_flybase
OCL example2:python Query.py --user 1 --activity file_access
OCL example3:python Query.py -u 1 -a file_access -ob asc
OCL example4:python Query.py -u 1 -a file_access -orderBy ascending

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
-ts or --timestamp

Interactive Mode (IM): 
To get to interactive mode from shell/terminal, first run python -i Query.py 

Then just type in a query using the same syntax as above:
> -u 6 -n 5 -et 1522429976475

Help: 
python Query.py --help
'''


import argparse
import sys
import os
import subprocess
import json
import pandas as pd

import utils

columns=['TIMESTAMP','NODE','ID','REFID','USER','ACTIVITY','RESOURCE']

################### Parses user-supplied arguments ###################
parser=argparse.ArgumentParser()

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

        chainName = os.getenv("CHAIN", "chain1")
        streamName = os.getenv("STREAM", "stream1")

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
        ts_txids=[]
        for sq in subqueries:
                cmd='multichain-cli {} liststreamkeyitems {} {} false 100000'.format(chainName, streamName, sq)
                recs=json.loads(subprocess.check_output(cmd, shell=True, stderr=logf))
                # ts_txids is a list of sets, each holding all the ts_txids found for that query, e.g. -u 9
                ts_txids.append(set([r['data'] for r in recs]))
        matching_ts_txids=reduce(set.intersection, ts_txids)
        results=[]
        for ts_txid in matching_ts_txids:
                ts, txid=utils.getTimestamp(ts_txid)
                if o.startTime <= int(ts) and int(ts) <= o.endTime:
                        cmd='multichain-cli {} getstreamitem {} {}'.format(chainName, streamName, txid)
                        rec=json.loads(subprocess.check_output(cmd, shell=True, stderr=logf))
                        flds=rec['data'].decode("hex").split(":")
                        results.append(flds)

        df=pd.DataFrame(results, columns=columns)
        if o.sortBy: df.sort_values(by=o.sortBy, ascending=(o.orderBy=='asc'), inplace=True)
        return df

# test driver
if __name__=='__main__':
        if len(sys.argv)>1: # non-interactive mode
                df=Query(sys.argv[1:])
                print(df)
                print("\n{} rows".format(len(df)))
                
        else:
                while True:
                        q=raw_input('> ')
                        # prepend the chain and stream args from the original invocation
                        df=Query(q.split())
                        print(df)
                        print("\n{} rows".format(len(df)))
