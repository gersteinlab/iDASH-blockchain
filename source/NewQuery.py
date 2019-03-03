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
import subprocess
import json
import pandas as pd

	################### Parses user-supplied arguments ###################
parser=argparse.ArgumentParser()
parser.add_argument('-cn','--chainName', dest='chainName', required=True, help='The name of the chain to be queried')
parser.add_argument('-sn','--streamName', dest='streamName', required=True, help='The name of the data stream to be queried')
parser.add_argument('-st','--startTime'.upper(), type=int, dest='startTime',default=-sys.maxint, help='Unix timestamp start time of query')
parser.add_argument('-et','--endTime'.upper(), type=int, dest='endTime',default=sys.maxint, help='Unix timestamp end time of query')
parser.add_argument('-n','--node'.upper(),'--node', dest='node',default=None,help='Node')
parser.add_argument('-i','--id'.upper(),'--id',dest='id',default=None,help='id')
parser.add_argument('-rd','--refid'.upper(),'--refid',dest='refid',default=None,help='refid')
parser.add_argument('-u','--user'.upper(),'--user',dest='user',default=None, help='user')
parser.add_argument('-a','--activity'.upper(),'--activity',dest='activity',default=None,help='activity')
parser.add_argument('-r','--resource'.upper(), '--resource',dest='resource',default=None,help='resource')
parser.add_argument('-sb','--sortBy',dest='sortBy',default=None,help='Sort data by a specific column')
parser.add_argument('-ob','--orderBy',dest='orderBy',default=None,help='Sort data in ascending or descending order')
parser.add_argument('-ts','--timestamp'.upper(), dest='timestamp', required=False, help='A single timestamp')


#o=parser.parse_args()

def Query(qstr):
        subqueries=[]
        o=parser.parse_args(qstr.split())
        if o.timestamp: subqueries.append('T:{}'.format(o.timestamp))
        if o.node:      subqueries.append('N:{}'.format(o.node))
        if o.id:        subqueries.append('I:{}'.format(o.id))
        if o.refid:     subqueries.append('F:{}'.format(o.refid))
        if o.user:      subqueries.append('U:{}'.format(o.user))
        if o.activity:  subqueries.append('A:{}'.format(o.activity))
        if o.resource:  subqueries.append('R:{}'.format(o.resource))

        txids=[]
        for sq in subqueries:
                cmd='multichain-cli {} liststreamkeyitems {} {}'.format(o.chainName, o.streamName, sq)
                recs=json.loads(subprocess.check_output(cmd, shell=True))
                # txids is a list of sets, each holding all the txids found for that query, e.g. -u 9
                txids.append(set([r['data'] for r in recs]))
        matching_txids=reduce(set.intersection, txids)
        for txid in matching_txids:
                cmd='multichain-cli {} getstreamitem {} {}'.format(o.chainName, o.streamName, txid)
                rec=json.loads(subprocess.check_output(cmd, shell=True))
                flds=rec['data'].decode("hex").split(":")
                ts=int(flds[0])
                if o.startTime < ts and ts < o.endTime:
                        print(" ".join(flds))

Query("-cn chain1 -sn stream1 -st 1522000017966 -et 1522000017966 -u 6")
#Query("-cn chain1 -sn stream1 -u 6 -i 5")
