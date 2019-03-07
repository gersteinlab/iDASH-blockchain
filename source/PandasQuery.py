
'''
to do:
- handle no input? 
'''

'''
This script emulates what Query.py does by simply reading in the data, creating a pandas df, and using pandas to
perform the query.  It is used to validate the multichain query.
'''

import argparse
import sys
import pandas as pd

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

class Query():
        def __init__(self, fn):
                self.df=pd.read_csv(fn, sep='\t', names=columns)
                
        def query(self, qstr):
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
                res=self.df.query(query)
                
                if o.sortBy:
                        res.sort_values(by='{}'.format(o.sortBy), ascending=(o.orderBy == 'asc'), inplace=True)

                return res

# test driver
if __name__=='__main__':

        Q=Query(sys.argv[1])
        
        if len(sys.argv)>2: # non-interactive mode
                print(Q.query(sys.argv[2:]))
        else:
                while True:
                        q=raw_input('> ')
                        print(q)
                        # prepend the chain and stream args from the original invocation
                        print(Q.query(q.split()))
                        

