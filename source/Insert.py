
'''
columns=['TIMESTAMP','NODE','ID','REFID','USER','ACTIVITY','RESOURCE']
1522000002801	1	1	1	1	REQ_RESOURCE	MOD_UCSC_Genome_Bioinformatics
'''

import sys
import subprocess
import time

import utils

#	Set variables, open file, split on new lines

cols=['T','N','I','F','U','A','R']

fn = sys.argv[1]
chainName=sys.argv[2]
streamName=sys.argv[3]

logf=open("Insert.log", 'w+')

# first count the lines
lines=0
for l in open(fn):
    lines+=1

# now for real
for i, l in enumerate(open(fn)):
     vals = l.strip().split()
     timestamp=vals[0]
     rec=':'.join(vals)
     # this is the main record for the event
     cmd='multichain-cli {} publish {} {} {}'.format(chainName, streamName, timestamp, rec.encode('hex'))
     txid=subprocess.check_output(cmd, shell=True, stderr=logf)
     ts_txid = utils.putTimestamp(timestamp, txid.rstrip())
     # these are the aux records for the event, one per queriable field.  The   
     for c, v in zip(cols,vals):
         cmd='multichain-cli {} publish {} {}:{} {}'.format(chainName, streamName, c, v, ts_txid)
         dummy=subprocess.check_output(cmd, shell=True, stderr=logf)

     sys.stdout.write('\r{0:12}:{1:12}'.format(i, lines))
     sys.stdout.flush()

