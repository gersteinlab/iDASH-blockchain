
'''
columns=['TIMESTAMP','NODE','ID','REFID','USER','ACTIVITY','RESOURCE']
1522000002801	1	1	1	1	REQ_RESOURCE	MOD_UCSC_Genome_Bioinformatics
'''

import sys
import subprocess
import time

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
     cmd='multichain-cli {} publish {} {} {}'.format(chainName, streamName, timestamp, rec.encode('hex'))
     txid=subprocess.check_output(cmd, shell=True, stderr=logf)
     for c, v in zip(cols,vals):
         cmd='multichain-cli {} publish {} {}:{} {}'.format(chainName, streamName, c, v, txid)
         dummy=subprocess.check_output(cmd, shell=True, stderr=logf)

     sys.stdout.write('\r{0:12}:{1:12}'.format(i, lines))
     sys.stdout.flush()

