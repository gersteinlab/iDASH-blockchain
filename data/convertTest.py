
import sys

'''
example input line
[('REFID', 6), ('ACTIVITY', 'FILE_ACCESS'), ('NODE', 1), ('ID', 51455)]"

convert to
"-f 6 -a FILE_ACCESS -n 1 -i 51455"
'''

tbl={
    "NODE":"-n",
    "ID":"-i",
    "REFID":"-f",
    "USER":"-u",
    "ACTIVITY":"-a",
    "RESOURCE":"-r",
    "TIMESTAMP":"-t",
    "STARTTIME":"-st",
    "ENDTIME":"-et",
    }

for line in open(sys.argv[1]):
    acc=[]
    l=eval(line.strip())
    for k, v in l:
        acc.append(tbl[k])
        acc.append(str(v))
    print (" ".join(acc))
    
