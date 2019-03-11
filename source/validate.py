import sys
import PandasQuery as P
import Query as Q

dataFile=sys.argv[1]
testFile=sys.argv[2]

PQ=P.Query(dataFile)
    
for test in open(testFile):
    if test.startswith("#"): continue
    q=test.strip().split()
    pdres=len(PQ.query(q))
    mcres = len(Q.Query(q))
    print(test)
    if pdres != mcres:
        print ("ERROR! {}: {} {}".format(q, pdres, mcres))
