import sys
import PandasQuery as P
import Query as Q

dataFile=sys.argv[1]
testFile=sys.argv[2]

# first count the lines
lines=0
for l in open(testFile):
    lines+=1

PQ=P.Query(dataFile)
    
for i, test in enumerate(open(testFile)):
    if test.startswith("#"): continue
    test=test.strip()
    q=test.split()
    pdres=len(PQ.query(q))
    mcres = len(Q.Query(q))

    print("{} {} {}".format(mcres, pdres, test))
    #sys.stdout.write('\r{0:12}:{1:12}'.format(i, lines))
    if mcres != pdres:
        print ("ERROR! {}: {} {}".format(q, mcres, pdres))
