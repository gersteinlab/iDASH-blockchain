

import random
import sys
import pandas as pd
import numpy as np


colnames = "TIMESTAMP NODE ID REFID USER ACTIVITY RESOURCE".split()
columns = "-t -n -i -f -u -a -r".split()

df = pd.read_csv(sys.argv[1], sep='\t')
df.columns = columns
values=[df[k].unique() for k in columns]

for i in range(5,7):
    values[i]=[v.upper() for v in values[i]]

ncols=len(columns)
mints, maxts=min(values[0]), max(values[0])

for i in range(int(sys.argv[2])):

    row=df.iloc[random.randrange(len(df)),]
    cols=random.sample(range(ncols), random.randint(1,ncols))
    qry=[]
    for c in cols:
        qry.extend([columns[c], str(row[c])])

    st=random.randrange(mints, maxts)
    et=random.randrange(st, maxts)

    if random.random() < 0.25:
        qry.extend(["-st", str(st)])
    if random.random() < 0.25:
        qry.extend(["-et", str(et)])

    if random.random() < 0.25:
        qry.extend(["-sb", random.choice(colnames)])
        if random.random() < 0.5:
            qry.extend(["-ob", random.choice(['asc', 'des'])])
    print(" ".join(qry))
        
