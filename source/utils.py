

def putTimestamp(ts, val):
    return "{}:{}".format(ts, val).encode('hex')

def getTimestamp(s):
    ts, val = s.decode("hex").split(':',1)
    return ts, val


