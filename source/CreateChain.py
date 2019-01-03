'''
zCreateChain.py

usage: python zCreateChain.py -cn/--chainName [chain name] -sn/--streamName [stream name] optional: -s/--server [user@server.example.org]

Example: python zCreateChain.py -cn testing2Chain -sn testing2Stream
Example: python zCreateChain.py -cn playChain -sn playStream -s meg98@idrac.gersteinlab.org

'''


import argparse
import subprocess
import time
import json
import os
import signal
import getpass


def handler(signum, frame):
	raise Exception('Done')


p=argparse.ArgumentParser()
p.add_argument('-cn','--chainName', default=None, help='Name of the chain')
p.add_argument('-sn','--streamName', default=None ,help='Name of the stream')
p.add_argument('-s' ,'--server', type=str, default=None, help='user@sample.ssh.org')

args,leftovers=p.parse_known_args()

o=p.parse_args()

subprocess.call(['multichain-util','create',str('{}'.format(o.chainName))])
if args.chainName is None:
	quit()

if args.streamName is None:
	quit()


if args.server is None:
	daemonCommand='multichaind {} -daemon'.format(o.chainName)
	subprocess.call(daemonCommand.split())
	time.sleep(1)
else:
	password=getpass.getpass('Password for {}:'.format(o.server))
        subprocess.call(['multichaind',str('{}'.format(o.chainName)),'-daemon'])
        addressCommand='multichain-cli {} getinfo'.format(o.chainName)
        address=subprocess.check_output(addressCommand.split())
        items=json.loads(str(address))
        newAddress=(items['nodeaddress']).strip()
        serverAddressCommand='sshpass -p {} ssh -o StrictHostKeyChecking=no {} multichaind {} | grep send'.format(password,o.server,newAddress)
        serverAddress=subprocess.check_output(serverAddressCommand.split())
        grabCommand=serverAddress.strip('\n')
        subprocess.call(grabCommand.split())
        connectCommand='sshpass -p {} ssh -o StrictHostKeyChecking=no {} multichaind {} -daemon'.format(password,o.server,o.chainName)
	signal.signal(signal.SIGALRM, handler)
	signal.alarm(10)
	try:
		subprocess.call(connectCommand.split())
	except Exception, exc:
		print exc


print('\n\n Chain {} successfully created!\n\n'.format(o.chainName))


createStreamCommand='multichain-cli {} create stream {} true'.format(o.chainName,o.streamName)
subscribeCommand='multichain-cli {} subscribe {}'.format(o.chainName,o.streamName)
#subprocess.call(createStreamCommand.split())
#subprocess.call(subscribeCommand.split())
#print('done')

if args.server is None:
	subprocess.call(createStreamCommand.split())
	time.sleep(1)
	subprocess.call(subscribeCommand.split())
else:
	subprocess.call(createStreamCommand.split())
	time.sleep(1)
	subprocess.call(subscribeCommand.split())
	subscribeRemoteCommand='sshpass -p {} ssh -o StrictHOstKeyChecking=no {} multichain-cli {} subscribe {}'.format(password,o.server,o.chainName,o.streamName)
	subprocess.call(subscribeRemoteCommand.split())

print('Stream {} created on chain {}'.format(o.streamName,o.chainName))

import os
import psutil
process = psutil.Process(os.getpid())
print('\n\n Total memory in bytes:\n\n')
print(process.memory_info().rss)
