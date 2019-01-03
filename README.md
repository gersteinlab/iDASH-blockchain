# iDASH-blockchain
After installing Multichain1.0.4 from https://www.multichain.com

(1) To create a chain use:

python CreateChain.py -cn <chain name> -sn <stream name>

(2) To insert logs into the chain and stream

python Insert.py <data> <chain name> <stream name>

(3) To query from the chain

python testing100Queries.py <chain name> <stream name> <query file>

3 uses the Query.py

to stop a chain

(4) multichain-cli <chain name> stop

For more multichain options see https://www.multichain.com
