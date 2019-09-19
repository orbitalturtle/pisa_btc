from pisa.conf import FEED_PROTOCOL, FEED_ADDR, FEED_PORT
from flask import Flask, request, Response, abort
from tests.simulator.zmq_publisher import ZMQPublisher
from threading import Thread
from pisa.rpc_errors import *
from pisa.tools import check_txid_format
import logging
import binascii
import json
import os
import time

app = Flask(__name__)
HOST = 'localhost'
PORT = '18443'
BLOCK_TIME = 10
GENESIS_PARENT = '0000000000000000000000000000000000000000000000000000000000000000'


@app.route('/fork', methods=['POST'])
def create_fork():
    """
    create_fork processes chain fork requests. It will create a fork with the following parameters:

    parent: the block hash from where the chain will be forked
    length: the length of the fork to be created (number of blocks to be mined on top of parent)
    stay: whether to stay in the forked chain after length blocks has been mined or to come back to the previous chain.
          Stay is optional and will default to False.
    """

    global prev_block_hash

    request_data = request.get_json()
    response = {"result": 0, "error": None}

    parent = request_data.get("parent")

    # FIXME: We only accept forks one by one for now

    if parent not in blocks:
        response["error"] = {"code": -1, "message": "Wrong parent block to fork from"}

    else:
        prev_block_hash = parent
        print("Forking chain from {}".format(parent))

        # FIXME: the blockchain is defined as a list (since forks in the sim where not possible til recently). Therefore
        #        block heights and blockchain length is currently incorrect. It does the trick to test forks, but should
        #        be fixed for better testing.

    return Response(json.dumps(response), status=200, mimetype='application/json')


@app.route('/', methods=['POST'])
def process_request():
    """
    process_requests simulates the bitcoin-rpc server run by bitcoind. The available commands are limited to the ones
    we'll need to use in pisa. The model we will be using is pretty simplified to reduce the complexity of simulating
    bitcoind:

    Raw transactions:       raw transactions will actually be transaction ids (txids). Pisa will, therefore, receive
                            encrypted blobs that encrypt ids instead of real transactions.

    decoderawtransaction:   querying for the decoding of a raw transaction will return a dictionary with a single
                            field: "txid", which will match with the txid provided in the request

    sendrawtransaction:     sending a rawtransaction will notify our mining simulator to include such transaction in a
                            subsequent block.

    getrawtransaction:      requesting a rawtransaction from a txid will return a dictionary containing a single field:
                            "confirmations", since rawtransactions are only queried to check whether a transaction has
                            made it to a block or not.

    getblockcount:          the block count will be get from the mining simulator by querying how many blocks have been
                            emited so far.

    getblock:               querying for a block will return a dictionary with a three fields: "tx" representing a list
                            of transactions, "height" representing the block height and "hash" representing the block
                            hash. Both will be got from the mining simulator.

    getblockhash:           a block hash is only queried by pisad on bootstrapping to check the network bitcoind is
                            running on.

    getbestblockhash:       returns the hash of the block in the tip of the chain

    help:                   help is only used as a sample command to test if bitcoind is running when bootstrapping
                            pisad. It will return a 200/OK with no data.
    """

    global mempool
    request_data = request.get_json()
    method = request_data.get('method')

    response = {"id": 0, "result": 0, "error": None}
    no_param_err = {"code": RPC_MISC_ERROR, "message": "JSON value is not a {} as expected"}

    if method == "decoderawtransaction":
        txid = get_param(request_data)

        if isinstance(txid, str):
            if check_txid_format(txid):
                response["result"] = {"txid": txid}

            else:
                response["error"] = {"code": RPC_DESERIALIZATION_ERROR, "message": "TX decode failed"}

        else:
            response["error"] = no_param_err
            response["error"]["message"] = response["error"]["message"].format("string")

    elif method == "sendrawtransaction":
        # TODO: A way of rejecting transactions should be added to test edge cases.
        txid = get_param(request_data)

        if isinstance(txid, str):
            if check_txid_format(txid):
                if txid not in list(mined_transactions.keys()):
                    mempool.append(txid)

                else:
                    response["error"] = {"code": RPC_VERIFY_ALREADY_IN_CHAIN,
                                         "message": "Transaction already in block chain"}

            else:
                response["error"] = {"code": RPC_DESERIALIZATION_ERROR, "message": "TX decode failed"}

        else:
            response["error"] = no_param_err
            response["error"]["message"] = response["error"]["message"].format("string")

    elif method == "getrawtransaction":
        txid = get_param(request_data)

        if isinstance(txid, str):
            block = blocks.get(mined_transactions.get(txid))

            if block:
                response["result"] = {"confirmations": len(blockchain) - block.get('height')}

            elif txid in mempool:
                response["result"] = {"confirmations": 0}

            else:
                response["error"] = {'code': RPC_INVALID_ADDRESS_OR_KEY,
                                     'message': 'No such mempool or blockchain transaction. Use gettransaction for '
                                                'wallet transactions.'}
        else:
            response["error"] = no_param_err
            response["error"]["message"] = response["error"]["message"].format("string")

    elif method == "getblockcount":
        response["result"] = len(blockchain)

    elif method == "getblock":
        blockid = get_param(request_data)

        if isinstance(blockid, str):
            block = blocks.get(blockid)

            if block:
                block["hash"] = blockid

                # FIXME: the confirmation counter depends on the chain the transaction is in (in case of forks). For
                #        now there will be only one, but multiple forks would come up handy to test edge cases
                block["confirmations"] = len(blockchain) - block["height"] + 1

                response["result"] = block

            else:
                response["error"] = {"code": RPC_INVALID_ADDRESS_OR_KEY, "message": "Block not found"}

        else:
            response["error"] = no_param_err
            response["error"]["message"] = response["error"]["message"].format("string")

    elif method == "getblockhash":
        height = get_param(request_data)

        if isinstance(height, int):
            if 0 <= height <= len(blockchain):
                response["result"] = blockchain[height]

            else:
                response["error"] = {"code": RPC_INVALID_PARAMETER, "message": "Block height out of range"}
        else:
            response["error"] = no_param_err
            response["error"]["message"] = response["error"]["message"].format("integer")

    elif method == "getbestblockhash":
        response["result"] = blockchain[-1]

    elif method == "help":
        pass

    else:
        return abort(404, "Method not found")

    return Response(json.dumps(response), status=200, mimetype='application/json')


def get_param(request_data):
    param = None

    params = request_data.get("params")
    if isinstance(params, list) and len(params) > 0:
        param = params[0]

    return param


def load_data():
    pass


def simulate_mining():
    global mempool, mined_transactions, blocks, blockchain, prev_block_hash

    while True:
        block_hash = binascii.hexlify(os.urandom(32)).decode('utf-8')
        coinbase_tx_hash = binascii.hexlify(os.urandom(32)).decode('utf-8')
        txs_to_mine = [coinbase_tx_hash]

        if len(mempool) != 0:
            # We'll mine up to 100 txs per block
            txs_to_mine += mempool[:99]
            mempool = mempool[99:]

        # Keep track of the mined transaction (to respond to getrawtransaction)
        for tx in txs_to_mine:
            mined_transactions[tx] = block_hash

        # FIXME: chain_work is being defined as a incremental counter for now. Multiple chains should be possible.
        blocks[block_hash] = {"tx": txs_to_mine, "height": len(blockchain), "previousblockhash": prev_block_hash,
                              "chainwork": len(blockchain)}
        mining_simulator.publish_data(binascii.unhexlify(block_hash))
        blockchain.append(block_hash)
        prev_block_hash = block_hash

        print("New block mined: {}".format(block_hash))
        print("Transactions: {}".format(txs_to_mine))

        time.sleep(BLOCK_TIME)


if __name__ == '__main__':
    mining_simulator = ZMQPublisher(topic=b'hashblock', feed_protocol=FEED_PROTOCOL, feed_addr=FEED_ADDR,
                                    feed_port=FEED_PORT)

    mempool = []
    blockchain = []
    mined_transactions = {}
    blocks = {}
    prev_block_hash = GENESIS_PARENT

    mining_thread = Thread(target=simulate_mining)
    mining_thread.start()

    # Setting Flask log to ERROR only so it does not mess with out logging
    logging.getLogger('werkzeug').setLevel(logging.ERROR)

    app.run(host=HOST, port=PORT)
