import re
from http.client import HTTPException

from pisa.logger import Logger
from pisa.rpc_errors import RPC_INVALID_ADDRESS_OR_KEY
from pisa.utils.auth_proxy import AuthServiceProxy, JSONRPCException


def bitcoin_cli(conf):
    return AuthServiceProxy("http://%s:%s@%s:%d" % (conf.RPC_USER, conf.RPC_PASSWD, conf.RPC_HOST, conf.RPC_PORT))


# TODO: currently only used in the Responder; might move there or in the BlockProcessor
def check_tx_in_chain(conf, tx_id, logger=Logger(), tx_label='Transaction'):
    tx_in_chain = False
    confirmations = 0

    try:
        tx_info = bitcoin_cli(conf).getrawtransaction(tx_id, 1)

        if tx_info.get("confirmations"):
            confirmations = int(tx_info.get("confirmations"))
            tx_in_chain = True
            logger.error("{} found in the blockchain".format(tx_label), txid=tx_id)

        else:
            logger.error("{} found in mempool".format(tx_label), txid=tx_id)

    except JSONRPCException as e:
        if e.error.get('code') == RPC_INVALID_ADDRESS_OR_KEY:
            logger.error("{} not found in mempool nor blockchain".format(tx_label), txid=tx_id)

        else:
            # ToDO: Unhandled errors, check this properly
            logger.error("JSONRPCException.", method='tools.check_tx_in_chain', error=e.error)

    return tx_in_chain, confirmations


def can_connect_to_bitcoind(conf):
    can_connect = True

    try:
        bitcoin_cli(conf).help()
    except (ConnectionRefusedError, JSONRPCException, HTTPException):
        can_connect = False

    return can_connect


def in_correct_network(conf, network):
    mainnet_genesis_block_hash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
    testnet3_genesis_block_hash = "000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943"
    correct_network = False

    genesis_block_hash = self.bitcoin_cli(conf).getblockhash(0)

    if network == 'mainnet' and genesis_block_hash == mainnet_genesis_block_hash:
        correct_network = True
    elif network == 'testnet' and genesis_block_hash == testnet3_genesis_block_hash:
        correct_network = True
    elif network == 'regtest' and genesis_block_hash not in [mainnet_genesis_block_hash, testnet3_genesis_block_hash]:
        correct_network = True

    return correct_network


def check_txid_format(txid):
    # TODO: #12-check-txid-regexp
    return isinstance(txid, str) and re.search(r'^[0-9A-Fa-f]{64}$', txid) is not None
