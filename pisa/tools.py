import re
from pisa.appointment import Appointment
from pisa.utils.authproxy import JSONRPCException
from pisa.rpc_errors import RPC_INVALID_ADDRESS_OR_KEY
from http.client import HTTPException


def check_tx_in_chain(bitcoin_cli, tx_id, debug, logging, parent='', tx_label='transaction'):
    tx_in_chain = False
    confirmations = 0

    try:
        tx_info = bitcoin_cli.getrawtransaction(tx_id, 1)

        if tx_info.get("confirmations"):
            confirmations = int(tx_info.get("confirmations"))
            tx_in_chain = True
            if debug:
                logging.error("[{}] {} found in the blockchain (txid: {}) ".format(parent, tx_label, tx_id))
        elif debug:
            logging.error("[{}] {} found in mempool (txid: {}) ".format(parent, tx_label, tx_id))
    except JSONRPCException as e:
        if e.error.get('code') == RPC_INVALID_ADDRESS_OR_KEY:
            if debug:
                logging.error("[{}] {} not found in mempool nor blockchain (txid: {}) ".format(parent, tx_label, tx_id))
        elif debug:
            # ToDO: Unhandled errors, check this properly
            logging.error("[{}] JSONRPCException. Error code {}".format(parent, e))

    return tx_in_chain, confirmations


def can_connect_to_bitcoind(bitcoin_cli):
    can_connect = True

    try:
        bitcoin_cli.help()
    except (ConnectionRefusedError, JSONRPCException, HTTPException):
        can_connect = False

    return can_connect


def in_correct_network(bitcoin_cli, network):
    mainnet_genesis_block_hash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
    testnet3_genesis_block_hash = "000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943"
    correct_network = False

    genesis_block_hash = bitcoin_cli.getblockhash(0)

    if network == 'mainnet' and genesis_block_hash == mainnet_genesis_block_hash:
        correct_network = True
    elif network == 'testnet' and genesis_block_hash == testnet3_genesis_block_hash:
        correct_network = True
    elif network == 'regtest' and genesis_block_hash not in [mainnet_genesis_block_hash, testnet3_genesis_block_hash]:
        correct_network = True

    return correct_network


def find_last_common_block(bitcoin_cli, last_known_blocks, debug, logging):
    last_common_block = None

    for block_hash in last_known_blocks:
        try:
            bitcoin_cli.getblock(block_hash)
            last_common_block = block_hash

        except JSONRPCException as e:
            if e.error.get('code') == RPC_INVALID_ADDRESS_OR_KEY:
                if debug:
                    if block_hash == last_known_blocks[-1]:
                        logging.info('[Pisad] cannot bootstrap from backed up data, no common block found')

                    else:
                        logging.info('[Pisad] block {} not found. Backtracking...')

    return last_common_block


def rewind_states(bitcoin_cli, watcher_appointments, responder_jobs):
    # There's nothing to be done in terms of updating nor moving appointments from the watcher to the responder.
    # Once the watcher is bootstrapped with the missed blocks and the old state it will be able to handle the changes.
    # However, some jobs may need to go back to the watcher in case of a reorg taking them out of the chain.

    for uuid, job in responder_jobs:
        try:
            tx_info = bitcoin_cli.getrawtransaction(job.justice_txid, 1)
            job.confirmations = int(tx_info.get("confirmations"))

        except JSONRPCException as e:
            if e.error.get('code') == RPC_INVALID_ADDRESS_OR_KEY:
                # FIXME: could this overwrite some watcher's data?
                # FIXME: we are missing most of the data since jobs were not supposed to go back
                watcher_appointments[uuid] = Appointment(job.locator, None, job.appointment_end, None, None, None, None)
                responder_jobs.pop(uuid)

    return watcher_appointments, responder_jobs


def get_missed_blocks(bitcoin_cli, last_common_block):
    current_block_hash = bitcoin_cli.getbestblockhash()

    missed_blocks = []

    while current_block_hash != last_common_block and current_block_hash is not None:
        missed_blocks.append(current_block_hash)

        current_block = bitcoin_cli.getblock(current_block_hash)
        current_block_hash = current_block.get("previousblockhash")

    return missed_blocks[::-1]


def check_txid_format(txid):
    # TODO: #12-check-txid-regexp
    return isinstance(txid, str) and re.search(r'^[0-9A-Fa-f]{64}$', txid) is not None
