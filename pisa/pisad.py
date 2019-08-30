import logging
import plyvel
import json
from sys import argv
from getopt import getopt
from pisa.api import start_api
from pisa.watcher import Watcher
from pisa.tools import can_connect_to_bitcoind, in_correct_network
from pisa.utils.authproxy import AuthServiceProxy
from pisa.conf import BTC_RPC_USER, BTC_RPC_PASSWD, BTC_RPC_HOST, BTC_RPC_PORT, BTC_NETWORK, SERVER_LOG_FILE, DB_PATH


def load_appointments_db(prefix, db_path=DB_PATH):
    appointment_db = plyvel.DB(db_path)
    data = {}

    for k, v in appointment_db.iterator(prefix=prefix):
        # Get uuid and appointment_data from the db
        uuid = k[1:].decode('utf-8')
        data[uuid] = json.loads(v)

    appointment_db.close()

    return data


def get_last_known_block(db_path=DB_PATH):
    appointment_db = plyvel.DB(db_path)
    last_known_block = appointment_db.get(b"last_known_block")

    appointment_db.close()
    return last_known_block


def get_missed_blocks(last_known_block_hash, bitcoin_cli):
    current_block_hash = bitcoin_cli.getbestblockhash()

    missed_blocks = []

    while current_block_hash != last_known_block_hash:
        missed_blocks.append(current_block_hash)

        current_block = bitcoin_cli.getblock(current_block_hash)
        current_block_hash = current_block.get("previousblockhash")

    return missed_blocks[::-1]


def main():
    debug = False
    opts, _ = getopt(argv[1:], 'd', ['debug'])
    for opt, arg in opts:
        if opt in ['-d', '--debug']:
            debug = True

    # Configure logging
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO, handlers=[
        logging.FileHandler(SERVER_LOG_FILE),
        logging.StreamHandler()
    ])

    bitcoin_cli = AuthServiceProxy("http://%s:%s@%s:%d" % (BTC_RPC_USER, BTC_RPC_PASSWD, BTC_RPC_HOST,
                                                           BTC_RPC_PORT))

    if can_connect_to_bitcoind(bitcoin_cli):
        if in_correct_network(bitcoin_cli, BTC_NETWORK):
            # Get previous appointment information from the db
            watcher_appointments = load_appointments_db(b'w')
            responder_jobs = load_appointments_db(b'r')

            if watcher_appointments or responder_jobs:
                if debug:
                    logging.info("[Pisad] bootstrapping from backed up data")

                # Check what we've missed while offline
                last_known_block_hash = get_last_known_block()
                missed_blocks = get_missed_blocks(last_known_block_hash, bitcoin_cli)

                # Create a watcher and responder from the previous states
                watcher = Watcher.load_prev_state(watcher_appointments, responder_jobs, missed_blocks)

                # TODO: Check how treads should be run here
                # And fire them
                watcher.responder.awake_if_asleep()
                watcher.awake_if_asleep()

            else:
                if debug:
                    logging.info("[Pisad] fresh bootstrap")

                watcher = Watcher()

            start_api(watcher, debug, logging)
        else:
            logging.error("[Pisad] bitcoind is running on a different network, check conf.py and bitcoin.conf. "
                          "Shutting down")
    else:
        logging.error("[Pisad] can't connect to bitcoind. Shutting down")


if __name__ == '__main__':
    main()
