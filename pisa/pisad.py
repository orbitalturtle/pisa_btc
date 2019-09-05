import logging
from sys import argv
from getopt import getopt
import pisa.conf as conf
from pisa.api import start_api
from pisa.watcher import Watcher
from pisa.tools import can_connect_to_bitcoind, in_correct_network, get_missed_blocks, find_last_common_block, \
    rewind_responder_state
from pisa.utils.authproxy import AuthServiceProxy
from pisa.db_manager import open_db, load_appointments_db, get_last_known_block


def main():
    debug = False
    opts, _ = getopt(argv[1:], 'd', ['debug'])
    for opt, arg in opts:
        if opt in ['-d', '--debug']:
            debug = True

    # Configure logging
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO, handlers=[
        logging.FileHandler(conf.SERVER_LOG_FILE),
        logging.StreamHandler()
    ])

    bitcoin_cli = AuthServiceProxy("http://%s:%s@%s:%d" % (conf.BTC_RPC_USER, conf.BTC_RPC_PASSWD, conf.BTC_RPC_HOST,
                                                           conf.BTC_RPC_PORT))

    if can_connect_to_bitcoind(bitcoin_cli):
        if in_correct_network(bitcoin_cli, conf.BTC_NETWORK):
            # Get previous appointment information from the db
            appointment_db = open_db(conf.DB_PATH)
            watcher_appointments = load_appointments_db(appointment_db, conf.WATCHER_PREFIX)
            responder_jobs = load_appointments_db(appointment_db, conf.RESPONDER_PREFIX)

            if watcher_appointments or responder_jobs:
                if debug:
                    logging.info("[Pisad] bootstrapping from backed up data")

                # Check what we've missed while offline
                lkb_watcher = get_last_known_block(appointment_db, conf.WATCHER_LAST_BLOCK_KEY)
                lkb_responder = get_last_known_block(appointment_db, conf.RESPONDER_LAST_BLOCK_KEY)

                lcb_watcher, _ = find_last_common_block(bitcoin_cli, lkb_watcher, debug, logging)
                lcb_responder, lcb_height_responder = find_last_common_block(bitcoin_cli, lkb_responder, debug, logging)

                missed_blocks_watcher = get_missed_blocks(bitcoin_cli, lcb_watcher)

                if lcb_watcher == lcb_responder:
                    missed_blocks_responder = missed_blocks_watcher
                else:
                    missed_blocks_responder = get_missed_blocks(bitcoin_cli, lcb_responder)

                if debug:
                    if missed_blocks_watcher:
                        logging.info("[Pisad] Watcher missed {} blocks".format(len(missed_blocks_watcher)))
                        logging.info("[Pisad] {}".format(missed_blocks_watcher))

                    if missed_blocks_responder:
                        logging.info("[Pisad] Responder missed {} blocks".format(len(missed_blocks_responder)))
                        logging.info("[Pisad] {}".format(missed_blocks_responder))

                if lcb_responder != lkb_responder:
                    responder_jobs = rewind_responder_state(bitcoin_cli, watcher_appointments, responder_jobs,
                                                            lcb_height_responder)

                # Only watcher's appointments that has not been triggered should be feed to the watcher
                watcher_appointments = {k: v for k, v in watcher_appointments.items() if
                                        v.get("triggered") is False}

                # Create a watcher and responder from the previous states
                watcher = Watcher.load_prev_state(watcher_appointments, missed_blocks_watcher, responder_jobs,
                                                  missed_blocks_responder, appointment_db)

                # And fire them
                if responder_jobs:
                    watcher.responder.awake_if_asleep(debug, logging)

                if watcher_appointments:
                    watcher.awake_if_asleep(debug, logging)

            else:
                if debug:
                    logging.info("[Pisad] fresh bootstrap")

                watcher = Watcher(appointment_db)

            # Fire the api
            start_api(watcher, debug, logging)

        else:
            logging.error("[Pisad] bitcoind is running on a different network, check conf.py and bitcoin.conf. "
                          "Shutting down")
    else:
        logging.error("[Pisad] can't connect to bitcoind. Shutting down")


if __name__ == '__main__':
    main()
