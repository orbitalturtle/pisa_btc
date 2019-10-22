from getopt import getopt
from sys import argv, exit
from signal import signal, SIGINT, SIGQUIT, SIGTERM

from pisa.logger import Logger
from pisa.api import start_api
from pisa.conf import BTC_NETWORK, BTC_RPC_USER, BTC_RPC_PASSWD, BTC_RPC_HOST, BTC_RPC_PORT
from pisa.tools import can_connect_to_bitcoind, in_correct_network

logger = Logger("Daemon")


def handle_signals(signal_received, frame):
    logger.info("Shutting down PISA")
    # TODO: #11-add-graceful-shutdown: add code to close the db, free any resources, etc.

    exit(0)


if __name__ == '__main__':
    logger.info("Starting PISA")

    signal(SIGINT, handle_signals)
    signal(SIGTERM, handle_signals)
    signal(SIGQUIT, handle_signals)

    opts, _ = getopt(argv[1:], '', [''])
    for opt, arg in opts:
        # FIXME: Leaving this here for future option/arguments
        pass

    try:
        if can_connect_to_bitcoind(BTC_RPC_USER, BTC_RPC_PASSWD, BTC_RPC_HOST, BTC_RPC_PORT):
            if in_correct_network(BTC_NETWORK):
                # Fire the api
                start_api()

            else:
                logger.error("bitcoind is running on a different network, check conf.py and bitcoin.conf."
                             "Shutting down")

        else:
            logger.error("Can't connect to bitcoind. Shutting down")
    except Exception as e:
        logger.error("There was an error while starting the daemon. Shutting down", error_args=e.args)
        exit(1)
