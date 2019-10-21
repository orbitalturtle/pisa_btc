from pisa import logging
from pisa.tools import bitcoin_cli, check_txid_format
from pisa.tools import can_connect_to_bitcoind, in_correct_network

logging.getLogger().disabled = True

# mock configuration settings
BTC_RPC_USER = "user"
BTC_RPC_PASSWD = "passwd"
BTC_RPC_HOST = "localhost"
BTC_RPC_PORT = 18443


def test_in_correct_network(run_bitcoind):
    # The simulator runs as if it was regtest, so every other network should fail
    assert in_correct_network('mainnet') is False
    assert in_correct_network('testnet') is False
    assert in_correct_network('regtest') is True


def test_can_connect_to_bitcoind():
    assert can_connect_to_bitcoind(BTC_RPC_USER, BTC_RPC_PASSWD, BTC_RPC_HOST, BTC_RPC_PORT) is True


# def test_can_connect_to_bitcoind_bitcoin_not_running():
#     # Kill the simulator thread and test the check fails
#     bitcoind_process.kill()
#     assert can_connect_to_bitcoind() is False


def test_check_txid_format():
    assert(check_txid_format(None) is False)
    assert(check_txid_format("") is False)
    assert(check_txid_format(0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef) is False)  # wrong type
    assert(check_txid_format("abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd") is True)  # lowercase
    assert(check_txid_format("ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD") is True)  # uppercase
    assert(check_txid_format("0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789ABCDEF") is True)  # mixed case
    assert(check_txid_format("0123456789012345678901234567890123456789012345678901234567890123") is True)  # only nums
    assert(check_txid_format("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdf") is False)  # too short
    assert(check_txid_format("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0") is False)  # too long
    assert(check_txid_format("g123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef") is False)  # non-hex
