from os import urandom

from pisa import logging
from pisa.errors import *
from pisa.inspector import Inspector
from pisa.appointment import Appointment
from pisa.tools import get_bitcoin_cli
from pisa.block_processor import BlockProcessor
from pisa.conf import BTC_RPC_USER, BTC_RPC_PASSWD, BTC_RPC_HOST, BTC_RPC_PORT
from pisa.conf import MIN_DISPUTE_DELTA, SUPPORTED_CIPHERS, SUPPORTED_HASH_FUNCTIONS

MIN_DISPUTE_DELTA = 20
SUPPORTED_CIPHERS = ["AES-GCM-128"]
SUPPORTED_HASH_FUNCTIONS = ["SHA256"]

bitcoin_cli = get_bitcoin_cli(BTC_RPC_USER, BTC_RPC_PASSWD, BTC_RPC_HOST, BTC_RPC_PORT)
block_processor = BlockProcessor(bitcoin_cli)
inspector = Inspector(
    min_dispute_delta=MIN_DISPUTE_DELTA,
    supported_ciphers=SUPPORTED_CIPHERS,
    supported_hash_functions=SUPPORTED_HASH_FUNCTIONS
)

APPOINTMENT_OK = (0, None)

NO_HEX_STINGS = ["R" * 64, urandom(31).hex() + "PP", "$"*64, " "*64]
WRONG_TYPES = [[], '', urandom(32).hex(), 3.2, 2.0, (), object, {}, " "*32, object()]
WRONG_TYPES_NO_STR = [[], urandom(32), 3.2, 2.0, (), object, {}, object()]

logging.getLogger().disabled = True


def test_check_locator():
    # Right appointment type, size and format
    locator = urandom(32).hex()
    assert(inspector.check_locator(locator) == APPOINTMENT_OK)

    # Wrong size (too big)
    locator = urandom(33).hex()
    assert(inspector.check_locator(locator)[0] == APPOINTMENT_WRONG_FIELD_SIZE)

    # Wrong size (too small)
    locator = urandom(31).hex()
    assert(inspector.check_locator(locator)[0] == APPOINTMENT_WRONG_FIELD_SIZE)

    # Empty
    locator = None
    assert (inspector.check_locator(locator)[0] == APPOINTMENT_EMPTY_FIELD)

    # Wrong type (several types tested, it should do for anything that is not a string)
    locators = [[], -1, 3.2, 0, 4, (), object, {}, object()]

    for locator in locators:
        assert (inspector.check_locator(locator)[0] == APPOINTMENT_WRONG_FIELD_TYPE)

    # Wrong format (no hex)
    locators = NO_HEX_STINGS
    for locator in locators:
        assert (inspector.check_locator(locator)[0] == APPOINTMENT_WRONG_FIELD_FORMAT)


def test_check_start_time():
    # Time is defined in block height
    current_time = 100

    # Right format and right value (start time in the future)
    start_time = 101
    assert (inspector.check_start_time(start_time, current_time) == APPOINTMENT_OK)

    # Start time too small (either same block or block in the past)
    start_times = [100, 99, 98, -1]
    for start_time in start_times:
        assert (inspector.check_start_time(start_time, current_time)[0] == APPOINTMENT_FIELD_TOO_SMALL)

    # Empty field
    start_time = None
    assert (inspector.check_start_time(start_time, current_time)[0] == APPOINTMENT_EMPTY_FIELD)

    # Wrong data type
    start_times = WRONG_TYPES
    for start_time in start_times:
        assert (inspector.check_start_time(start_time, current_time)[0] == APPOINTMENT_WRONG_FIELD_TYPE)


def test_check_end_time():
    # Time is defined in block height
    current_time = 100
    start_time = 120

    # Right format and right value (start time before end and end in the future)
    end_time = 121
    assert (inspector.check_end_time(end_time, start_time, current_time) == APPOINTMENT_OK)

    # End time too small (start time after end time)
    end_times = [120, 119, 118, -1]
    for end_time in end_times:
        assert (inspector.check_end_time(end_time, start_time, current_time)[0] == APPOINTMENT_FIELD_TOO_SMALL)

    # End time too small (either same height as current block or in the past)
    current_time = 130
    end_times = [130, 129, 128, -1]
    for end_time in end_times:
        assert (inspector.check_end_time(end_time, start_time, current_time)[0] == APPOINTMENT_FIELD_TOO_SMALL)

    # Empty field
    end_time = None
    assert (inspector.check_end_time(end_time, start_time, current_time)[0] == APPOINTMENT_EMPTY_FIELD)

    # Wrong data type
    end_times = WRONG_TYPES
    for end_time in end_times:
        assert (inspector.check_end_time(end_time, start_time, current_time)[0] == APPOINTMENT_WRONG_FIELD_TYPE)


def test_check_delta():
    # Right value, right format
    deltas = [MIN_DISPUTE_DELTA, MIN_DISPUTE_DELTA+1, MIN_DISPUTE_DELTA+1000]
    for delta in deltas:
        assert (inspector.check_delta(delta) == APPOINTMENT_OK)

    # Delta too small
    deltas = [MIN_DISPUTE_DELTA-1, MIN_DISPUTE_DELTA-2, 0, -1, -1000]
    for delta in deltas:
        assert (inspector.check_delta(delta)[0] == APPOINTMENT_FIELD_TOO_SMALL)

    # Empty field
    delta = None
    assert(inspector.check_delta(delta)[0] == APPOINTMENT_EMPTY_FIELD)

    # Wrong data type
    deltas = WRONG_TYPES
    for delta in deltas:
        assert (inspector.check_delta(delta)[0] == APPOINTMENT_WRONG_FIELD_TYPE)


def test_check_blob():
    # Right format and length
    encrypted_blob = urandom(120).hex()
    assert(inspector.check_blob(encrypted_blob) == APPOINTMENT_OK)

    # # Wrong content
    # # FIXME: There is not proper defined format for this yet. It should be restricted by size at least, and check it
    # #        is multiple of the block size defined by the encryption function.

    # Wrong type
    encrypted_blobs = WRONG_TYPES_NO_STR
    for encrypted_blob in encrypted_blobs:
        assert (inspector.check_blob(encrypted_blob)[0] == APPOINTMENT_WRONG_FIELD_TYPE)

    # Empty field
    encrypted_blob = None
    assert (inspector.check_blob(encrypted_blob)[0] == APPOINTMENT_EMPTY_FIELD)

    # Wrong format (no hex)
    encrypted_blobs = NO_HEX_STINGS
    for encrypted_blob in encrypted_blobs:
        assert (inspector.check_blob(encrypted_blob)[0] == APPOINTMENT_WRONG_FIELD_FORMAT)


def test_check_cipher():
    # Right format and content (any case combination should be accepted)
    for cipher in SUPPORTED_CIPHERS:
        cipher_cases = [cipher, cipher.lower(), cipher.capitalize()]
        for case in cipher_cases:
            assert(inspector.check_cipher(case) == APPOINTMENT_OK)

    # Wrong type
    ciphers = WRONG_TYPES_NO_STR
    for cipher in ciphers:
        assert(inspector.check_cipher(cipher)[0] == APPOINTMENT_WRONG_FIELD_TYPE)

    # Wrong value
    ciphers = NO_HEX_STINGS
    for cipher in ciphers:
        assert(inspector.check_cipher(cipher)[0] == APPOINTMENT_CIPHER_NOT_SUPPORTED)

    # Empty field
    cipher = None
    assert (inspector.check_cipher(cipher)[0] == APPOINTMENT_EMPTY_FIELD)


def test_check_hash_function():
    # Right format and content (any case combination should be accepted)
    for hash_function in SUPPORTED_HASH_FUNCTIONS:
        hash_function_cases = [hash_function, hash_function.lower(), hash_function.capitalize()]
        for case in hash_function_cases:
            assert (inspector.check_hash_function(case) == APPOINTMENT_OK)

    # Wrong type
    hash_functions = WRONG_TYPES_NO_STR
    for hash_function in hash_functions:
        assert (inspector.check_hash_function(hash_function)[0] == APPOINTMENT_WRONG_FIELD_TYPE)

    # Wrong value
    hash_functions = NO_HEX_STINGS
    for hash_function in hash_functions:
        assert (inspector.check_hash_function(hash_function)[0] == APPOINTMENT_HASH_FUNCTION_NOT_SUPPORTED)

    # Empty field
    hash_function = None
    assert (inspector.check_hash_function(hash_function)[0] == APPOINTMENT_EMPTY_FIELD)


def test_inspect(run_bitcoind):
    # At this point every single check function has been already tested, let's test inspect with an invalid and a valid
    # appointments.

    # Invalid appointment, every field is empty
    appointment_data = dict()
    appointment = inspector.inspect(appointment_data)
    assert (type(appointment) == tuple and appointment[0] != 0)

    # Valid appointment
    locator = urandom(32).hex()
    start_time = BlockProcessor.get_block_count() + 5
    end_time = start_time + 20
    dispute_delta = MIN_DISPUTE_DELTA
    encrypted_blob = urandom(64).hex()
    cipher = SUPPORTED_CIPHERS[0]
    hash_function = SUPPORTED_HASH_FUNCTIONS[0]

    appointment_data = {"locator": locator, "start_time": start_time, "end_time": end_time,
                        "dispute_delta": dispute_delta, "encrypted_blob": encrypted_blob, "cipher": cipher,
                        "hash_function": hash_function}

    appointment = inspector.inspect(appointment_data)

    assert(type(appointment) == Appointment and appointment.locator == locator and appointment.start_time == start_time
           and appointment.end_time == end_time and appointment.dispute_delta == dispute_delta and
           appointment.encrypted_blob.data == encrypted_blob and appointment.cipher == cipher and
           appointment.hash_function == hash_function)
