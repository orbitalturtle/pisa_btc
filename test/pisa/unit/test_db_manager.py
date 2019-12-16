import os
import json
import pytest
import shutil
from uuid import uuid4

from pisa import c_logger
from pisa.db_manager import DBManager
from pisa.db_manager import WATCHER_LAST_BLOCK_KEY, RESPONDER_LAST_BLOCK_KEY, LOCATOR_MAP_PREFIX

from common.constants import LOCATOR_LEN_BYTES

from test.pisa.unit.conftest import get_random_value_hex, generate_dummy_appointment

c_logger.disabled = True


@pytest.fixture(scope="module")
def watcher_appointments():
    return {uuid4().hex: generate_dummy_appointment(real_height=False)[0] for _ in range(10)}


@pytest.fixture(scope="module")
def responder_trackers():
    return {get_random_value_hex(16): get_random_value_hex(32) for _ in range(10)}


def open_create_db(db_path):

    try:
        db_manager = DBManager(db_path)

        return db_manager

    except ValueError:
        return False


def test_init():
    db_path = "init_test_db"

    # First we check if the db exists, and if so we delete it
    if os.path.isdir(db_path):
        shutil.rmtree(db_path)

    # Check that the db can be created if it does not exist
    db_manager = open_create_db(db_path)
    assert isinstance(db_manager, DBManager)
    db_manager.db.close()

    # Check that we can open an already create db
    db_manager = open_create_db(db_path)
    assert isinstance(db_manager, DBManager)
    db_manager.db.close()

    # Check we cannot create/open a db with an invalid parameter
    assert open_create_db(0) is False

    # Removing test db
    shutil.rmtree(db_path)


def test_load_appointments_db(db_manager):
    # Let's made up a prefix and try to load data from the database using it
    prefix = "XX"
    db_appointments = db_manager.load_appointments_db(prefix)

    assert len(db_appointments) == 0

    # We can add a bunch of data to the db and try again (data is stored in json by the manager)
    local_appointments = {}
    for _ in range(10):
        key = get_random_value_hex(16)
        value = get_random_value_hex(32)
        local_appointments[key] = value

        db_manager.db.put((prefix + key).encode("utf-8"), json.dumps({"value": value}).encode("utf-8"))

    db_appointments = db_manager.load_appointments_db(prefix)

    # Check that both keys and values are the same
    assert db_appointments.keys() == local_appointments.keys()

    values = [appointment["value"] for appointment in db_appointments.values()]
    assert set(values) == set(local_appointments.values()) and (len(values) == len(local_appointments))


def test_get_last_known_block():
    db_path = "empty_db"

    # First we check if the db exists, and if so we delete it
    if os.path.isdir(db_path):
        shutil.rmtree(db_path)

    # Check that the db can be created if it does not exist
    db_manager = open_create_db(db_path)

    # Trying to get any last block for either the watcher or the responder should return None for an empty db

    for key in [WATCHER_LAST_BLOCK_KEY, RESPONDER_LAST_BLOCK_KEY]:
        assert db_manager.get_last_known_block(key) is None

    # After saving some block in the db we should get that exact value
    for key in [WATCHER_LAST_BLOCK_KEY, RESPONDER_LAST_BLOCK_KEY]:
        block_hash = get_random_value_hex(32)
        db_manager.db.put(key.encode("utf-8"), block_hash.encode("utf-8"))
        assert db_manager.get_last_known_block(key) == block_hash

    # Removing test db
    shutil.rmtree(db_path)


def test_create_entry(db_manager):
    key = get_random_value_hex(16)
    value = get_random_value_hex(32)

    # Adding a value with no prefix (create entry encodes values in utf-8 internally)
    db_manager.create_entry(key, value)

    # We should be able to get it straightaway from the key
    assert db_manager.db.get(key.encode("utf-8")).decode("utf-8") == value

    # If we prefix the key we should be able to get it if we add the prefix, but not otherwise
    key = get_random_value_hex(16)
    prefix = "w"
    db_manager.create_entry(key, value, prefix=prefix)

    assert db_manager.db.get((prefix + key).encode("utf-8")).decode("utf-8") == value
    assert db_manager.db.get(key.encode("utf-8")) is None

    # Same if we try to use any other prefix
    another_prefix = "r"
    assert db_manager.db.get((another_prefix + key).encode("utf-8")) is None


def test_delete_entry(db_manager):
    # Let's first get the key all the things we've wrote so far in the db
    data = [k.decode("utf-8") for k, v in db_manager.db.iterator()]

    # Let's empty the db now
    for key in data:
        db_manager.delete_entry(key)

    assert len([k for k, v in db_manager.db.iterator()]) == 0

    # Let's check that the same works if a prefix is provided.
    prefix = "r"
    key = get_random_value_hex(16)
    value = get_random_value_hex(32)
    db_manager.create_entry(key, value, prefix)

    # Checks it's there
    assert db_manager.db.get((prefix + key).encode("utf-8")).decode("utf-8") == value

    # And now it's gone
    db_manager.delete_entry(key, prefix)
    assert db_manager.db.get((prefix + key).encode("utf-8")) is None


def test_load_watcher_appointments_empty(db_manager):
    assert len(db_manager.load_watcher_appointments()) == 0


def test_load_responder_trackers_empty(db_manager):
    assert len(db_manager.load_responder_trackers()) == 0


def test_load_locator_map_empty(db_manager):
    assert db_manager.load_locator_map(get_random_value_hex(LOCATOR_LEN_BYTES)) is None


def test_store_update_locator_map_empty(db_manager):
    uuid = uuid4().hex
    locator = get_random_value_hex(LOCATOR_LEN_BYTES)
    db_manager.store_update_locator_map(locator, uuid)

    # Check that the locator map has been properly stored
    assert db_manager.load_locator_map(locator) == [uuid]

    # If we try to add the same uuid again the list shouldn't change
    db_manager.store_update_locator_map(locator, uuid)
    assert db_manager.load_locator_map(locator) == [uuid]

    # Add another uuid to the same locator and check that it also works
    uuid2 = uuid4().hex
    db_manager.store_update_locator_map(locator, uuid2)

    assert set(db_manager.load_locator_map(locator)) == set([uuid, uuid2])


def test_delete_locator_map(db_manager):
    locator_maps = db_manager.load_appointments_db(prefix=LOCATOR_MAP_PREFIX)
    assert len(locator_maps) != 0

    for locator, uuids in locator_maps.items():
        db_manager.delete_locator_map(locator)

    locator_maps = db_manager.load_appointments_db(prefix=LOCATOR_MAP_PREFIX)
    assert len(locator_maps) == 0


def test_store_load_watcher_appointment(db_manager, watcher_appointments):
    for uuid, appointment in watcher_appointments.items():
        db_manager.store_watcher_appointment(uuid, appointment.to_json())

    db_watcher_appointments = db_manager.load_watcher_appointments()

    # Check that the two appointment collections are equal by checking:
    # - Their size is equal
    # - Each element in one collection exists in the other

    assert watcher_appointments.keys() == db_watcher_appointments.keys()

    for uuid, appointment in watcher_appointments.items():
        assert json.dumps(db_watcher_appointments[uuid], sort_keys=True, separators=(",", ":")) == appointment.to_json()


def test_store_load_triggered_appointment(db_manager):
    db_watcher_appointments = db_manager.load_watcher_appointments()
    db_watcher_appointments_with_triggered = db_manager.load_watcher_appointments(include_triggered=True)

    assert db_watcher_appointments == db_watcher_appointments_with_triggered

    # Create an appointment flagged as triggered
    triggered_appointment, _ = generate_dummy_appointment(real_height=False)
    uuid = uuid4().hex
    db_manager.store_watcher_appointment(uuid, triggered_appointment.to_json(triggered=True))

    # The new appointment is grabbed only if we set include_triggered
    assert db_watcher_appointments == db_manager.load_watcher_appointments()
    assert uuid in db_manager.load_watcher_appointments(include_triggered=True)


def test_store_load_responder_trackers(db_manager, responder_trackers):
    for key, value in responder_trackers.items():
        db_manager.store_responder_tracker(key, json.dumps({"value": value}))

    db_responder_trackers = db_manager.load_responder_trackers()

    values = [tracker["value"] for tracker in db_responder_trackers.values()]

    assert responder_trackers.keys() == db_responder_trackers.keys()
    assert set(responder_trackers.values()) == set(values) and len(responder_trackers) == len(values)


def test_delete_watcher_appointment(db_manager, watcher_appointments):
    # Let's delete all we added
    db_watcher_appointments = db_manager.load_watcher_appointments(include_triggered=True)
    assert len(db_watcher_appointments) != 0

    for key in watcher_appointments.keys():
        db_manager.delete_watcher_appointment(key)

    db_watcher_appointments = db_manager.load_watcher_appointments()
    assert len(db_watcher_appointments) == 0


def test_delete_responder_tracker(db_manager, responder_trackers):
    # Same for the responder
    db_responder_trackers = db_manager.load_responder_trackers()
    assert len(db_responder_trackers) != 0

    for key in responder_trackers.keys():
        db_manager.delete_responder_tracker(key)

    db_responder_trackers = db_manager.load_responder_trackers()
    assert len(db_responder_trackers) == 0


def test_store_load_last_block_hash_watcher(db_manager):
    # Let's first create a made up block hash
    local_last_block_hash = get_random_value_hex(32)
    db_manager.store_last_block_hash_watcher(local_last_block_hash)

    db_last_block_hash = db_manager.load_last_block_hash_watcher()

    assert local_last_block_hash == db_last_block_hash


def test_store_load_last_block_hash_responder(db_manager):
    # Same for the responder
    local_last_block_hash = get_random_value_hex(32)
    db_manager.store_last_block_hash_responder(local_last_block_hash)

    db_last_block_hash = db_manager.load_last_block_hash_responder()

    assert local_last_block_hash == db_last_block_hash
