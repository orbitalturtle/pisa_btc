import json
import plyvel


def open_db(db_path):
    try:
        appointments_db = plyvel.DB(db_path)

    except plyvel.Error as e:
        if 'create_if_missing is false' in str(e):
            print("No db found. Creating a fresh one")
            appointments_db = plyvel.DB(db_path, create_if_missing=True)

    return appointments_db


def load_appointments_db(db, prefix):
    data = {}

    for k, v in db.iterator(prefix=prefix):
        # Get uuid and appointment_data from the db
        uuid = k[1:].decode('utf-8')
        data[uuid] = json.loads(v)

    return data


def get_last_known_block(db, prefix):
    last_block = db.get(prefix)

    if last_block:
        last_block = last_block.decode('utf-8')

    return last_block
