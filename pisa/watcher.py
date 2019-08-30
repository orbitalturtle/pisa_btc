from binascii import hexlify, unhexlify
from queue import Queue
from threading import Thread
import pisa.conf as conf
from pisa.responder import Responder
from pisa.zmq_subscriber import ZMQHandler
from pisa.appointment import Appointment
from pisa.utils.authproxy import AuthServiceProxy, JSONRPCException
from hashlib import sha256
from uuid import uuid4
import json


class Watcher:
    def __init__(self, appointment_db, max_appointments=conf.MAX_APPOINTMENTS):
        self.appointments = dict()
        self.locator_uuid_map = dict()
        self.block_queue = None
        self.asleep = True
        self.max_appointments = max_appointments
        self.zmq_subscriber = None
        self.responder = Responder(appointment_db)
        self.appointment_db = appointment_db

    @classmethod
    def load_prev_state(cls, appointments, missed_blocks_watcher, jobs, missed_blocks_responder, appointment_db):
        # Create a new watcher
        watcher = cls(appointment_db)

        if jobs:
            # Bootstrap the responder from a previous state
            watcher.responder = Responder.load_prev_state(jobs, missed_blocks_responder, appointment_db)

        if appointments:
            # Restore the appointments dictionary and locator:uuid map
            for uuid, json_appointment in appointments.items():
                appointment = Appointment.from_json(json_appointment)
                watcher.appointments[uuid] = appointment

                if appointment.locator in watcher.locator_uuid_map:
                    watcher.locator_uuid_map[appointment.locator].append(uuid)

                else:
                    watcher.locator_uuid_map[appointment.locator] = [uuid]

            # Fetch all the missed blocks to the block queue
            watcher.block_queue = Queue()

            for block in missed_blocks_watcher:
                watcher.block_queue.put(block)

        return watcher

    def awake_if_asleep(self, debug, logging):
        if self.asleep:
            # Define a new queue if there's none defined (queue is predefined if bootstrapping from and old state)
            if not self.block_queue:
                self.block_queue = Queue()

            self.asleep = False
            zmq_thread = Thread(target=self.do_subscribe, args=[self.block_queue, debug, logging])
            watcher = Thread(target=self.do_watch, args=[debug, logging])
            zmq_thread.start()
            watcher.start()

            if debug:
                logging.info("[Watcher] waking up!")

    def add_appointment(self, appointment, debug, logging):
        # Rationale:
        # The Watcher will analyze every received block looking for appointment matches. If there is no work
        # to do the watcher can go sleep (if appointments = {} then asleep = True) otherwise for every received block
        # the watcher will get the list of transactions and compare it with the list of appointments.
        # If the watcher is awake, every new appointment will just be added to the appointment list until
        # max_appointments is reached.

        if len(self.appointments) < self.max_appointments:
            # Appointments are identified by the locator: the sha256 of commitment txid (H(tx_id)).
            # Two different nodes may ask for appointments using the same commitment txid, what will result in a
            # collision in our appointments structure (and may be an attack surface). In order to avoid such collisions
            # we will identify every appointment with a uuid

            uuid = uuid4().hex
            self.appointments[uuid] = appointment

            if appointment.locator in self.locator_uuid_map:
                self.locator_uuid_map[appointment.locator].append(uuid)

            else:
                self.locator_uuid_map[appointment.locator] = [uuid]

            self.awake_if_asleep(debug, logging)

            self.appointment_db.put(conf.WATCHER_PREFIX+uuid.encode('utf-8'),
                                    json.dumps(appointment.to_json()).encode('utf-8'))

            if debug:
                logging.info('[Watcher] new appointment accepted (locator = {})'.format(appointment.locator))
                logging.info('[Watcher] new appointment recorded in the db {}'.format(uuid))

            appointment_added = True

        else:
            appointment_added = False

            if debug:
                logging.info('[Watcher] maximum appointments reached, appointment rejected (locator = {})'
                             .format(appointment.locator))

        return appointment_added

    def do_subscribe(self, block_queue, debug, logging):
        self.zmq_subscriber = ZMQHandler(parent='Watcher')
        self.zmq_subscriber.handle(block_queue, debug, logging)

    def do_watch(self, debug, logging):
        bitcoin_cli = AuthServiceProxy("http://%s:%s@%s:%d" % (conf.BTC_RPC_USER, conf.BTC_RPC_PASSWD,
                                                               conf.BTC_RPC_HOST, conf.BTC_RPC_PORT))

        while len(self.appointments) > 0:
            block_hash = self.block_queue.get()

            try:
                block = bitcoin_cli.getblock(block_hash)
                txids = block.get('tx')

                if debug:
                    logging.info("[Watcher] new block received {}".format(block_hash))
                    logging.info("[Watcher] list of transactions: {}".format(txids))

                self.delete_expired_appointment(block, debug, logging)

                potential_locators = {sha256(unhexlify(txid)).hexdigest(): txid for txid in txids}

                # Check is any of the tx_ids in the received block is an actual match
                # Get the locators that are both in the map and in the potential locators dict.
                intersection = set(self.locator_uuid_map.keys()).intersection(potential_locators.keys())
                potential_matches = {locator: potential_locators[locator] for locator in intersection}

                if debug:
                    if len(potential_matches) > 0:
                        logging.info("[Watcher] list of potential matches: {}".format(potential_matches))
                    else:
                        logging.info("[Watcher] no potential matches found")

                matches = self.check_potential_matches(potential_matches, bitcoin_cli, debug, logging)

                for locator, uuid, dispute_txid, justice_txid, justice_rawtx in matches:
                    if debug:
                        logging.info("[Watcher] notifying responder about {} and deleting appointment {} (uuid: {})"
                                     .format(justice_txid, locator, uuid))

                    self.responder.add_response(uuid, dispute_txid, justice_txid, justice_rawtx,
                                                self.appointments[uuid].end_time, debug, logging)

                    # Delete the appointment
                    self.appointments.pop(uuid)

                    # If there was only one appointment that matches the locator we can delete the whole list
                    if len(self.locator_uuid_map[locator]) == 1:
                        self.locator_uuid_map.pop(locator)
                    else:
                        # Otherwise we just delete the appointment that matches locator:appointment_pos
                        self.locator_uuid_map[locator].remove(uuid)

                    # Delete appointment from the db
                    self.appointment_db.delete(conf.WATCHER_PREFIX + uuid.encode('utf-8'))

                    if debug:
                        logging.info("[Watcher] deleting {} from db".format(uuid))

                # Register the last processed block for the watcher
                self.appointment_db.put(conf.WATCHER_LAST_BLOCK_KEY, block_hash.encode('utf-8'))

            except JSONRPCException as e:
                if debug:
                    logging.error("[Watcher] couldn't get block from bitcoind. Error code {}".format(e))

        # Go back to sleep if there are no more appointments
        self.asleep = True
        self.zmq_subscriber.terminate = True

        if debug:
            logging.error("[Watcher] no more pending appointments, going back to sleep")

    def delete_expired_appointment(self, block, debug, logging):
        to_delete = [uuid for uuid, appointment in self.appointments.items() if block["height"] > appointment.end_time
                     + conf.EXPIRY_DELTA]

        for uuid in to_delete:
            locator = self.appointments[uuid].locator

            self.appointments.pop(uuid)

            if len(self.locator_uuid_map[locator]) == 1:
                self.locator_uuid_map.pop(locator)

            else:
                self.locator_uuid_map[locator].remove(uuid)

            if debug:
                logging.info("[Watcher] end time reached with no match! Deleting appointment {} (uuid: {})"
                             .format(locator, uuid))

            # Delete appointment from the db
            self.appointment_db.delete(conf.WATCHER_PREFIX + uuid)

            if debug:
                logging.info("[Watcher] deleting {} from db".format(uuid))

    def check_potential_matches(self, potential_matches, bitcoin_cli, debug, logging):
        matches = []

        for locator, dispute_txid in potential_matches.items():
            for uuid in self.locator_uuid_map[locator]:
                try:
                    # ToDo: #20-test-tx-decrypting-edge-cases
                    justice_rawtx = self.appointments[uuid].encrypted_blob.decrypt(unhexlify(dispute_txid), debug,
                                                                                   logging)
                    justice_rawtx = hexlify(justice_rawtx).decode()
                    justice_txid = bitcoin_cli.decoderawtransaction(justice_rawtx).get('txid')
                    matches.append((locator, uuid, dispute_txid, justice_txid, justice_rawtx))

                    if debug:
                        logging.info("[Watcher] match found for locator {} (uuid: {}): {}".format(locator, uuid,
                                                                                                  justice_txid))
                except JSONRPCException as e:
                    # Tx decode failed returns error code -22, maybe we should be more strict here. Leaving it simple
                    # for the POC
                    if debug:
                        logging.error("[Watcher] can't build transaction from decoded data. Error code {}".format(e))

        return matches
