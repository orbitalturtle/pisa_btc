import os
import sys
import time
import json
import requests
import binascii
from sys import argv
from uuid import uuid4
from coincurve import PublicKey
from getopt import getopt, GetoptError
from requests import ConnectTimeout, ConnectionError
from requests.exceptions import MissingSchema, InvalidSchema, InvalidURL

from cli.help import show_usage, help_add_appointment, help_get_appointment, help_get_all_appointments
from cli import DEFAULT_CONF, DATA_DIR, CONF_FILE_NAME, LOG_PREFIX

import common.cryptographer
from common.blob import Blob
from common import constants
from common.logger import Logger
from common.appointment import Appointment
from common.config_loader import ConfigLoader
from common.cryptographer import Cryptographer
from common.tools import setup_logging, setup_data_folder
from common.tools import check_sha256_hex_format, check_locator_format, compute_locator

logger = Logger(actor="Client", log_name_prefix=LOG_PREFIX)
common.cryptographer.logger = Logger(actor="Cryptographer", log_name_prefix=LOG_PREFIX)


def load_keys(teos_pk_path, cli_sk_path, cli_pk_path):
    """
    Loads all the keys required so sign, send, and verify the appointment.

    Args:
        teos_pk_path (:obj:`str`): path to the TEOS public key file.
        cli_sk_path (:obj:`str`): path to the client private key file.
        cli_pk_path (:obj:`str`): path to the client public key file.

    Returns:
        :obj:`tuple` or ``None``: a three item tuple containing a teos_pk object, cli_sk object and the cli_sk_der
        encoded key if all keys can be loaded. ``None`` otherwise.
    """

    if teos_pk_path is None:
        logger.error("TEOS's public key file not found. Please check your settings")
        return None

    if cli_sk_path is None:
        logger.error("Client's private key file not found. Please check your settings")
        return None

    if cli_pk_path is None:
        logger.error("Client's public key file not found. Please check your settings")
        return None

    try:
        teos_pk_der = Cryptographer.load_key_file(teos_pk_path)
        teos_pk = PublicKey(teos_pk_der)

    except ValueError:
        logger.error("TEOS public key is invalid or cannot be parsed")
        return None

    cli_sk_der = Cryptographer.load_key_file(cli_sk_path)
    cli_sk = Cryptographer.load_private_key_der(cli_sk_der)

    if cli_sk is None:
        logger.error("Client private key is invalid or cannot be parsed")
        return None

    try:
        cli_pk_der = Cryptographer.load_key_file(cli_pk_path)
        PublicKey(cli_pk_der)

    except ValueError:
        logger.error("Client public key is invalid or cannot be parsed")
        return None

    return teos_pk, cli_sk, cli_pk_der


def add_appointment(args, teos_url, config):
    """
    Manages the add_appointment command, from argument parsing, trough sending the appointment to the tower, until
    saving the appointment receipt.

    The life cycle of the function is as follows:
        - Load the add_appointment arguments
        - Check that the given commitment_txid is correct (proper format and not missing)
        - Check that the transaction is correct (not missing)
        - Create the appointment locator and encrypted blob from the commitment_txid and the penalty_tx
        - Load the client private key and sign the appointment
        - Send the appointment to the tower
        - Wait for the response
        - Check the tower's response and signature
        - Store the receipt (appointment + signature) on disk

    If any of the above-mentioned steps fails, the method returns false, otherwise it returns true.

    Args:
        args (:obj:`list`): a list of arguments to pass to ``parse_add_appointment_args``. Must contain a json encoded
            appointment, or the file option and the path to a file containing a json encoded appointment.
        teos_url (:obj:`str`): the teos base url.
        config (:obj:`dict`): a config dictionary following the format of :func:`create_config_dict <common.config_loader.ConfigLoader.create_config_dict>`.

    Returns:
        :obj:`bool`: True if the appointment is accepted by the tower and the receipt is properly stored, false if any
        error occurs during the process.
    """

    # Currently the base_url is the same as the add_appointment_endpoint
    add_appointment_endpoint = teos_url

    teos_pk, cli_sk, cli_pk_der = load_keys(
        config.get("TEOS_PUBLIC_KEY"), config.get("CLI_PRIVATE_KEY"), config.get("CLI_PUBLIC_KEY")
    )

    try:
        hex_pk_der = binascii.hexlify(cli_pk_der)

    except binascii.Error as e:
        logger.error("Could not successfully encode public key as hex", error=str(e))
        return False

    if teos_pk is None:
        return False

    # Get appointment data from user.
    appointment_data = parse_add_appointment_args(args)

    if appointment_data is None:
        logger.error("The provided appointment JSON is empty")
        return False

    valid_txid = check_sha256_hex_format(appointment_data.get("tx_id"))

    if not valid_txid:
        logger.error("The provided txid is not valid")
        return False

    tx_id = appointment_data.get("tx_id")
    tx = appointment_data.get("tx")

    if None not in [tx_id, tx]:
        appointment_data["locator"] = compute_locator(tx_id)
        appointment_data["encrypted_blob"] = Cryptographer.encrypt(Blob(tx), tx_id)

    else:
        logger.error("Appointment data is missing some fields")
        return False

    appointment = Appointment.from_dict(appointment_data)
    signature = Cryptographer.sign(appointment.serialize(), cli_sk)

    if not (appointment and signature):
        return False

    data = {"appointment": appointment.to_dict(), "signature": signature, "public_key": hex_pk_der.decode("utf-8")}

    # Send appointment to the server.
    server_response = post_appointment(data, add_appointment_endpoint)
    if server_response is None:
        return False

    response_json = process_post_appointment_response(server_response)

    if response_json is None:
        return False

    signature = response_json.get("signature")
    # Check that the server signed the appointment as it should.
    if signature is None:
        logger.error("The response does not contain the signature of the appointment")
        return False

    rpk = Cryptographer.recover_pk(appointment.serialize(), signature)
    if not Cryptographer.verify_rpk(teos_pk, rpk):
        logger.error("The returned appointment's signature is invalid")
        return False

    logger.info("Appointment accepted and signed by the Eye of Satoshi")

    # All good, store appointment and signature
    return save_appointment_receipt(appointment.to_dict(), signature, config)


def parse_add_appointment_args(args):
    """
    Parses the arguments of the add_appointment command.

    Args:
        args (:obj:`list`): a list of arguments to pass to ``parse_add_appointment_args``. Must contain a json encoded
            appointment, or the file option and the path to a file containing a json encoded appointment.

    Returns:
        :obj:`dict` or :obj:`None`: A dictionary containing the appointment data if it can be loaded. ``None``
        otherwise.
    """

    use_help = "Use 'help add_appointment' for help of how to use the command"

    if not args:
        logger.error("No appointment data provided. " + use_help)
        return None

    arg_opt = args.pop(0)

    try:
        if arg_opt in ["-h", "--help"]:
            sys.exit(help_add_appointment())

        if arg_opt in ["-f", "--file"]:
            fin = args.pop(0)
            if not os.path.isfile(fin):
                logger.error("Can't find file", filename=fin)
                return None

            try:
                with open(fin) as f:
                    appointment_data = json.load(f)

            except IOError as e:
                logger.error("I/O error", errno=e.errno, error=e.strerror)
                return None
        else:
            appointment_data = json.loads(arg_opt)

    except json.JSONDecodeError:
        logger.error("Non-JSON encoded data provided as appointment. " + use_help)
        return None

    return appointment_data


def post_appointment(data, add_appointment_endpoint):
    """
    Sends appointment data to add_appointment endpoint to be processed by the tower.

    Args:
        data (:obj:`dict`): a dictionary containing three fields: an appointment, the client-side signature, and the
            der-encoded client public key.
        add_appointment_endpoint (:obj:`str`): the teos endpoint where to send appointments to.

    Returns:
        :obj:`dict` or ``None``: a json-encoded dictionary with the server response if the data can be posted.
        None otherwise.
    """

    logger.info("Sending appointment to the Eye of Satoshi")

    try:
        return requests.post(url=add_appointment_endpoint, json=json.dumps(data), timeout=5)

    except ConnectTimeout:
        logger.error("Can't connect to the Eye of Satoshi's API. Connection timeout")
        return None

    except ConnectionError:
        logger.error("Can't connect to the Eye of Satoshi's API. Server cannot be reached")
        return None

    except (InvalidSchema, MissingSchema, InvalidURL):
        logger.error("Invalid URL. No schema, or invalid schema, found ({})".format(add_appointment_endpoint))

    except requests.exceptions.Timeout:
        logger.error("The request timed out")


def process_post_appointment_response(response):
    """
    Processes the server response to an add_appointment request.

    Args:
        response (:obj:`requests.models.Response`): a ``Response`` object obtained from the sent request.

    Returns:
        :obj:`dict` or :obj:`None`: a dictionary containing the tower's response data if it can be properly parsed and
        the response type is ``HTTP_OK``. ``None`` otherwise.
    """

    try:
        response_json = response.json()

    except json.JSONDecodeError:
        logger.error(
            "The server returned a non-JSON response", status_code=response.status_code, reason=response.reason
        )
        return None

    if response.status_code != constants.HTTP_OK:
        if "error" not in response_json:
            logger.error(
                "The server returned an error status code but no error description", status_code=response.status_code
            )
        else:
            error = response_json["error"]
            logger.error(
                "The server returned an error status code with an error description",
                status_code=response.status_code,
                description=error,
            )
        return None

    return response_json


def save_appointment_receipt(appointment, signature, config):
    """
    Saves an appointment receipt to disk. A receipt consists in an appointment and a signature from the tower.

    Args:
        appointment (:obj:`Appointment <common.appointment.Appointment>`): the appointment to be saved on disk.
        signature (:obj:`str`): the signature of the appointment performed by the tower.
        config (:obj:`dict`): a config dictionary following the format of :func:`create_config_dict <common.config_loader.ConfigLoader.create_config_dict>`.

    Returns:
        :obj:`bool`: True if the appointment if properly saved, false otherwise.

    Raises:
        IOError: if an error occurs whilst writing the file on disk.
    """

    # Create the appointments directory if it doesn't already exist
    os.makedirs(config.get("APPOINTMENTS_FOLDER_NAME"), exist_ok=True)

    timestamp = int(time.time())
    locator = appointment["locator"]
    uuid = uuid4().hex  # prevent filename collisions

    filename = "{}/appointment-{}-{}-{}.json".format(config.get("APPOINTMENTS_FOLDER_NAME"), timestamp, locator, uuid)
    data = {"appointment": appointment, "signature": signature}

    try:
        with open(filename, "w") as f:
            json.dump(data, f)
            logger.info("Appointment saved at {}".format(filename))
            return True

    except IOError as e:
        logger.error("There was an error while saving the appointment", error=e)
        return False


def get_appointment(locator, get_appointment_endpoint):
    """
    Gets information about an appointment from the tower.

    Args:
        locator (:obj:`str`): the appointment locator used to identify it.
        get_appointment_endpoint (:obj:`str`): the teos endpoint where to get appointments from.

    Returns:
        :obj:`dict` or :obj:`None`: a dictionary containing the appointment data if the locator is valid and the tower
        responds. ``None`` otherwise.
    """

    valid_locator = check_locator_format(locator)

    if not valid_locator:
        logger.error("The provided locator is not valid", locator=locator)
        return None

    parameters = "?locator={}".format(locator)

    try:
        r = requests.get(url=get_appointment_endpoint + parameters, timeout=5)
        return r.json()

    except ConnectTimeout:
        logger.error("Can't connect to the Eye of Satoshi's API. Connection timeout")
        return None

    except ConnectionError:
        logger.error("Can't connect to the Eye of Satoshi's API. Server cannot be reached")
        return None

    except requests.exceptions.InvalidSchema:
        logger.error("No transport protocol found. Have you missed http(s):// in the server url?")

    except requests.exceptions.Timeout:
        logger.error("The request timed out")


def get_all_appointments(get_all_appointments_endpoint):
    """ 
    Gets information about all appointments stored in the tower, if the user requesting the data is an administrator.

    Args:
        get_all_appointments_endpoint (:obj:`str`): the teos endpoint from which all appointments can be retrieved.

    Returns:
        :obj:`dict` a dictionary containing the all the appointments stored by the responder and watcher if the tower
        responds. 
    """

    try:
        r = requests.get(url=get_all_appointments_endpoint, timeout=5)
        return r.json()

    except ConnectTimeout:
        logger.error("Can't connect to the Eye of Satoshi's API. Connection timeout")
        return None

    except ConnectionError:
        logger.error("Can't connect to the Eye of Satoshi's API. Server cannot be reached")
        return None

    except requests.exceptions.Timeout:
        logger.error("The request timed out")


def main(args, command_line_conf):
    # Loads config and sets up the data folder and log file
    config_loader = ConfigLoader(DATA_DIR, CONF_FILE_NAME, DEFAULT_CONF, command_line_conf)
    config = config_loader.build_config()

    setup_data_folder(DATA_DIR)
    setup_logging(config.get("LOG_FILE"), LOG_PREFIX)

    # Set the teos url
    teos_url = "{}:{}".format(config.get("TEOS_SERVER"), config.get("TEOS_PORT"))
    # If an http or https prefix if found, leaves the server as is. Otherwise defaults to http.
    if not teos_url.startswith("http"):
        teos_url = "http://" + teos_url

    try:
        if args:
            command = args.pop(0)

            if command in commands:
                if command == "add_appointment":
                    add_appointment(args, teos_url, config)

                elif command == "get_appointment":
                    if not args:
                        logger.error("No arguments were given")

                    else:
                        arg_opt = args.pop(0)

                        if arg_opt in ["-h", "--help"]:
                            sys.exit(help_get_appointment())

                        get_appointment_endpoint = "{}/get_appointment".format(teos_url)
                        appointment_data = get_appointment(arg_opt, get_appointment_endpoint)
                        if appointment_data:
                            print(appointment_data)

                elif command == "get_all_appointments":
                    get_all_appointments_endpoint = "{}/get_all_appointments".format(teos_url)
                    appointment_data = get_all_appointments(get_all_appointments_endpoint)
                    if appointment_data:
                        print(appointment_data)

                elif command == "help":
                    if args:
                        command = args.pop(0)

                        if command == "add_appointment":
                            sys.exit(help_add_appointment())

                        elif command == "get_appointment":
                            sys.exit(help_get_appointment())

                        elif command == "get_all_appointments":
                            sys.exit(help_get_all_appointments())

                        else:
                            logger.error("Unknown command. Use help to check the list of available commands")

                    else:
                        sys.exit(show_usage())

            else:
                logger.error("Unknown command. Use help to check the list of available commands")

        else:
            logger.error("No command provided. Use help to check the list of available commands")

    except json.JSONDecodeError:
        logger.error("Non-JSON encoded appointment passed as parameter")


if __name__ == "__main__":
    command_line_conf = {}
    commands = ["add_appointment", "get_appointment", "get_all_appointments", "help"]

    try:
        opts, args = getopt(argv[1:], "s:p:h", ["server", "port", "help"])

        for opt, arg in opts:
            if opt in ["-s", "--server"]:
                if arg:
                    command_line_conf["TEOS_SERVER"] = arg

            if opt in ["-p", "--port"]:
                if arg:
                    try:
                        command_line_conf["TEOS_PORT"] = int(arg)
                    except ValueError:
                        sys.exit("port must be an integer")

            if opt in ["-h", "--help"]:
                sys.exit(show_usage())

        main(args, command_line_conf)

    except GetoptError as e:
        logger.error("{}".format(e))
