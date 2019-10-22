import zmq
import binascii
from pisa.logger import Logger


# ToDo: #7-add-async-back-to-zmq
class ZMQHandler:
    """ Adapted from https://github.com/bitcoin/bitcoin/blob/master/contrib/zmq/zmq_sub.py"""
    def __init__(self, parent, feed_protocol, feed_addr, feed_port):
        self.zmqContext = zmq.Context()
        self.zmqSubSocket = self.zmqContext.socket(zmq.SUB)
        self.zmqSubSocket.setsockopt(zmq.RCVHWM, 0)
        self.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "hashblock")
        self.zmqSubSocket.connect("%s://%s:%s" % (feed_protocol, feed_addr, feed_port))
        self.logger = Logger("ZMQHandler-{}".format(parent))

        self.terminate = False

    def handle(self, block_queue):
        while not self.terminate:
            msg = self.zmqSubSocket.recv_multipart()

            # Terminate could have been set wile the thread was blocked in recv
            if not self.terminate:
                topic = msg[0]
                body = msg[1]

                if topic == b"hashblock":
                    block_hash = binascii.hexlify(body).decode('UTF-8')
                    block_queue.put(block_hash)

                    self.logger.info("New block received via ZMQ", block_hash=block_hash)
