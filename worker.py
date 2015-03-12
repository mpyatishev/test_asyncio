# -*- coding: utf-8 -*-

import array
import asyncio
import json
import logging
import queue
import socket
import threading
import time

from kombu import (
    Connection,
    Exchange,
    Queue,
)
from kombu.mixins import ConsumerMixin

logger = None

exchange = Exchange('commands', type='direct')
connection = Connection('amqp://localhost/')

q = queue.Queue()


class Server(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.thread = threading.Thread(target=self.loop.run_forever)

    def run(self):
        self.thread.start()
        while True:
            try:
                sock = q.get(block=False)
            except queue.Empty:
                pass
            else:
                logger.info(sock)
                self.loop.add_reader(sock, self.reader, sock)
            time.sleep(0.5)
            # logger.info(threading.active_count())

    def reader(self, sock):
        data = sock.recv(5)
        logger.info(data.decode())


class Worker(ConsumerMixin):
    def __init__(self, worker, server_sock):
        self.worker = worker
        self.server_sock = server_sock
        self.socks = []
        self.server = Server()
        self.connection = connection
        self.command_queue = Queue(self.worker, exchange, routing_key=self.worker)
        self.command_queue.maybe_bind(self.connection)

        self.set_logger()

        logger.info('worker %s created' % self.worker)

        self.server.start()
        self.run()

    def set_logger(self):
        global logger

        logger = logging.getLogger(self.worker + __name__)
        ch = logging.StreamHandler()
        logger.addHandler(ch)
        logger.setLevel(logging.INFO)

    def get_consumers(self, consumer, channel):
        return [
            consumer(queues=self.command_queue, callbacks=[self.on_command])
        ]

    def on_command(self, body, message):
        logger.info('worker %s received: %s' % (self.worker, body))

        body = json.loads(body)
        if 'sock' in body:
            fd = body['sock']
            family = body['family']
            type = body['type']
            proto = body['proto']
            sock = socket.fromfd(fd, family, type, proto)
            self.socks.append(sock)
            q.put(sock)

        message.ack()

        msg, fds = self.recv_sock()
        msg = json.loads(msg.decode())
        if 'sock' in msg:
            family = msg['family']
            type = msg['type']
            proto = msg['proto']
            for fd in fds:
                sock = socket.fromfd(fd, family, type, proto)
                self.socks.append(sock)
                q.put(sock)

    def recv_sock(self):
        fds = array.array("i")
        msglen = 90
        maxfds = 5
        msg, ancdata, flags, addr = self.server_sock.recvmsg(
            msglen, socket.CMSG_LEN(maxfds * fds.itemsize))
        for cmsg_level, cmsg_type, cmsg_data in ancdata:
            if (cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS):
                # Append data, ignoring any truncated integers at the end.
                fds.fromstring(cmsg_data[:len(cmsg_data) - (len(cmsg_data) % fds.itemsize)])
        return msg, list(fds)


if __name__ == '__main__':
    w = Worker('game-worker0')
    w.run()
