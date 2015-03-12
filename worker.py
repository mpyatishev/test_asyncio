# -*- coding: utf-8 -*-

import array
import asyncio
import json
import logging
import queue
import socket
import threading
import time

logger = None

q = queue.Queue()


class GameProtocol(asyncio.Protocol):
    def __init__(self, loop):
        self.loop = loop
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        info = self.transport.get_extra_info('peername')
        logger.info('connection to %s closed' % (info,))
        if exc:
            logger.info(exc)

    def data_received(self, data):
        logger.info(data.decode())

    def eof_received(self):
        self.transport.close()


class Game(threading.Thread):
    def __init__(self, loop, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self):
        while True:
            try:
                sock = q.get(block=False)
            except queue.Empty:
                pass
            else:
                logger.info(threading.active_count())

            time.sleep(0.01)


class Worker:
    def __init__(self, worker, server_sock):
        self.worker = worker
        self.server_sock = server_sock
        self.socks = []
        self.loop = asyncio.new_event_loop()
        self.loop.add_reader(self.server_sock, self.reader)

        self.set_logger()

        logger.info('worker %s created' % self.worker)

        self.start()

    def set_logger(self):
        global logger

        logger = logging.getLogger(self.worker + __name__)
        ch = logging.StreamHandler()
        logger.addHandler(ch)
        logger.setLevel(logging.INFO)

    def start(self):
        self.loop.run_forever()

    def on_command(self, body, message):
        logger.info('worker %s received: %s' % (self.worker, body))
        body = json.dumps(body)
        message.ack()

        if 'queue' not in body:
            return

    def reader(self):
        data, fds = self.recv_sock()
        for msg in data.decode().split("\r\n"):
            if not msg:
                continue
            try:
                msg = json.loads(msg)
            except ValueError as e:
                logger.info(e)
                continue
            if 'sock' in msg:
                family = msg['family']
                type = msg['type']
                proto = msg['proto']
                for fd in fds:
                    sock = socket.fromfd(fd, family, type, proto)
                    sock.setblocking(False)
                    self.socks.append(sock)
                    coro = self.loop.create_connection(lambda: GameProtocol(self.loop),
                                                    sock=sock)
                    self.loop.create_task(coro)
            else:
                logger.info('worker %s received: %s' % (self.worker, msg))

    def recv_sock(self):
        fds = array.array("i")
        msglen = 4096
        maxfds = 5
        msg, ancdata, flags, addr = self.server_sock.recvmsg(
            msglen, socket.CMSG_LEN(maxfds * fds.itemsize))
        for cmsg_level, cmsg_type, cmsg_data in ancdata:
            if (cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS):
                # Append data, ignoring any truncated integers at the end.
                fds.fromstring(
                    cmsg_data[:len(cmsg_data) - (len(cmsg_data) % fds.itemsize)])
        return msg, list(fds)


if __name__ == '__main__':
    w = Worker('game-worker0')
    w.run()
