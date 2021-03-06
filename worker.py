# -*- coding: utf-8 -*-

import array
import asyncio
import functools
import json
import logging
import queue
import socket
import threading
import time

from utils import send_msg, recv_msg

logger = None

q = queue.Queue()


class GameProtocol(asyncio.Protocol):
    def __init__(self, loop):
        self.loop = loop
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        client = self.transport.get_extra_info('peername')
        logger.info('connection to %s closed' % (client,))
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
        self.loop.close()

    def reader(self):
        data, fds = recv_msg(self.server_sock)
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
                    future = self.loop.create_task(coro)
                    future.add_done_callback(functools.partial(self.client_disconnected,
                                                               sock.getpeername()))
            else:
                logger.info('worker %s received: %s' % (self.worker, msg))

    def client_disconnected(self, client, future):
        send_msg(self.server_sock, client)

        tasks = future.all_tasks(self.loop)
        if not tasks or (future in tasks and len(tasks) == 1):
            self.loop.stop()


if __name__ == '__main__':
    w = Worker('game-worker0')
    w.run()
