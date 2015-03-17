# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import os
import queue
import socket
import threading
import time

from utils import send_msg, recv_msg

logger = None

q = queue.Queue()


class GameProtocol(asyncio.Protocol):
    count = 0

    def __init__(self, loop):
        self.loop = loop
        self.transport = None
        self._connection_lost_callback = None
        self.__class__.count += 1
        self.count = self.__class__.count
        logger.info('gameprotocol%s created' % self.count)

    def connection_made(self, transport):
        self.transport = transport
        logger.info('gameprotocol%s: connection made' % self.count)
        # logger.info(self.transport)

    def connection_lost(self, exc):
        client = self.transport.get_extra_info('peername')
        # logger.info(self.transport)
        logger.info('gameprotocol%s: connection to %s closed' % (self.count, client))
        if exc:
            logger.info(exc)

        if self._connection_lost_callback:
            self._connection_lost_callback(client)

    def data_received(self, data):
        # logger.info(self.transport)
        logger.info('gameprotocol%s: %s' % (self.count, data.decode()))
        if 'help!' in data.decode():
            time.sleep(0.5)
            logger.info('gameprotocol%s: sending "ok"' % (self.count,))
            self.transport.write(json.dumps('ok').encode())

    def eof_received(self):
        # logger.info(self.transport)
        logger.info('gameprotocol%s: eof' % self.count)
        self.transport.close()
        # logger.info('gameprotocol%s: %s' % (self.count, self.transport))

    def set_connection_lost_callback(self, callback):
        self._connection_lost_callback = callback


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
                logger.info(sock)

            time.sleep(0.01)


class Worker:
    def __init__(self, worker, server_sock):
        self.worker = worker
        self.server_sock = server_sock
        self.socks = []
        self.clients = []

        self._init()
        self._set_logger()

        logger.info('worker %s created' % self.worker)

        self.start()

    def _init(self):
        self.loop = asyncio.new_event_loop()
        self.main_task = self.loop.create_task(self._main())
        self.loop.add_reader(self.server_sock, self.reader)

    def _set_logger(self):
        global logger

        logger = logging.getLogger(self.worker + __name__)
        ch = logging.StreamHandler()
        logger.addHandler(ch)
        logger.setLevel(logging.INFO)

    @asyncio.coroutine
    def _main(self):
        while True:
            yield from asyncio.sleep(1, loop=self.loop)
            tasks = [_ for _ in asyncio.Task.all_tasks(loop=self.loop) if not _.done()]
            tasks.remove(self.main_task)
            if len(tasks) == 0:
                logger.info('shutting down')
                break

    def start(self):
        self.loop.run_until_complete(self.main_task)
        self.loop.close()
        send_msg(self.server_sock, {'done': os.getpid()})
        self.server_sock.close()

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
                    self.socks.append(sock)
                    # logger.info(sock)
                    protocol = GameProtocol(self.loop)
                    protocol.set_connection_lost_callback(self.client_disconnected)
                    coro = self.loop.create_connection(lambda: protocol, sock=sock)
                    self.loop.create_task(coro)
                    self.clients.append(sock.getpeername())
            else:
                logger.info('worker %s received: %s' % (self.worker, msg))

    def client_connected(self, client, future):
        transport, protocol = future.result()
        logger.info('%s %s' % (transport, protocol))
        protocol.set_connection_lost_callback(self.client_disconnected)
        self.clients.append(client)

    def client_disconnected(self, client):
        data = {'client': client}
        send_msg(self.server_sock, data)
        self.clients.remove(client)


if __name__ == '__main__':
    w = Worker('game-worker0')
    w.run()
