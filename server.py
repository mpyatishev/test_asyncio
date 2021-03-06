#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import array
import asyncio
import functools
import hashlib
import heapq
import json
import logging
import random
import socket
import time

from concurrent.futures import ProcessPoolExecutor

from utils import send_msg, recv_msg
from worker import Worker


logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
logger.addHandler(ch)
logger.setLevel(logging.INFO)


class NewServer:
    max_clients = 5
    workers = []
    clients_to_workers = {}
    socks_to_workers = {}
    executor = ProcessPoolExecutor()
    loop = None

    def __init__(self, loop, *args, **kwargs):
        self.loop = loop

    def connection_made(self, transport):
        info = transport.get_extra_info('peername')
        logger.info('client from %s connected' % (info,))
        self.transport = transport

    def connection_lost(self, exc):
        client = self.transport.get_extra_info('peername')
        logger.info('connection to %s closed' % (client,))
        if exc:
            logger.info(exc)

    def data_received(self, data):
        message = json.loads(data.decode())
        logger.info('data: %s' % message)

        token = self.auth(message)
        if token:
            worker = self.get_worker()
            command = {'queue': token}
            self.send_msg(worker, command)
            self.send_sock(worker)
            resp = {
                'token': token,
            }

            client = self.transport.get_extra_info('peername')
            self.clients_to_workers[client] = worker

            self.transport.write(json.dumps(resp).encode())
        else:
            self.transport.write('No such user\n'.encode())

        self.transport.close()

    def eof_received(self):
        self.transport.close()
        logger.info('eof received')

    def auth(self, message):
        if 'user' in message and 'passwd' in message\
                and message['user'] == 'max' and message['passwd'] == 'secret':
            md5 = hashlib.md5()
            md5.update(message['user'].encode())
            md5.update(str(time.time()).encode())
            md5.update(str(random.random()).encode())
            return md5.hexdigest()

    def get_worker(self):
        try:
            clients, worker = heapq.heappop(self.workers)
        except IndexError:
            clients = None

        if clients is None or clients >= self.max_clients:
            if clients is not None:
                heapq.heappush(self.workers, (clients, worker))
            clients = 0
            worker = self.create_worker()
            # future = self.run_worker(worker)
            self.run_worker(worker)

        heapq.heappush(self.workers, (clients + 1, worker))

        return worker

    def create_worker(self):
        return 'game-worker%s' % len(self.workers)

    def run_worker(self, worker):
        socks = socket.socketpair()
        self.socks_to_workers[worker] = socks[0]
        future = self.loop.run_in_executor(self.executor, Worker, worker, socks[1])
        self.loop.add_reader(socks[0], self.reader, socks[0])
        future.add_done_callback(functools.partial(self.worker_done, worker))
        return future

    def send_msg(self, worker, msg, *args):
        worker_sock = self.socks_to_workers[worker]
        send_msg(worker_sock, msg)

    def send_sock(self, worker):
        sock = self.transport.get_extra_info('socket')
        # logger.info(sock)
        msg = {
            'sock': True,
            'family': sock.family,
            'type': sock.type,
            'proto': sock.proto,
        }
        fds = [sock.fileno()]
        worker_sock = self.socks_to_workers[worker]
        send_msg(worker_sock, msg, [(socket.SOL_SOCKET,
                                     socket.SCM_RIGHTS, array.array("i", fds))])

    def reader(self, sock):
        msg, fds = recv_msg(sock)
        # logger.info('dispatcher: %s' % msg.decode())
        for data in msg.decode().split('\r\n'):
            if not data:
                continue
            client = json.loads(data)
            self.client_disconnected(client)

    def client_disconnected(self, client):
        if not isinstance(client, tuple):
            client = tuple(client)

        worker = self.clients_to_workers.pop(client, None)
        if worker is not None:
            for (clients, w) in self.workers:
                if w == worker:
                    self.workers.remove((clients, w))
                    clients -= 1
                    heapq.heappush(self.workers, (clients, w))
                    break

    def worker_done(self, worker, future):
        logger.info('%s done' % worker)
        self.socks_to_workers[worker].close()
        del self.socks_to_workers[worker]
        for (clients, w) in self.workers:
            if w == worker:
                self.workers.remove((clients, w))
                break


if __name__ == '__main__':
    random.seed()

    loop = asyncio.get_event_loop()
    coro = loop.create_server(lambda: NewServer(loop), port=8888)
    loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    loop.close()
