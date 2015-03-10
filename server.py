#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import hashlib
import heapq
import json
import logging
import random
import time

from concurrent.futures import ProcessPoolExecutor

from kombu import (
    Connection,
    Exchange,
    Producer,
)

from worker import Worker


logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
logger.addHandler(ch)
logger.setLevel(logging.INFO)


class Command(Producer):
    command = None

    def send_command(self, command):
        self.publish(command)


class NewServer:
    max_clients = 1
    command_queues = {}
    workers = []
    clients_to_workers = {}
    connection = Connection('amqp://localhost/')
    exchange = Exchange('commands', type='direct')
    executor = ProcessPoolExecutor()
    loop = None

    def __init__(self, loop):
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

        worker = self.clients_to_workers.pop(client, None)
        if worker is not None:
            for (clients, w) in self.workers:
                if w == worker:
                    self.workers.remove((clients, w))
                    clients -= 1
                    heapq.heappush(self.workers, (clients, w))
                    break

    def data_received(self, data):
        message = json.loads(data.decode())
        logger.info('data: %s' % message)

        token = self.auth(message)
        if token:
            worker = self.get_worker()
            command = {'queue': token}
            self.command_queues[worker].send_command(json.dumps(command))
            resp = {
                'token': token,
            }

            client = self.transport.get_extra_info('peername')
            self.clients_to_workers[client] = worker

            self.transport.write(json.dumps(resp).encode())
        else:
            self.transport.write('No such user\n'.encode())

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
            self.command_queues[worker] = self.create_queue(worker)
            future = self.run_worker(worker)

        heapq.heappush(self.workers, (clients + 1, worker))

        return worker

    def create_worker(self):
        return 'game-worker%s' % len(self.workers)

    def create_queue(self, worker):
        queue = Command(channel=self.connection, exchange=self.exchange,
                        routing_key=worker)
        return queue

    def run_worker(self, worker):
        return self.loop.run_in_executor(self.executor, Worker, worker)


if __name__ == '__main__':
    random.seed()

    loop = asyncio.get_event_loop()
    coro = loop.create_server(lambda: NewServer(loop), port=8888)
    server = loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    loop.close()
