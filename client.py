#!/usr/bin/env python3

# -*- coding: utf-8 -*-


import asyncio
import json
import logging
import random
import time


logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
logger.addHandler(ch)
logger.setLevel(logging.INFO)
max_clients = 1000


class NewClient:
    def __init__(self, loop, client_no):
        self.loop = loop
        self.user = 'max'
        self.passwd = 'secret'
        self.client_no = client_no
        self.name = self.get_name()
        self._disconnected = False

    def connection_made(self, transport):
        self.transport = transport
        data = {
            'user': self.user,
            'passwd': self.passwd,
        }
        self.transport.write(json.dumps(data).encode())

    def connection_lost(self, exc):
        info = self.transport.get_extra_info('peername')
        logger.info('%s: connection to %s closed' % (info, self.name))
        if exc:
            logger.info(exc)

        self._disconnected = True

    def data_received(self, data):
        try:
            message = json.loads(data.decode())
        except ValueError as e:
            logger.info(e)
            logger.info('%s: data: %s' % (self.name, data.decode()))
            return
        logger.info('%s: data: %s' % (self.name, message))

        if message == 'ok':
            self.loop.create_task(self.some_work())
        else:
            logger.info('%s: sending name' % self.name)
            self.transport.write(self.name.encode())
            logger.info('%s: sending message' % self.name)
            self.transport.write(str(self.name + ': help!').encode())

    @asyncio.coroutine
    def some_work(self):
        wait_secs = random.random() * 5
        logger.info('%s: sleep for %s seconds' % (self.name, wait_secs))
        yield from asyncio.sleep(wait_secs)
        logger.info('%s: closing connection' % self.name)
        self.transport.close()

    def eof_received(self):
        pass

    def get_name(self):
        return 'client%s' % self.client_no

    def disconnected(self):
        return self._disconnected


@asyncio.coroutine
def wait_clients(loop, clients):
    protocols = []

    for client in clients:
        transport, protocol = client.result()
        protocols.append(protocol)

    while True:
        for protocol in protocols:
            if protocol.disconnected():
                protocols.remove(protocol)
        if not protocols:
            break
        yield


if __name__ == '__main__':
    clients = []
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    for i in range(max_clients):
        coro = loop.create_connection(lambda i=i: NewClient(loop, i),
                                      '127.0.0.1', port=8888)
        clients.append(loop.create_task(coro))
    time.sleep(3)
    loop.run_until_complete(asyncio.wait(clients))
    loop.run_until_complete(wait_clients(loop, clients))
    # loop.run_forever()
    loop.close()
