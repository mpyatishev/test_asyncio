#!/usr/bin/env python3

# -*- coding: utf-8 -*-


import asyncio
import json
import logging
import time


logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
logger.addHandler(ch)
logger.setLevel(logging.INFO)
max_clients = 2


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
        logger.info('connection to %s closed' % (info,))
        if exc:
            logger.info(exc)

        self._disconnected = True

    def data_received(self, data):
        message = json.loads(data.decode())
        logger.info('%s: data: %s' % (self.name, message))

        logger.info('%s: sending name' % self.name)
        self.transport.write(self.name.encode())
        logger.info('%s: sending message' % self.name)
        self.transport.write(str(self.name + ': help!').encode())

        self.transport.close()

    def eof_received(self):
        self.transport.close()

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


if __name__ == '__main__':
    clients = []
    loop = asyncio.get_event_loop()
    for i in range(max_clients):
        coro = loop.create_connection(lambda i=i: NewClient(loop, i),
                                      '127.0.0.1', port=8888)
        clients.append(loop.create_task(coro))
    time.sleep(3)
    loop.run_until_complete(asyncio.wait(clients))
    # loop.run_until_complete(wait_clients(loop, clients))
    # loop.run_forever()
    loop.close()
