#!/usr/bin/env python3

# -*- coding: utf-8 -*-


import asyncio
import json
import logging


logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
logger.addHandler(ch)
logger.setLevel(logging.INFO)


class NewClient:
    def __init__(self, loop):
        self.loop = loop
        self.user = 'max'
        self.passwd = 'secret'

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
        self.loop.stop()

    def data_received(self, data):
        message = json.loads(data.decode())
        logger.info('data: %s' % message)

    def eof_received(self):
        self.transport.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    coro = loop.create_connection(lambda: NewClient(loop), '127.0.0.1', port=8888)
    loop.run_until_complete(coro)
    loop.run_forever()
    loop.close()
