# -*- coding: utf-8 -*-

# import asyncio
import logging

from kombu import (
    Connection,
    Exchange,
    Queue,
)
from kombu.mixins import ConsumerMixin

logger = None

exchange = Exchange('commands', type='direct')
connection = Connection('amqp://localhost/')


class Worker(ConsumerMixin):
    def __init__(self, worker):
        self.worker = worker
        self.connection = connection
        self.command_queue = Queue(self.worker, exchange, routing_key=self.worker)
        self.command_queue.maybe_bind(self.connection)

        self.set_logger()

        logger.info('worker %s created' % self.worker)

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
        message.ack()


if __name__ == '__main__':
    w = Worker('game-worker0')
    w.run()
