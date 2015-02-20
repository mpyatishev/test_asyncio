# -*- coding: utf-8 -*-

# import asyncio
import logging

from kombu import (
    Connection,
    Consumer,
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
        self.command_queue = Queue('commands', exchange, routing_key=self.worker)
        self.queues = [
            Consumer(connection, self.command_queue, callbacks=[self.on_command]),
        ]

        self.set_logger()

        logger.info('worker %s' % self.worker)

    def set_logger(self):
        global logger

        logger = logging.getLogger(self.worker + __name__)
        ch = logging.StreamHandler()
        logger.addHandler(ch)
        logger.setLevel(logging.INFO)

    def get_consumers(self, consumer, channel):
        logger.info(self.queues)
        return self.queues

    def on_command(self, body, message):
        logger.info(body)
