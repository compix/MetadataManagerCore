from typing import Callable
import pika
from concurrent.futures import ThreadPoolExecutor
import threading
from MetadataManagerCore import config
from pika.adapters.blocking_connection import BlockingChannel
import logging

logger = logging.getLogger(__name__)

logging.getLogger("pika").setLevel(logging.CRITICAL)

class Messenger(object):
    def __init__(self, host: str, username: str, password: str) -> None:
        super().__init__()

        self.connection = None
        self.channel = None
        self.host = host
        self.username = username
        self.password = password

        self.connect()
        self.running = True

    def connect(self):
        if not self.connection or self.connection.is_closed:
            credentials = pika.PlainCredentials(self.username, self.password)
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host, credentials=credentials))
            self.channel = self.connection.channel()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if not self.connection.is_closed:
                self.connection.close()
        except Exception as e:
            logger.warning(str(e))

    def consume(self, exchange: str, callback):
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.channel.exchange_declare(exchange=exchange, exchange_type='fanout')
        self.channel.queue_bind(exchange=exchange, queue=result.method.queue)

        try:
            for msg in self.channel.consume(result.method.queue, auto_ack=True, inactivity_timeout=1):
                if not self.running:
                    break

                method, properties, body = msg
                if method == None and properties == None and body == None:
                    continue

                callback(body)
        except Exception as e:
            logger.error(f'Message consumption stopped unexpectedly for exchange {exchange}. Exception: {str(e)}')

        try:
            if not self.channel.is_closed:
                self.channel.cancel()
        except Exception as e:
            logger.warning(str(e))

    def stop(self):
        self.running = False

    @staticmethod
    def configured():
        return Messenger(config.RABBIT_MQ_HOST, config.RABBIT_MQ_USERNAME, config.RABBIT_MQ_PASSWORD)

class MessengerConsumerThread(object):
    def __init__(self, exchange: str, callback: Callable[[str], int]):
        self.messenger = None

        self.thread = threading.Thread(target=lambda: self._startConsumer(exchange, callback))
        self.thread.start()

    def stop(self):
        if self.messenger:
            self.messenger.stop()

    def _startConsumer(self, exchange: str, callback):
        try:
            with Messenger(config.RABBIT_MQ_HOST, config.RABBIT_MQ_USERNAME, config.RABBIT_MQ_PASSWORD) as self.messenger:
                self.messenger.consume(exchange, callback=callback)
        except Exception as e:
            logger.warning(f'Failed to start consumer: {str(e)}')

class FanoutPublisher(Messenger):
    def __init__(self, exchange: str) -> None:
        super().__init__(config.RABBIT_MQ_HOST, config.RABBIT_MQ_USERNAME, config.RABBIT_MQ_PASSWORD)

        self.exchange = exchange
        if self.channel.is_open:
            self.channel.exchange_declare(exchange=exchange, exchange_type='fanout')

    def publish(self, body: str):
        try:
            self.channel.basic_publish(exchange=self.exchange, routing_key='', body=body)
        except (pika.exceptions.ConnectionClosed, pika.exceptions.ChannelClosed, pika.exceptions.ChannelWrongStateError):
            self.connect()
            self.channel.exchange_declare(exchange=self.exchange, exchange_type='fanout')
            self.channel.basic_publish(exchange=self.exchange, routing_key='', body=body)