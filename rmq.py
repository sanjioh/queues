# coding: utf-8
import logging
import time

import pika
from pika.exceptions import AMQPConnectionError


def _setup_logging():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())
    # logger.addHandler(logging.NullHandler)
    return logger


logger = _setup_logging()


class QueueError(Exception):
    pass


class QueueConnectionError(QueueError, ConnectionError):
    pass


class QueueOperationError(QueueError):
    pass


class AbstractQueue:
    def connect(self):
        raise NotImplemented

    def disconnect(self):
        raise NotImplemented

    def push(self, message, options=None):
        raise NotImplemented

    def pop(self, options=None):
        raise NotImplemented

    def ack(self):
        raise NotImplemented

    def nack(self):
        raise NotImplemented


class RabbitMQQueue(AbstractQueue):
    def __init__(self, queue, *, host='localhost', port=5672, vhost='/',
                 username='guest', password='guest',
                 exchange='', routing_key=None,
                 conn_class=None, **kwargs):
        self._queue = queue
        self._conn_params = pika.ConnectionParameters(
            host=host, port=port, virtual_host=vhost,
            credentials=pika.PlainCredentials(username, password),
            **kwargs,
        )
        self._exchange = exchange
        self._routing_key = routing_key or queue
        self._conn_class = conn_class or pika.BlockingConnection
        self._reset()

    def _reset(self):
        self._conn = None
        self._channel = None

    def connect(self, *, retry=True):
        while True:
            logger.info('Connecting to queue...')
            try:
                self._conn = self._conn_class(self._conn_params)
                self._channel = self._conn.channel()
                logger.info('Connected')
                break
            except AMQPConnectionError as e:
                logger.error(f'Failed to connect to queue: {e!a}')
                if retry:
                    time.sleep(1)
                else:
                    raise QueueConnectionError from e

    def disconnect(self):
        logger.info('Disconnecting from queue...')
        try:
            self._channel.cancel()
        except Exception as e:
            logger.error(f'Failed to cancel channel: {e!a}')

        for obj in (self._channel, self._conn):
            try:
                obj.close()
            except Exception as e:
                logger.error(f'Failed to close channel or connection: {e!a}')

        self._reset()
        logger.info('Disconnected')

    def push(self, message, **kwargs):
        try:
            self._channel.basic_publish(
                self._exchange,
                self._routing_key,
                message,
                **kwargs,
            )
        except AMQPConnectionError as e:
            raise QueueConnectionError from e
        except Exception as e:
            raise QueueOperationError from e

    def pop(self, **kwargs):
        try:
            method_frame, properties, body = (
                next(self._channel.consume(self._queue, **kwargs))
            )
        except AMQPConnectionError as e:
            raise QueueConnectionError from e
        except Exception as e:
            raise QueueOperationError from e

        return {
            'msgid': method_frame.delivery_tag,
            'body': body,
        }


def _get_rabbitmq_queue():
    return RabbitMQQueue('testq')


if __name__ == '__main__':
    q = _get_rabbitmq_queue()
    q.connect()
    input()
    q.push('blah')
    while True:
        print(q.pop(no_ack=True))
    q.disconnect()
