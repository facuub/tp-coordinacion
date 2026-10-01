import pika
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)


def _translate_error(error):
    if isinstance(error, pika.exceptions.AMQPConnectionError):
        return MessageMiddlewareDisconnectedError(str(error))
    return MessageMiddlewareMessageError(str(error))


class _MessageMiddlewareRabbitMQ:

    def _connect(self, host, declare):
        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host, heartbeat=0)
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        try:
            self._channel = self._connection.channel()
            declare()
        except pika.exceptions.AMQPError as e:
            if self._connection.is_open:
                self._connection.close()
            raise _translate_error(e) from e

    def _consume(self, queue_names, on_message_callback):
        def callback(channel, method, properties, body):
            on_message_callback(
                body,
                lambda: channel.basic_ack(delivery_tag=method.delivery_tag),
                lambda: channel.basic_nack(delivery_tag=method.delivery_tag),
            )

        try:
            for queue_name in queue_names:
                self._channel.basic_consume(
                    queue=queue_name, on_message_callback=callback
                )
            self._channel.start_consuming()
        except pika.exceptions.AMQPError as e:
            raise _translate_error(e) from e

    def _publish(self, exchange_name, routing_key, message):
        try:
            self._channel.basic_publish(
                exchange=exchange_name, routing_key=routing_key, body=message
            )
        except pika.exceptions.AMQPError as e:
            raise _translate_error(e) from e

    def stop_consuming(self):
        try:
            self._connection.add_callback_threadsafe(self._channel.stop_consuming)
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e

    def close(self):
        try:
            if self._connection.is_open:
                self._connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError(str(e)) from e


class MessageMiddlewareQueueRabbitMQ(
    _MessageMiddlewareRabbitMQ, MessageMiddlewareQueue
):

    def __init__(self, host, queue_name):
        self._queue_name = queue_name
        self._connect(host, lambda: self._channel.queue_declare(queue=queue_name))

    def start_consuming(self, on_message_callback):
        self._consume([self._queue_name], on_message_callback)

    def send(self, message):
        self._publish("", self._queue_name, message)


class MessageMiddlewareExchangeRabbitMQ(
    _MessageMiddlewareRabbitMQ, MessageMiddlewareExchange
):

    def __init__(self, host, exchange_name, routing_keys):
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
        self._connect(host, self._declare)

    def _declare(self):
        self._channel.exchange_declare(
            exchange=self._exchange_name, exchange_type="direct"
        )
        for routing_key in self._routing_keys:
            queue_name = self._queue_name(routing_key)
            self._channel.queue_declare(queue=queue_name)
            self._channel.queue_bind(
                queue=queue_name, exchange=self._exchange_name, routing_key=routing_key
            )

    def _queue_name(self, routing_key):
        return f"{self._exchange_name}.{routing_key}"

    def start_consuming(self, on_message_callback):
        self._consume(
            [self._queue_name(routing_key) for routing_key in self._routing_keys],
            on_message_callback,
        )

    def send(self, message):
        for routing_key in self._routing_keys:
            self._publish(self._exchange_name, routing_key, message)
