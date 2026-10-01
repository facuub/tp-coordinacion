import os
import logging
import signal
import threading
import zlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


def _build_data_output_exchanges():
    return [
        middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
        )
        for i in range(AGGREGATION_AMOUNT)
    ]


def _aggregation_index(fruit):
    return zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.control_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_{ID}"]
        )
        self.control_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT)],
        )
        self.data_output_exchanges = _build_data_output_exchanges()
        self.control_data_output_exchanges = _build_data_output_exchanges()
        self.amount_by_fruit_by_client = {}
        self.records_by_client = {}
        self.total_by_client = {}
        self.lock = threading.Lock()

    def _flush(self, client_id, data_output_exchanges, is_control):
        amount_by_fruit = self.amount_by_fruit_by_client.pop(client_id, {})
        records = self.records_by_client.pop(client_id, 0)
        fruits_by_aggregation = [[] for _ in range(AGGREGATION_AMOUNT)]
        for item in amount_by_fruit.values():
            fruits_by_aggregation[_aggregation_index(item.fruit)].append(
                [item.fruit, item.amount]
            )
        for data_output_exchange, fruits in zip(
            data_output_exchanges, fruits_by_aggregation
        ):
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    [
                        client_id,
                        self.total_by_client[client_id],
                        records,
                        is_control,
                        fruits,
                    ]
                )
            )

    def _process_data(self, client_id, fruit, amount):
        with self.lock:
            amount_by_fruit = self.amount_by_fruit_by_client.setdefault(client_id, {})
            amount_by_fruit[fruit] = amount_by_fruit.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
            self.records_by_client[client_id] = (
                self.records_by_client.get(client_id, 0) + 1
            )
            if client_id in self.total_by_client:
                self._flush(client_id, self.data_output_exchanges, False)

    def _process_eof(self, client_id, total):
        logging.info(f"Broadcasting EOF for client {client_id}")
        self.control_output_exchange.send(
            message_protocol.internal.serialize([client_id, total])
        )

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def process_control_message(self, message, ack, nack):
        [client_id, total] = message_protocol.internal.deserialize(message)
        logging.info(f"Flushing data for client {client_id}")
        with self.lock:
            self.total_by_client[client_id] = total
            self._flush(client_id, self.control_data_output_exchanges, True)
        ack()

    def _consume_control(self):
        self.control_input_exchange.start_consuming(self.process_control_message)

    def stop(self):
        self.input_queue.stop_consuming()
        self.control_input_exchange.stop_consuming()

    def close(self):
        for middleware_instance in [
            self.input_queue,
            self.control_input_exchange,
            self.control_output_exchange,
            *self.data_output_exchanges,
            *self.control_data_output_exchanges,
        ]:
            middleware_instance.close()

    def start(self):
        control_thread = threading.Thread(target=self._consume_control)
        control_thread.start()
        self.input_queue.start_consuming(self.process_data_messsage)
        control_thread.join()
        self.close()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: sum_filter.stop())
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
