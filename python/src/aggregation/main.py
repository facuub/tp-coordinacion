import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.amount_by_fruit_by_client = {}
        self.records_by_client = {}
        self.controls_by_client = {}

    def _send_top(self, client_id):
        logging.info(f"Sending partial top for client {client_id}")
        amount_by_fruit = self.amount_by_fruit_by_client.pop(client_id)
        del self.records_by_client[client_id]
        del self.controls_by_client[client_id]
        fruit_top = sorted(amount_by_fruit.values(), reverse=True)[:TOP_SIZE]
        self.output_queue.send(
            message_protocol.internal.serialize(
                [client_id, [[item.fruit, item.amount] for item in fruit_top]]
            )
        )

    def process_messsage(self, message, ack, nack):
        [client_id, total, records, is_control, fruits] = (
            message_protocol.internal.deserialize(message)
        )
        amount_by_fruit = self.amount_by_fruit_by_client.setdefault(client_id, {})
        for fruit, amount in fruits:
            amount_by_fruit[fruit] = amount_by_fruit.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, amount)
        self.records_by_client[client_id] = (
            self.records_by_client.get(client_id, 0) + records
        )
        self.controls_by_client[client_id] = self.controls_by_client.get(
            client_id, 0
        ) + int(is_control)
        if (
            self.controls_by_client[client_id] == SUM_AMOUNT
            and self.records_by_client[client_id] == total
        ):
            self._send_top(client_id)
        ack()

    def stop(self):
        self.input_exchange.stop_consuming()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)
        self.input_exchange.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: aggregation_filter.stop())
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
