import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_items_by_client = {}
        self.partial_tops_by_client = {}

    def _send_top(self, client_id):
        logging.info(f"Sending top for client {client_id}")
        fruit_items = self.fruit_items_by_client.pop(client_id)
        del self.partial_tops_by_client[client_id]
        fruit_top = sorted(fruit_items, reverse=True)[:TOP_SIZE]
        self.output_queue.send(
            message_protocol.internal.serialize(
                [client_id, [[item.fruit, item.amount] for item in fruit_top]]
            )
        )

    def process_messsage(self, message, ack, nack):
        [client_id, partial_top] = message_protocol.internal.deserialize(message)
        self.fruit_items_by_client.setdefault(client_id, []).extend(
            fruit_item.FruitItem(fruit, amount) for fruit, amount in partial_top
        )
        self.partial_tops_by_client[client_id] = (
            self.partial_tops_by_client.get(client_id, 0) + 1
        )
        if self.partial_tops_by_client[client_id] == AGGREGATION_AMOUNT:
            self._send_top(client_id)
        ack()

    def stop(self):
        self.input_queue.stop_consuming()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)
        self.input_queue.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: join_filter.stop())
    join_filter.start()
    return 0


if __name__ == "__main__":
    main()
