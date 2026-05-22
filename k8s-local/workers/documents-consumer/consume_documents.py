import json
import logging
import os

from confluent_kafka import Consumer, Producer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka.demo.svc.cluster.local:9092")


RETRY_TOPIC = "documents-retry"


def make_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "documents-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def process(msg: dict) -> None:
    pass


def run() -> None:
    consumer = make_consumer()
    producer = make_producer()
    consumer.subscribe(["documents"])
    log.info("subscribed to documents")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            log.error("consumer error: %s", msg.error())
            continue
        try:
            process(json.loads(msg.value()))
        except Exception as exc:
            log.error("failed to process message, publishing to %s: %s", RETRY_TOPIC, exc)
            producer.produce(topic=RETRY_TOPIC, value=msg.value())
            producer.flush()
        consumer.commit(msg)


if __name__ == "__main__":
    run()
