import json
import logging
import os
import time

from confluent_kafka import Consumer, Producer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka.demo.svc.cluster.local:9092")


RETRY_TOPIC = "emails-retry"


def make_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "emails-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def process(msg: dict) -> None:
    pass


def wait_for_topic(consumer: Consumer, topic: str, interval: float = 3.0) -> None:
    while True:
        md = consumer.list_topics(timeout=10.0)
        if topic in md.topics and md.topics[topic].error is None:
            log.info("topic %s available", topic)
            return
        log.info("waiting for topic %s...", topic)
        time.sleep(interval)


def run() -> None:
    consumer = make_consumer()
    producer = make_producer()
    wait_for_topic(consumer, "emails")
    consumer.subscribe(["emails"])
    log.info("subscribed to emails")
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
