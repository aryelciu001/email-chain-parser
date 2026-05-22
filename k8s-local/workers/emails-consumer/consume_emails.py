import hashlib
import json
import logging
import os
import time

import requests
from confluent_kafka import Consumer, Producer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka.demo.svc.cluster.local:9092")
ES_URL = os.environ.get("ES_URL", "http://elasticsearch.demo.svc.cluster.local:9200")
ES_INDEX = "email"


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


def find_similar(msg: dict) -> list[dict]:
    query = {
        "size": 5,
        "query": {
            "bool": {
                "filter": [
                    {"terms": {"from": msg.get("from", [])}},
                    {"terms": {"to": msg.get("to", [])}},
                ],
                "must": [
                    {"match": {"content": msg.get("content", "")}},
                ],
            }
        },
    }
    resp = requests.post(f"{ES_URL}/{ES_INDEX}/_search", json=query, timeout=10)
    resp.raise_for_status()
    return resp.json()["hits"]["hits"]


def process(msg: dict) -> None:
    doc_id = msg["doc_id"]
    canon_order = msg["canon_order"]
    es_id = f"{doc_id}:{canon_order}"

    hits = find_similar(msg)
    if hits:
        best = hits[0]
        log.info("best content similarity id=%s score=%.3f", best["_id"], best["_score"])
    else:
        log.info("no similar email found")

    content = msg.get("content") or ""
    doc = {
        "id": es_id,
        "doc_id": doc_id,
        "canon_order": canon_order,
        "from": msg.get("from"),
        "to": msg.get("to"),
        "subject": msg.get("subject"),
        "date": msg.get("date"),
        "content": content,
        "content_hash": hashlib.md5(content.encode()).hexdigest(),
    }
    resp = requests.put(f"{ES_URL}/{ES_INDEX}/_doc/{es_id}", json=doc, timeout=10)
    resp.raise_for_status()
    log.info("indexed email id=%s subject=%s", es_id, msg.get("subject"))


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
