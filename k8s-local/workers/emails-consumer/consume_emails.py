import hashlib
import json
import logging
import os
import time
import uuid

import requests
from confluent_kafka import Consumer, Producer
from rapidfuzz.distance import Levenshtein

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka.demo.svc.cluster.local:9092")
ES_URL = os.environ.get("ES_URL", "http://elasticsearch.demo.svc.cluster.local:9200")
ES_INDEX = "email"
SIMILARITY_THRESHOLD = 0.9


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


def key_of(addresses: list | None) -> str:
    return ",".join(sorted(addresses or []))


def find_similar(msg: dict, from_key: str, to_key: str, thread_id: str | None) -> list[dict]:
    filters = [
        {"term": {"from_key": from_key}},
        {"term": {"to_key": to_key}},
    ]
    if thread_id:
        filters.append({"term": {"thread_id": thread_id}})
    query = {
        "size": 5,
        "query": {
            "bool": {
                "filter": filters,
                "must": [
                    {"match": {"content": msg.get("content", "")}},
                ],
            }
        },
    }
    resp = requests.post(f"{ES_URL}/{ES_INDEX}/_search", json=query, timeout=10)
    resp.raise_for_status()
    return resp.json()["hits"]["hits"]


def index_email(es_id: str, msg: dict, thread_id: str, content: str,
                content_hash: str, from_key: str, to_key: str) -> None:
    doc = {
        "id": es_id,
        "doc_id": msg["doc_id"],
        "canon_order": msg["canon_order"],
        "from": msg.get("from"),
        "to": msg.get("to"),
        "from_key": from_key,
        "to_key": to_key,
        "thread_id": thread_id,
        "subject": msg.get("subject"),
        "date": msg.get("date"),
        "content": content,
        "content_hash": content_hash,
    }
    resp = requests.put(f"{ES_URL}/{ES_INDEX}/_doc/{es_id}", json=doc, timeout=10)
    resp.raise_for_status()
    log.info("indexed email id=%s thread_id=%s", es_id, thread_id)


def process_email(email: dict, thread_id: str | None) -> str:
    """Index one email, returning the thread_id it belongs to (to carry forward)."""
    docname = os.path.splitext(email["doc_id"])[0]
    content = email.get("content") or ""
    content_hash = hashlib.md5(content.encode()).hexdigest()
    from_key = key_of(email.get("from"))
    to_key = key_of(email.get("to"))

    hits = find_similar(email, from_key, to_key, thread_id)
    if hits:
        best = hits[0]
        src = best["_source"]
        if src.get("content_hash") == content_hash:
            log.info("exact content match id=%s, skipping", best["_id"])
            return src["thread_id"]
        ratio = Levenshtein.normalized_similarity(content, src.get("content") or "")
        log.info("best candidate id=%s lev=%.3f", best["_id"], ratio)
        if ratio > SIMILARITY_THRESHOLD:
            es_id = best["_id"] + "m"
            index_email(es_id, email, src["thread_id"], content, content_hash, from_key, to_key)
            return src["thread_id"]

    thread_id = thread_id or str(uuid.uuid4())
    es_id = f"{docname}_{email['canon_order']}"
    index_email(es_id, email, thread_id, content, content_hash, from_key, to_key)
    return thread_id


def process(msg: dict) -> None:
    emails = sorted(msg["emails"], key=lambda e: e["canon_order"])
    log.info("processing %s emails for doc_id=%s", len(emails), msg.get("doc_id"))
    thread_id = None
    for email in emails:
        thread_id = process_email(email, thread_id)


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
