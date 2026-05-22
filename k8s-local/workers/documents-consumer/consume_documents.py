import email
import json
import logging
import os
import re
import time
from email.utils import getaddresses

import psycopg2
from confluent_kafka import Consumer, Producer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka.demo.svc.cluster.local:9092")
POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN",
    "host=postgres.demo.svc.cluster.local dbname=emaildb user=app password=app",
)
DATA_DIR = os.environ.get("DATA_DIR", "/data/sample-data")


RETRY_TOPIC = "documents-retry"
EMAILS_TOPIC = "emails"

db = psycopg2.connect(POSTGRES_DSN)
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def make_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "documents-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })


def addrs(header: str | None) -> list[str]:
    if not header:
        return []
    return sorted(a.lower() for _, a in getaddresses([header]) if a)


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def parse_emails(path: str, doc_id: str) -> list[dict]:
    with open(path) as f:
        raw = f.read()
    chunks = [c for c in re.split(r"(?m)^From .*\n", raw) if c.strip()]
    emails = []
    for order, chunk in enumerate(chunks):
        m = email.message_from_string(chunk)
        emails.append({
            "doc_id": doc_id,
            "canon_order": order,
            "from": addrs(m["From"]),
            "to": addrs(m["To"]),
            "subject": m["Subject"],
            "date": m["Date"],
            "content": normalize(m.get_payload()),
        })
    return emails


def process(msg: dict) -> None:
    doc_url = msg["doc_url"]
    doc_id = os.path.basename(doc_url)
    try:
        with db.cursor() as cur:
            cur.execute(
                "INSERT INTO document (name, url) VALUES (%s, %s) "
                "ON CONFLICT (name) DO NOTHING",
                (doc_id, doc_url),
            )
            if cur.rowcount == 0:
                db.rollback()
                log.info("document name=%s already processed, skipping", doc_id)
                return

        emails = parse_emails(os.path.join(DATA_DIR, doc_id), doc_id)
        errors: list = []
        producer.produce(
            topic=EMAILS_TOPIC,
            key=doc_id.encode(),
            value=json.dumps({"doc_id": doc_id, "emails": emails}).encode(),
            on_delivery=lambda err, _m: err and errors.append(err),
        )
        producer.flush()
        if errors:
            raise RuntimeError(f"kafka delivery failed: {errors}")

        db.commit()
        log.info("stored document name=%s, produced %s emails to %s", doc_id, len(emails), EMAILS_TOPIC)
    except Exception:
        db.rollback()
        raise


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
    wait_for_topic(consumer, "documents")
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
