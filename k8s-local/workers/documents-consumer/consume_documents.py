import json
import logging
import os
import re
import time
from datetime import datetime
from email.utils import getaddresses

import psycopg2
from confluent_kafka import Consumer, Producer
from dateutil import parser as dateparser

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka.demo.svc.cluster.local:9092")
POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN",
    "host=postgres.demo.svc.cluster.local dbname=emaildb user=app password=app",
)
DATA_DIR = os.environ.get("DATA_DIR", "/data/test")


RETRY_TOPIC = "documents-retry"
EMAILS_TOPIC = "emails"

db = psycopg2.connect(POSTGRES_DSN)
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

SEPARATOR = re.compile(r"\n-{40}\n")
QUOTE_PREFIX = re.compile(r"^[|>]+ *", re.MULTILINE)
INLINE_QUOTE_START = re.compile(r"\n(>{1,}|[|]) *From:", re.IGNORECASE)
HEADER_RE = re.compile(r"^(From|To|Cc|Date|Subject|Message-ID):\s*(.+)", re.IGNORECASE)


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


def strip_prefix(block: str) -> str:
    return QUOTE_PREFIX.sub("", block)


def parse_block(block: str) -> dict:
    lines = block.splitlines()
    headers: dict[str, str] = {}
    body_lines: list[str] = []
    in_body = False
    for line in lines:
        if in_body:
            body_lines.append(line)
            continue
        if line.strip() == "":
            in_body = True
            continue
        m = HEADER_RE.match(line)
        if m:
            key = m.group(1).lower().replace("-", "_")
            headers[key] = m.group(2).strip()
        else:
            in_body = True
            body_lines.append(line)
    return {
        "from": addrs(headers.get("from")),
        "to": addrs(headers.get("to")),
        "cc": addrs(headers.get("cc")),
        "date": headers.get("date"),
        "subject": headers.get("subject"),
        "message_id": headers.get("message_id"),
        "content": "\n".join(body_lines).strip(),
    }


def split_inline(block: str) -> list[tuple[str, bool]]:
    parts = []
    remaining = block
    quoted = False
    while True:
        m = INLINE_QUOTE_START.search(remaining)
        if not m:
            parts.append((remaining, quoted))
            break
        parts.append((remaining[: m.start()], quoted))
        remaining = remaining[m.start() + 1:]
        quoted = True
    return parts


def parse_date(date_str: str | None) -> datetime:
    if not date_str:
        return datetime.min
    try:
        return dateparser.parse(date_str, ignoretz=True)
    except Exception:
        return datetime.min


def parse_emails(path: str, doc_id: str) -> list[dict]:
    with open(path) as f:
        raw = f.read()

    sep_blocks = SEPARATOR.split(raw)
    emails = []
    for sep_idx, block in enumerate(sep_blocks):
        block = block.strip()
        if not block:
            continue
        if sep_idx > 0:
            block = strip_prefix(block)
        for text, is_quoted in split_inline(block):
            text = text.strip()
            if not text:
                continue
            if is_quoted:
                text = strip_prefix(text)
            emails.append(parse_block(text))

    emails.sort(key=lambda e: parse_date(e.get("date")))
    for i, e in enumerate(emails):
        e["doc_id"] = doc_id
        e["canon_order"] = i
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
