import json
import logging
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import requests
from confluent_kafka import Producer

ES_URL = os.environ.get("ES_URL", "http://elasticsearch.demo.svc.cluster.local:9200")
ES_INDEX = "email"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka.demo.svc.cluster.local:9092")

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def wait_for_topic(topic: str, interval: float = 3.0) -> None:
    while True:
        md = producer.list_topics(timeout=10.0)
        if topic in md.topics and md.topics[topic].error is None:
            log.info("topic %s available", topic)
            return
        log.info("waiting for topic %s...", topic)
        time.sleep(interval)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        if parsed.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        elif parsed.path == "/threads":
            thread_id = params.get("thread_id", [None])[0]
            doc_id = params.get("doc_id", [None])[0]
            if thread_id or doc_id:
                self._get_thread_docs(thread_id, doc_id)
            else:
                self._get_threads()
        else:
            self.send_response(404)
            self.end_headers()

    def _get_threads(self):
        query = {
            "size": 0,
            "aggs": {
                "threads": {
                    "terms": {"field": "thread_id", "size": 10000}
                }
            }
        }
        try:
            resp = requests.post(f"{ES_URL}/{ES_INDEX}/_search", json=query, timeout=10)
            resp.raise_for_status()
            buckets = resp.json()["aggregations"]["threads"]["buckets"]
            threads = [{"thread_id": b["key"], "count": b["doc_count"]} for b in buckets]
            self._respond(200, json.dumps({"threads": threads, "total": len(threads)}).encode())
        except Exception as exc:
            log.error("failed to fetch threads: %s", exc)
            self._respond(500, json.dumps({"error": str(exc)}).encode())

    def _get_thread_docs(self, thread_id: str | None, doc_id: str | None):
        filters = []
        if thread_id:
            filters.append({"term": {"thread_id": thread_id}})
        if doc_id:
            filters.append({"term": {"doc_id": doc_id}})
        query = {
            "size": 1000,
            "query": {"bool": {"filter": filters}},
            "sort": [{"canon_order": "asc"}],
        }
        try:
            resp = requests.post(f"{ES_URL}/{ES_INDEX}/_search", json=query, timeout=10)
            resp.raise_for_status()
            hits = resp.json()["hits"]["hits"]
            docs = [h["_source"] for h in hits]
            self._respond(200, json.dumps({"thread_id": thread_id, "doc_id": doc_id, "docs": docs, "total": len(docs)}).encode())
        except Exception as exc:
            log.error("failed to fetch thread docs thread_id=%s doc_id=%s: %s", thread_id, doc_id, exc)
            self._respond(500, json.dumps({"error": str(exc)}).encode())

    def do_POST(self):
        if self.path != "/ingest":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", 0))
        try:
            body = json.loads(self.rfile.read(length))
            doc_url = body["doc_url"]
        except (json.JSONDecodeError, KeyError):
            self._respond(400, b"body must be JSON with doc_url field")
            return

        message = json.dumps({"doc_url": doc_url}).encode()
        producer.produce(topic="documents", value=message)
        producer.flush()
        log.info("published doc_url=%s to documents", doc_url)

        self._respond(202, json.dumps({"doc_url": doc_url, "status": "queued"}).encode())

    def _respond(self, code: int, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    wait_for_topic("documents")
    HTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
