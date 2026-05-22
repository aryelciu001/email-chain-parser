import json
import logging
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from confluent_kafka import Producer

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
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

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
