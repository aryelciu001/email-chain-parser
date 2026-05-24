import json
import logging
import os
import pathlib
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import requests
from confluent_kafka import Producer

FRONTEND_DIR = pathlib.Path(__file__).parent / "frontend"

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
        if parsed.path in ("/", "/threads", "/docs"):
            self._serve_file(FRONTEND_DIR / "index.html", "text/html; charset=utf-8")
        elif self._is_static(parsed.path):
            self._serve_static(parsed.path)
        elif parsed.path == "/api/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        elif parsed.path == "/api/threads":
            thread_id = params.get("thread_id", [None])[0]
            doc_id = params.get("doc_id", [None])[0]
            if thread_id or doc_id:
                self._get_thread_docs(thread_id, doc_id)
            else:
                self._get_threads()
        elif parsed.path == "/api/docs":
            self._get_all_docs()
        else:
            self.send_response(404)
            self.end_headers()

    def _get_threads(self):
        query = {
            "size": 0,
            "aggs": {
                "threads": {
                    "terms": {"field": "thread_id", "size": 10000},
                    "aggs": {
                        "first_email": {
                            "top_hits": {
                                "size": 1,
                                "sort": [{"canon_order": "asc"}],
                                "_source": ["subject"]
                            }
                        }
                    }
                }
            }
        }
        try:
            resp = requests.post(f"{ES_URL}/{ES_INDEX}/_search", json=query, timeout=10)
            resp.raise_for_status()
            buckets = resp.json()["aggregations"]["threads"]["buckets"]
            threads = []
            for b in buckets:
                hits = b["first_email"]["hits"]["hits"]
                subject = hits[0]["_source"].get("subject", "") if hits else ""
                threads.append({"thread_id": b["key"], "count": b["doc_count"], "subject": subject})
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

    def _get_all_docs(self):
        query = {
            "size": 0,
            "aggs": {
                "docs": {
                    "terms": {"field": "doc_id", "size": 10000, "order": {"_key": "asc"}}
                }
            }
        }
        try:
            resp = requests.post(f"{ES_URL}/{ES_INDEX}/_search", json=query, timeout=10)
            resp.raise_for_status()
            buckets = resp.json()["aggregations"]["docs"]["buckets"]
            docs = [{"doc_id": b["key"], "count": b["doc_count"]} for b in buckets]
            self._respond(200, json.dumps({"docs": docs, "total": len(docs)}).encode())
        except Exception as exc:
            log.error("failed to fetch all docs: %s", exc)
            self._respond(500, json.dumps({"error": str(exc)}).encode())

    def do_POST(self):
        if self.path != "/api/ingest":
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

    _MIME = {
        ".html": "text/html; charset=utf-8",
        ".css":  "text/css; charset=utf-8",
        ".js":   "application/javascript; charset=utf-8",
    }

    def _is_static(self, url_path: str) -> bool:
        return pathlib.PurePosixPath(url_path).suffix in self._MIME

    def _serve_static(self, url_path: str) -> None:
        # strip leading slash, resolve inside frontend dir only
        rel = pathlib.PurePosixPath(url_path).relative_to("/")
        file_path = (FRONTEND_DIR / rel).resolve()
        # prevent path traversal outside frontend dir
        if not str(file_path).startswith(str(FRONTEND_DIR.resolve())):
            self.send_response(403)
            self.end_headers()
            return
        content_type = self._MIME.get(file_path.suffix, "application/octet-stream")
        self._serve_file(file_path, content_type)

    def _serve_file(self, path: pathlib.Path, content_type: str) -> None:
        try:
            data = path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except FileNotFoundError:
            self.send_response(404)
            self.end_headers()

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
