# Solution Plan

## Architecture

### Components

1. **Server** — HTTP API, accepts user requests
2. **Document Storage** — stores raw documents
3. **Email Storage** — Elasticsearch; stores parsed/deduped emails with full-text + similarity search
4. **Kafka topic: documents** — queue for document processing jobs
5. **Kafka topic: emails** — queue for email deduplication jobs
6. **Parser Worker** — consumes `documents` topic, parses emails, stores to email storage, produces to `emails` topic
7. **Dedup Worker** — consumes `emails` topic, deduplicates and assigns `thread_id` + `canon_order`

### Flow

```
User
 └─> POST /documents (id, url)
      ├─> store document → Document Storage
      └─> produce { doc_id } → Kafka topic: documents
                                      │
                             Parser Worker (consumer: documents)
                              ├─> fetch document from Document Storage
                              ├─> parse emails from document
                              ├─> store emails → Email Storage
                              └─> produce { email } → Kafka topic: emails
                                                              │
                                                   Dedup Worker (consumer: emails)
                                                    ├─> deduplicate against existing emails
                                                    ├─> assign thread_id + canon_order
                                                    └─> update Email Storage
```

---

## Kafka Topics

Created by the `kafka-init-topics` Job (see `k8s-local/manifests/kafka/deployment.yaml`). Auto-create is disabled (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`).

| Topic | Partitions | Replication | Producer | Consumer |
|-------|-----------|-------------|----------|----------|
| `documents` | 20 | 1 | Server (`POST /ingest`) | Parser Worker |
| `documents-retry` | 20 | 1 | Parser Worker (on failure) | — |
| `emails` | 20 | 1 | Parser Worker | Dedup Worker |
| `emails-retry` | 20 | 1 | Dedup Worker (on failure) | — |

### Message Schemas

- `documents`: `{ "doc_url": "<path>" }`
- `emails`: `{ "email": <Email object>, "from": <list of email addresses>, "to": <list of email addresses>, "docId": <doc_id> }`

### Conventions

- **Partitions = 20** — caps consumer parallelism per topic; consumers scale up to 20 active instances.
- **Replication = 1** — single-broker local cluster; not production-safe.
- **Retry topics** — failed messages are republished to `<topic>-retry` rather than blocking the main topic. No automatic redrive; retry topics are inspected/replayed manually.
- **Manual offset commit** — consumers set `enable.auto.commit=false` and commit only after processing, giving at-least-once delivery.
- **Topic-availability wait** — server and consumers poll `list_topics()` on startup until their topic exists before producing/subscribing, tolerating cold-start ordering against the init Job.

---

## HTTP API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/documents` | Ingest a new document `{ id, url }` |
| `GET`  | `/documents?thread_id=x` | Get all documents for a canonical thread |
| `GET`  | `/emails?doc_id=x` | Get all emails for a document |

---

## Workers (Kubernetes Deployments)

- **Parser Worker**: horizontally scalable, consumes `documents` topic
- **Dedup Worker**: horizontally scalable, consumes `emails` topic

---

## Assumptions

- Document Storage holds only `{ id, url }` — no raw content stored server-side
- Parser Worker fetches document content directly via URL from the Kafka message; does not read Document Storage
- Parser Worker stores raw email content to Email Storage, then produces `{ email_id }` to Kafka — no other DB writes
- Dedup Worker is the sole writer for `thread_id` + `canon_order` fields on Email Storage
- Email Storage is Elasticsearch — chosen for horizontal scalability, fuzzy/full-text search, and auto-rebalancing
- Pipeline is idempotent — Dedup Worker skips exact-match duplicates
- Kafka partitioning ensures a given email is consumed by exactly one Dedup Worker (no race conditions)
- `from` / `to` fields are raw email address strings; not used in dedup logic
- `GET /documents?thread_id=x` returns all documents that have at least one email with `thread_id = x`
- Hierarchy between canonical threads is implicit via `canon_order` prefix — no explicit `parent_id` field needed

---

## ES Configuration

ES contains result of parsed douments -> email

### ES Mapping
- `id`: keyword
- `doc_id`: keyword
- `from` (emails, asc order): keyword
- `to` (emails, asc order): keyword
- `thread_id`: keyword
- `canon_order`: keyword
- `content`: text

---

## Postgres Configuration

### Table - Document
- `id` (auto increment)
- `name`
- `url`