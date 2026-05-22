# Solution Plan

## Schema

### Document
- `id`
- `url`

### Email
- `id`
- `doc_id`
- `from` (emails, asc order)
- `to` (emails, asc order)
- `thread_id`
- `canon_order`
- `content`

---

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

## ES Mapping
- `id`: keyword
- `doc_id`: keyword
- `from` (emails, asc order): keyword
- `to` (emails, asc order): keyword
- `thread_id`: keyword
- `canon_order`: keyword
- `content`: text