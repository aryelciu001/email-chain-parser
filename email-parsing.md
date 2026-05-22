# Email Deduplication Plan (emails-consumer)

How the emails-consumer deduplicates parsed emails into canonical threads when indexing into Elasticsearch.

## Goal

For each incoming email, decide one of three outcomes:

1. **Exact match** — identical content already indexed → ignore + log.
2. **Similar match** — near-duplicate (minor edits/whitespace) → link to the canonical original.
3. **No match** — genuinely new email → index as a new canonical thread.

## Schema additions (Elasticsearch `email` index)

Exact participant matching requires *all* `from` and *all* `to` to be identical. ES `terms` matches *any* value, not set-equality, so store canonical join keys (sorted addresses joined by `,`):

- `from_key` — keyword, `",".join(sorted(from))`
- `to_key` — keyword, `",".join(sorted(to))`
- `content_hash` — keyword, md5 of normalized content
- `thread_id` — keyword

## Message & thread propagation

One `emails` message carries all emails of a document: `{ "doc_id", "emails": [...] }`.
`process(msg)` sorts them by `canon_order` and processes in order, carrying a `thread_id`
across the document (initially `None`). Each `process_email` returns the thread_id the
email belongs to, which seeds the next email. This makes a new document **extend** the
thread it matches.

## Algorithm — `process_email(email, thread_id)`

1. Compute `content_hash`, `from_key`, `to_key`.
2. Query ES:
   - `filter`: exact `term` on `from_key` **and** `to_key`; **plus `term` on `thread_id`** when one is already known (scopes matching to the established thread so we only extend it).
   - `must`: `match` on `content` → BM25 relevance scoring.
   - Candidates returned sorted by score.
3. If candidates exist, take the best (`hits[0]`):
   - **Exact** — `best.content_hash == content_hash` → log, skip (no index). Return `best.thread_id`.
   - **Similar** — `Levenshtein.ratio(content, best.content) > 0.9`:
     - `id = best.id + "m"` (matching `doc1_2m` yields `doc1_2mm`)
     - `thread_id = best.thread_id`, index. Return it.
   - **Below threshold** → fall through to new.
4. No candidate (or below threshold) → **new/extend**:
   - `id = "{docname}_{order}"` — strip `.txt`, e.g. `doc1_2`
   - `thread_id =` carried-in thread_id if known (extends it), else `uuid4()` (brand-new thread)
   - index. Return it.

## ID scheme

- Natural id: `{docname}_{order}` (e.g. `doc1_2`).
- Near-duplicate: original id + `m` suffix (e.g. `doc1_2m`, `doc1_2mm`). Mirrors the eval ground-truth convention (`5_1` vs `5_1m`).
- The id encodes canonical grouping; `doc_id` + `canon_order` fields preserve true provenance (the `m` email may originate from a different source document).

## Similarity metric

- **Levenshtein ratio** via `rapidfuzz` (bounded 0–1, char-level, handles whitespace + small word edits). Threshold `> 0.9`.
- BM25 `_score` is used only for candidate retrieval (unbounded/corpus-relative, unsuitable as a threshold).

## Accepted prototype limitations

- **Participant filter is strict** — a near-dup with a tweaked recipient list won't be retrieved and is treated as new. Expected.
- **ID collision** — two distinct incoming emails best-matching the same `doc1_2` with equal similarity in the same poll both compute `doc1_2m`; the second overwrites. The "add more m" rule mitigates once the first `m` is indexed.
- **BM25-best ≠ Levenshtein-best** — only the top BM25 candidate's content is compared, not all hits.

## Dependencies

- `rapidfuzz` (add to `emails-consumer/requirements.txt`).
