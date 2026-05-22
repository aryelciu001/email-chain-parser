# Assignment: Email Ingestion and Deduplication

## Goal
Build a prototype that ingests raw email threads and deduplicates them into **canonical threads**.

---

## Problem
Emails from multiple sources have:
- **Duplicates** — exact copies of the same email
- **Near-duplicates** — minor formatting/whitespace/encoding differences
- **Threading** — emails form chains via replies/forwards

---

## Data Model

Each raw document is a sequence of emails in a thread:
```
doc1: 0
doc2: 0-1
doc3: 0-1m       ← near-duplicate of doc2
doc4: 0-1-2
doc5: 0-1-2m     ← near-duplicate of doc4
```
`m` suffix = slightly modified (near-duplicate).

---

## Expected Output

**Canonical threads** — group near-identical sequences:
```
Canon0: [doc1]        → sequence: 0
Canon1: [doc2, doc3]  → sequence: 0-1 / 0-1m
Canon2: [doc4, doc5]  → sequence: 0-1-2 / 0-1-2m
```

**Hierarchy:**
```
Canon0 → Canon1 → Canon2
```

---

## Implementation Requirements

### 1. Data Ingestion
- Pipeline that processes raw email threads as they arrive
- Multiple workers for parallel processing via Kubernetes deployment

### 2. Canonical Thread Construction
- Group near-duplicate threads into canonical threads
- Map raw document IDs → canonical thread ID
- Maintain parent/child links between canonical threads
- Construct in real-time as documents are ingested

### 3. Data Storage / Queries
Database must support efficient queries for:
- Canonical thread ID given a raw document ID (filename)
- All raw document IDs given a canonical thread ID
- Parent/children canonical thread IDs of a given canonical thread

---

## Data

| Directory | Description |
|---|---|
| `test/` | Raw files (`docXXXX.txt`), no ground truth |
| `eval/` | Files with ground truth encoded in filename (e.g. `5_1m.txt`, `5_1.txt`) |

Eval filename convention: `5_1m` and `5_1` share the same canonical thread (same sequence, minor variation).

---

## Evaluation Criteria
- **Correctness** — identifies duplicates/near-duplicates (100% not required)
- **Efficiency** — handles real-time ingestion of multiple threads
- **Scalability** — extensible to larger datasets

Prototype quality expected — shortcuts are fine.
