# Email Ingestion & Deduplication

Prototype pipeline that ingests raw email threads (mbox `.txt` files), parses them into individual emails, and deduplicates them into canonical threads. Runs on local Kubernetes (minikube).

See [plan.md](plan.md) for architecture and [assignment.md](assignment.md) for the spec.

## Architecture

```
POST /ingest {doc_url}
  → server publishes to Kafka `documents`
  → documents-consumer: stores doc to Postgres, parses emails, publishes to Kafka `emails`
  → emails-consumer: deduplicates, assigns thread_id (Elasticsearch)
```

Components: HTTP server, Kafka, Postgres (documents), Elasticsearch (emails), two consumer deployments.

## Prerequisites

- `minikube`, `kubectl`, `docker`

## Setup

```bash
# 1. Start cluster + ingress addon
make cluster-up

# 2. Build images
make build

# 3. Deploy (also copies sample-data into the minikube node for the document worker)
make deploy
```

> **Wait ~1-2 minutes after `make deploy`.** Kafka topics and the Postgres table are
> created by init Jobs that run *after* Kafka/Postgres become ready. The server and
> consumers block until their topics exist (logging `waiting for topic ...`), so
> ingestion will not work until those Jobs finish. Check readiness with `make status`
> — wait until all pods are `Running` and the init Jobs show `Completed`.

### Expose the ingress

In a separate terminal:

```bash
make tunnel        # keep running
```

Add the host to `/etc/hosts` (one time):

```bash
echo "127.0.0.1 my.local" | sudo tee -a /etc/hosts
```

Verify:

```bash
make test          # GET /health → "ok"
```

## Usage

```bash
make ingest DOC=doc1.txt     # ingest a file from sample-data/
```

## Inspecting

```bash
make status            # pods, services, ingress
make get-apps          # deployments
make log-server        # server logs
make log-document      # documents-consumer logs
make log-email         # emails-consumer logs
make scale             # scale server to 4 replicas
```

## Teardown

```bash
make cluster-down
```
