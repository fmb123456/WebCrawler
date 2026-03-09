# 05. Docker Settings and Deployment Notes

## 5.1 Compose Service Definitions

From `docker-compose.yml`, services are:

- `postgres`
- `scheduler_control`
- `scheduler_ingest`
- `crawler`

Common runtime properties on non-DB services:

- `init: true`
- `restart: unless-stopped`
- bind mount source code: `./:/app`
- shared IPC mount: `/data/ipc:/data/ipc`
- JSON file log driver with rotation:
  - `max-size: 50m`
  - `max-file: 10`

Crawler-specific networking:

- DNS explicitly set to `8.8.8.8` and `1.1.1.1`.

## 5.2 Dockerfiles

### `containers/scheduler_control/Dockerfile`

- Base: `python:3.12-slim`
- Installs: `supervisor`
- Python deps: `sqlalchemy`, `psycopg2-binary`, `pyyaml`
- Entrypoint: supervisord

### `containers/scheduler_ingest/Dockerfile`

- Base: `python:3.12-slim`
- Installs: `supervisor`
- Python deps: `tldextract`, `pyyaml`, `sqlalchemy`, `psycopg2-binary`
- Entrypoint: supervisord

### `containers/crawler/Dockerfile`

- Base: `python:3.12-slim`
- Installs: `supervisor`
- Python deps: `scrapy`, `requests`, `tldextract`
- `PYTHONPATH=/app`
- Entrypoint: supervisord

## 5.3 Supervisor Process Topology

- `scheduler_control`: 16 offerers + 1 accounting rolloff worker
- `scheduler_ingest`: 16 routers + 16 ingestors + 16 extractors + 1 stats aggregator
- `crawler`: 16 spiders

Total long-running app processes (excluding postgres internals):

- `17 + 49 + 16 = 82`

## 5.4 Operational Configuration Coupling

The following must remain aligned:

- `offerer.total_shards`, `router.num_shards`, and actual number of shard tables.
- `offerer.shards_per_offerer` and offerer process count.
- `router.shards_per_ingestor` and ingest/extractor process counts.
- queue/result/progress path templates across all services.

## 5.5 Current DSN and Network Assumption

Configured DSN in YAML:

- `postgresql+psycopg2://crawler:crawler@172.16.191.1:5432/crawlerdb`

Implication:

- Services currently expect PostgreSQL at host IP `172.16.191.1`, not `postgres` service DNS name.
- If using compose-internal networking for DB, update DSN host accordingly.

## 5.6 Startup and Health Considerations

- `postgres` has healthcheck but application services do not declare `depends_on` with health conditions.
- Services rely on internal retry/restart behavior.
- Router has explicit transient DB retry (3 attempts with backoff).
- Accounting rolloff runs daily in UTC and also supports one-shot execution (`--once`) for manual verification.

## 5.7 Data Durability

- DB durability: persisted via `/data/postgres` mount.
- IPC data durability: persisted via `/data/ipc` mount.
- Queue/crawl/stats files survive container restarts if host paths persist.

## 5.8 Deployment Checklist

1. Ensure `/data/postgres` and `/data/ipc` exist with writable permissions.
2. Initialize DB schema (all non-sharded + 256 shard table families).
3. Verify YAML DSN host resolves from containers.
4. Confirm shard parameters match schema generation.
5. Start compose stack and verify all supervisord child processes are healthy.
6. Monitor `/data/ipc/stats/bad` for aggregation failures.
