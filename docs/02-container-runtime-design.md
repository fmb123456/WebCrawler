# 02. Container Runtime Design

## 2.1 `postgres` Container

Defined in `docker-compose.yml`:

- Image: `postgres:16`
- DB: `crawlerdb`
- User/password: `crawler/crawler`
- Tunings:
  - `max_connections=500`
  - `max_wal_size=20GB`
  - `checkpoint_timeout=15min`
- Persistent volume: `/data/postgres:/var/lib/postgresql/data`
- Exposed port: `5432`

Primary role:

- Stores all domain, URL state/history, event counters, content features, and aggregated daily stats.

## 2.2 `scheduler_control` Container

Runs `supervisord` with two program types:

- `offerer` with `numprocs=16` (`offerer_00..offerer_15`)
- `accounting_rolloff` with `numprocs=1`

Runtime behavior per offerer:

1. Check queue depth in `/data/ipc/url_queue/crawler_{id:02d}`.
2. If below `low_watermark_batches`, query its 16 assigned shards in DB.
3. Select URLs where `should_crawl=TRUE` using strategy ordering:
   - Phase A: score-aware (`url_score`, `domain_score`, `last_scheduled`, `first_seen`)
   - Phase B: fairness (`last_scheduled`, `first_seen`)
4. Atomically update selected rows:
   - `should_crawl=FALSE`
   - `last_scheduled=NOW()`
   - `num_scheduled_90d += 1`
5. Upsert daily per-URL schedule event in `url_event_counter_{shard}`.
6. Emit queue files containing URL batches (`batch_size=512`).
7. Write stats delta with `num_scheduled` and per-domain schedule counts.

Runtime behavior of `accounting_rolloff`:

1. Wake up by configured polling interval and check daily UTC schedule.
2. For each shard (`0..255`), read `url_event_counter_{shard}` rows where:
   - `accounted=TRUE`
   - `event_date <= CURRENT_DATE - event_retention_days` (default 90)
3. Process in batches (`batch_size`, configurable) with `FOR UPDATE SKIP LOCKED`:
   - aggregate picked event rows by URL within the batch,
   - subtract aggregated values from `url_state_current_{shard}` 90-day counters (floor at 0),
   - append snapshots into `url_state_history_{shard}`,
   - set processed (and missing-current-row) event rows to `accounted=FALSE`.
4. Commit each batch independently to reduce lock duration and avoid long transactions.

## 2.3 `crawler` Container

Runs `supervisord` with:

- `scrapy crawl html_spider -a crawler_id=%(process_num)d`
- `numprocs=16`

Crawler worker behavior:

1. Pop earliest queue batch JSON from `/data/ipc/url_queue/crawler_{id:02d}`.
2. Delete queue file immediately after read.
3. Crawl URLs with Scrapy (robots obeyed, retries enabled, custom per-domain download slots).
4. For each response:
   - if HTML/XHTML: save full content + extracted outlinks.
   - otherwise: emit failure record (`NonHTML content-type`).
5. For request errors: emit failure record with normalized reason (`HttpError`, `IgnoreRequest`, etc.).
6. Write records as JSONL into `/data/ipc/crawl_result/crawler_{id:02d}/{YYYYMMDD}/{HHMM}/HHMM.jsonl`.

## 2.4 `scheduler_ingest` Container

Runs 4 program families via `supervisord`:

- `router` x16
- `ingestor` x16
- `feature_extractor` x16
- `stats_aggregator` x1

### Router (x16)

Input: crawler result folders `crawler_{id}`.

Responsibilities:

- Read crawler JSON/JSONL records from ready time buckets.
- Resolve source URL domain into `domain_state` (insert if missing).
- Compute shard/ingestor destination.
- For fetch-success records, compute `content_hash` (SHA-1 of content).
- For each outlink, ensure destination domain exists and emit separate `status="new"` records.
- Write transformed records to `ingestor_{id}` bucket JSONL files.

### Ingestor (x16)

Input: router output folders `ingestor_{id}`.

Responsibilities:

- `status="new"` records: insert new URL candidates into `url_state_current_{shard}` and history.
- Fetch result records:
  - upsert `url_state_current_{shard}` counters and status fields,
  - append snapshots to `url_state_history_{shard}`,
  - upsert `url_event_counter_{shard}` daily fetch/update events.
- Emit stats deltas (`new_links`, fetch OK/fail, content updates, fail reasons, ingest errors).

### Feature Extractor (x16)

Input: same `ingestor_{id}` folders.

Responsibilities:

- For `status="ok"` records, derive basic features:
  - content length
  - content hash
  - outlink count
- Upsert latest features into `content_feature_current_{shard}`.
- Append snapshots into `content_feature_history_{shard}`.

### Stats Aggregator (x1)

Input: `/data/ipc/stats/*.json` deltas.

Responsibilities:

- Apply deltas to:
  - `summary_daily` (global counters)
  - `domain_stats_daily` (per-domain counters)
- Aggregate `fail_reasons` JSONB maps via `jsonb_set` increments.
- Move malformed/unprocessable files into `/data/ipc/stats/bad`.
- Emit `stats_error` delta if aggregation fails.
