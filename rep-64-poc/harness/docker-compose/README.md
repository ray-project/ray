# Phase 1 baseline harness (Redis, Docker Compose)

One-command Redis baseline microbenchmark. Establishes the Redis-side numbers we'll later compare against RocksDB.

## What it measures

Single-threaded Redis-protocol latency for the operations Ray's `RedisStoreClient` issues:

| Op | Redis primitive | StoreClient counterpart |
|---|---|---|
| `put` | `HSET ns@table field value` | `AsyncPut` |
| `get_hit` / `get_miss` | `HGET ns@table field` | `AsyncGet` |
| `exists_hit` / `exists_miss` | `HEXISTS ns@table field` | `AsyncExists` |
| `multi_get_10` | `HMGET ns@table f1 ... f10` | `AsyncMultiGet` |
| `prefix_scan` | `HSCAN ns@table MATCH prefix*` | `AsyncGetKeys` |
| `get_all` | `HGETALL ns@table` | `AsyncGetAll` |
| `delete` | `HDEL ns@table field` | `AsyncDelete` |
| `batch_delete_10` | `HDEL ns@table f1 ... f10` | `AsyncBatchDelete` |

`AsyncGetNextJobID` (Redis `INCR`) is not in the workload — it's exercised in Phase 5's concurrency tests.

## Why the Redis container is configured the way it is

| Setting | Value | Reason |
|---|---|---|
| `appendonly yes` + `appendfsync always` | enabled | Forces fsync per write so the comparison against RocksDB's `WriteOptions::sync = true` is fair. Default Redis batches up to 1 s of writes, which would inflate Redis numbers ~10x. |
| `save ""` | RDB snapshots disabled | We're measuring per-op latency, not background snapshot cost. |
| `maxmemory-policy noeviction` | enabled | Bench errors on OOM rather than silently evicting and producing misleading results. |

## Running

```bash
./run.sh                       # writes ./baseline-redis.json
./run.sh /tmp/baseline.json    # custom output path
```

Prereqs: Docker (24+) with `docker compose` (v2) or legacy `docker-compose`. Python 3.9+. The runner creates a venv inside this directory; first invocation downloads `redis-py`.

## Result schema (v1)

```jsonc
{
  "harness": {
    "name": "rep-64-poc",
    "schema_version": 1,
    "phase": 1,
    "backend": "redis",                // or "rocksdb" / "in_memory" later
    "started_at": "2026-04-29T23:50:00+00:00",
    "duration_seconds": 12.3,
    "params": {                        // varies between runs
      "n_warmup": 1000,
      "n_measure": 10000,
      "value_size_bytes": 256,
      "table_count": 10,
      "concurrency": 1
    },
    "environment": {                   // captured at run time
      "host_kernel": "5.15.x",
      "redis_version": "7.4.x",
      "redis_image_tag": "redis:7.4-alpine@sha256:...",
      "container_runtime": "docker"
      // ... + arch, OS, Python version
    }
  },
  "operations": {
    "put":         {"n":10000, "errors":0,
                    "p50_us":0.0, "p99_us":0.0, "p999_us":0.0,
                    "mean_us":0.0, "ops_per_sec":0.0},
    "get_hit":     {"n":...},
    // ... one entry per op above
  }
}
```

Schema is shared across backends and phases. Phase 7's RocksDB run emits the same shape so `harness/compare.py` can diff them mechanically.

## Reproducibility pinning

`docker-compose.yml` currently uses `redis:7.4-alpine` (a moving tag). After the first successful run we should pin to the immutable digest the run actually used (the runner records it in `harness.environment.redis_image_tag` of the result JSON). To pin:

```yaml
# docker-compose.yml
image: redis:7.4-alpine@sha256:<digest from a result file>
```

Ditto for cloud / kind harnesses — addresses risk **R10** in `RISKS.md`.

## What this does *not* measure

- **Ray's RedisStoreClient overhead.** This bench drives Redis directly. Ray's client adds a per-key request queue, callback dispatch, and namespace prefix logic — all measured in Phase 7 via the C++ microbench against the real `RedisStoreClient`. Treat these Phase 1 numbers as the protocol-level ceiling.
- **Network latency to a remote Redis.** The container is on `127.0.0.1`; production Redis is typically a separate pod or external service. Add ~0.5–2 ms RTT for that case.
- **Concurrency.** GCS is single-threaded, so single-thread is the right baseline; concurrency stress is in Phase 5.
- **Recovery time.** Phase 8's job — needs Ray cluster setup, not just protocol ops.
