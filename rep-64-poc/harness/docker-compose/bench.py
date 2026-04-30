"""Phase 1 baseline microbenchmark.

Exercises the Redis-protocol-level operations Ray's RedisStoreClient
performs (HSET / HGET / HGETALL / HMGET / HDEL / HSCAN / HEXISTS) so we
have a reference point to compare RocksDB against in later phases.

Output: a single JSON document on stdout (and optionally a file) with the
schema documented in README.md ("Result schema").

This is *not* a comparison against Ray-via-RedisStoreClient itself —
that's Phase 7's job. This is the protocol-level Redis baseline: what a
single client process can drive Redis to do under fsync-on-write
semantics. Treat it as a ceiling for what the Ray code path could
achieve, not as an apples-to-apples Ray comparison.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

import redis

# Mirrors RedisStoreClient's key layout: outer hash = "{ns}@{table}",
# field = "{key}". See src/ray/gcs/store_client/redis_store_client.cc.
NAMESPACE = "rep64poc"
TABLES = ["ACTOR", "NODE", "JOB", "PLACEMENT_GROUP", "WORKER",
          "PG_SCHEDULE", "ACTOR_TASK_SPEC", "RESOURCE_USAGE_BATCH",
          "WORKERS", "INTERNAL_CONFIG"]


def hash_key(table: str) -> str:
    return f"{NAMESPACE}@{table}"


@dataclass
class OpResult:
    n: int
    errors: int
    p50_us: float
    p99_us: float
    p999_us: float
    mean_us: float
    ops_per_sec: float


@dataclass
class HarnessMeta:
    name: str = "rep-64-poc"
    schema_version: int = 1
    phase: int = 1
    backend: str = "redis"
    started_at: str = ""
    duration_seconds: float = 0.0
    params: dict = field(default_factory=dict)
    environment: dict = field(default_factory=dict)


def percentiles(samples_ns: list[int]) -> tuple[float, float, float, float]:
    """Return (p50, p99, p999, mean) in microseconds."""
    if not samples_ns:
        return 0.0, 0.0, 0.0, 0.0
    s = sorted(samples_ns)
    n = len(s)
    p50 = s[int(0.50 * (n - 1))] / 1000.0
    p99 = s[int(0.99 * (n - 1))] / 1000.0
    p999 = s[int(0.999 * (n - 1))] / 1000.0
    mean = (sum(s) / n) / 1000.0
    return p50, p99, p999, mean


def time_one(fn: Callable[[], object]) -> tuple[int, bool]:
    """Run fn, return (elapsed_ns, ok)."""
    t0 = time.perf_counter_ns()
    try:
        fn()
        return time.perf_counter_ns() - t0, True
    except Exception:  # noqa: BLE001 — we record errors, don't suppress
        return time.perf_counter_ns() - t0, False


def measure(fn: Callable[[int], object], n: int) -> OpResult:
    samples_ns: list[int] = []
    errors = 0
    t_start = time.perf_counter()
    for i in range(n):
        elapsed, ok = time_one(lambda i=i: fn(i))
        samples_ns.append(elapsed)
        if not ok:
            errors += 1
    wall = time.perf_counter() - t_start
    p50, p99, p999, mean = percentiles(samples_ns)
    return OpResult(
        n=n,
        errors=errors,
        p50_us=round(p50, 3),
        p99_us=round(p99, 3),
        p999_us=round(p999, 3),
        mean_us=round(mean, 3),
        ops_per_sec=round(n / wall, 1) if wall > 0 else 0.0,
    )


def collect_environment(client: redis.Redis) -> dict:
    info = client.info(section="server")
    return {
        "host_kernel": platform.uname().release,
        "host_machine": platform.machine(),
        "host_python": platform.python_version(),
        "redis_version": info.get("redis_version"),
        "redis_mode": info.get("redis_mode"),
        "redis_os": info.get("os"),
        "redis_arch_bits": info.get("arch_bits"),
        "redis_image_tag": os.environ.get("REDIS_IMAGE_TAG", "unknown"),
        "container_runtime": os.environ.get("HARNESS_RUNTIME", "unknown"),
    }


def run_workload(client: redis.Redis, n_warmup: int, n_measure: int,
                 value_size: int) -> dict[str, OpResult]:
    """Run all measured operations sequentially, single-threaded.

    GCS calls into StoreClient from a single event loop, so single-thread
    is the relevant baseline.
    """
    results: dict[str, OpResult] = {}
    table = TABLES[0]
    hk = hash_key(table)
    value = b"x" * value_size

    # Clean slate.
    for t in TABLES:
        client.delete(hash_key(t))

    # Pre-populate keys 0..(n_warmup + n_measure) so reads always hit.
    total_keys = n_warmup + n_measure
    pipe = client.pipeline()
    for i in range(total_keys):
        pipe.hset(hk, f"k{i}", value)
    pipe.execute()

    # PUT: overwrite existing keys.
    for i in range(n_warmup):
        client.hset(hk, f"k{i}", value)
    results["put"] = measure(
        lambda i: client.hset(hk, f"k{i}", value), n_measure)

    # GET hit.
    for i in range(n_warmup):
        client.hget(hk, f"k{i}")
    results["get_hit"] = measure(
        lambda i: client.hget(hk, f"k{i}"), n_measure)

    # GET miss.
    results["get_miss"] = measure(
        lambda i: client.hget(hk, f"absent_{i}"), n_measure)

    # EXISTS hit / miss.
    results["exists_hit"] = measure(
        lambda i: client.hexists(hk, f"k{i}"), n_measure)
    results["exists_miss"] = measure(
        lambda i: client.hexists(hk, f"absent_{i}"), n_measure)

    # MULTI_GET (10 keys per call).
    def mget10(i: int) -> object:
        keys = [f"k{(i + j) % total_keys}" for j in range(10)]
        return client.hmget(hk, keys)

    results["multi_get_10"] = measure(mget10, n_measure)

    # PREFIX_SCAN: HSCAN with MATCH.
    def hscan_full(i: int) -> object:
        cursor = 0
        out: list = []
        while True:
            cursor, batch = client.hscan(hk, cursor=cursor, match="k1*",
                                         count=200)
            out.extend(batch.items())
            if cursor == 0:
                break
        return out

    # Prefix scan is expensive — measure fewer iterations.
    results["prefix_scan"] = measure(hscan_full, max(100, n_measure // 100))

    # GET_ALL: HGETALL on the full table (one large operation).
    # Measure fewer iterations since each call returns the full table.
    results["get_all"] = measure(lambda i: client.hgetall(hk),
                                 max(100, n_measure // 100))

    # DELETE individual keys (consume from the populated set).
    delete_n = min(n_measure, total_keys // 2)
    results["delete"] = measure(
        lambda i: client.hdel(hk, f"k{i}"), delete_n)

    # BATCH_DELETE 10 at a time (against the second half of the keys).
    def batch_del10(i: int) -> object:
        base = total_keys // 2 + (i * 10) % (total_keys // 2 - 10)
        keys = [f"k{base + j}" for j in range(10)]
        return client.hdel(hk, *keys)

    results["batch_delete_10"] = measure(
        batch_del10, max(100, n_measure // 100))

    return results


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "localhost"))
    p.add_argument("--port", type=int,
                   default=int(os.environ.get("REDIS_PORT", "6379")))
    p.add_argument("--n-warmup", type=int, default=1000)
    p.add_argument("--n-measure", type=int, default=10000)
    p.add_argument("--value-size", type=int, default=256,
                   help="Value size in bytes (default 256 — typical actor "
                        "metadata).")
    p.add_argument("--output", default="-",
                   help="Path to write JSON; '-' for stdout.")
    args = p.parse_args()

    meta = HarnessMeta()
    meta.started_at = datetime.now(timezone.utc).isoformat()
    meta.params = {
        "n_warmup": args.n_warmup,
        "n_measure": args.n_measure,
        "value_size_bytes": args.value_size,
        "table_count": len(TABLES),
        "concurrency": 1,
    }

    client = redis.Redis(host=args.host, port=args.port,
                         decode_responses=False, socket_timeout=10)
    # Fail fast if Redis is misconfigured for the durability comparison.
    cfg = client.config_get("appendfsync")
    if cfg.get("appendfsync") != "always":
        print(f"REFUSING TO RUN: redis appendfsync={cfg.get('appendfsync')}; "
              "expected 'always' to match RocksDB's sync-write contract. "
              "Use the docker-compose runner or set --requirepass and "
              "configure manually.", file=sys.stderr)
        return 2

    meta.environment = collect_environment(client)
    t0 = time.perf_counter()
    results = run_workload(client, args.n_warmup, args.n_measure,
                           args.value_size)
    meta.duration_seconds = round(time.perf_counter() - t0, 2)

    doc = {
        "harness": asdict(meta),
        "operations": {k: asdict(v) for k, v in results.items()},
    }
    out = json.dumps(doc, indent=2)
    if args.output == "-":
        print(out)
    else:
        with open(args.output, "w") as f:
            f.write(out)
        print(f"wrote {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
