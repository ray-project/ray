"""Shared utilities for streaming_split_benchmark-derived benchmarks.

Provides:
- Deterministic synthetic parquet data generation (idempotent across reps).
- A `ConsumingActor` that mirrors the upstream test's actor and also
  captures the iterator's `stats()` text so the driver can parse R1
  (coordinator overhead) and R2 (output-splitter overhead).
- Stats parsers for the streaming-split signals exposed by Ray Data
  2.55's stats text:
    * `Streaming split coordinator overhead time: <X>s`           (R1)
    * `'output_splitter_overhead_time': <Y>` in extra_metrics      (R2)
  These signals are built into the codebase under measurement; we don't
  monkey-patch anything. The patches we considered are documented in
  the calibration notes — both SplitCoordinator and OutputSplitter
  live inside the coordinator actor's Python process, which a
  driver-side class patch can't reach.
- A driver harness `run_streaming_split_once` that spawns N consumer
  actors, calls `streaming_split`, and aggregates per-rep signals.
- `run_reps` + `summarize` + `print_summary` (warmup + N measured reps).
"""

from __future__ import annotations

import gc
import os
import re
import resource
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import psutil

import ray
from ray._private.internal_api import (
    get_memory_info_reply,
    get_state_from_address,
)


# ----------------------------------------------------------------------------
# Synthetic data generation. Idempotent: skips files that already exist.
# ----------------------------------------------------------------------------

def make_parquet_dataset(
    out_dir: str,
    *,
    num_files: int,
    rows_per_file: int,
    payload_bytes: int = 4096,
    seed: int = 0,
) -> str:
    """Generate `num_files` parquet files mimicking the ImageNet-like layout
    used by the upstream test: a `label` int column and an `image` binary
    payload column.

    Each file's content depends only on (seed, file_idx), so identical
    invocations across reps reuse the same files. SNAPPY compression on
    high-entropy bytes ≈ 1× ratio so the on-disk size approximates raw.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    os.makedirs(out_dir, exist_ok=True)
    schema = pa.schema([
        pa.field("label", pa.int64()),
        pa.field("image", pa.binary(payload_bytes)),
    ])
    for i in range(num_files):
        path = os.path.join(out_dir, f"part-{i:05d}.parquet")
        if os.path.exists(path):
            continue
        rng = np.random.default_rng(seed * 10_007 + i)
        labels = rng.integers(0, 1000, size=rows_per_file, dtype=np.int64)
        blob = rng.bytes(rows_per_file * payload_bytes)
        payload = [blob[j * payload_bytes:(j + 1) * payload_bytes]
                   for j in range(rows_per_file)]
        table = pa.Table.from_arrays(
            [pa.array(labels), pa.array(payload, type=pa.binary(payload_bytes))],
            schema=schema,
        )
        pq.write_table(table, path, compression="snappy")
    return out_dir


# ----------------------------------------------------------------------------
# Signal capture helpers
# ----------------------------------------------------------------------------

def get_spilled_bytes_total() -> int:
    try:
        memory_info = get_memory_info_reply(
            get_state_from_address(ray.get_runtime_context().gcs_address)
        )
        return memory_info.store_stats.spilled_bytes_total
    except Exception:
        return -1


def driver_rss_bytes() -> int:
    return int(psutil.Process(os.getpid()).memory_info().rss)


def driver_maxrss_bytes() -> int:
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024)


# ----------------------------------------------------------------------------
# Stats text parsers (built-in metrics, no monkey-patching).
# ----------------------------------------------------------------------------

def parse_streaming_split_coord_seconds(stats: str) -> float:
    """Parse `Streaming split coordinator overhead time: <X>s` from a
    `StreamSplitDataIterator.stats()` text.

    This is the cumulative wall time the SplitCoordinator's `get()`
    method spent for ALL N consumers combined. With N concurrent
    consumers, this can exceed real wall time — it's per-call summed.

    Falls through several time-unit formats: s, ms, us, µs.
    Returns 0.0 if not found.
    """
    m = re.search(
        r"Streaming split coordinator overhead time:\s*([0-9.]+)\s*([smµun]+)",
        stats,
    )
    if not m:
        return 0.0
    val = float(m.group(1))
    unit = m.group(2)
    if unit.startswith("ms"):
        return val / 1000
    if unit.startswith("us") or unit.startswith("µs"):
        return val / 1_000_000
    return val


def parse_output_splitter_overhead_seconds(stats: str) -> float:
    """Parse `output_splitter_overhead_time` from the verbose extra_metrics
    block in an iterator stats() text.

    Only present when DataContext.verbose_stats_logs=True (set in the
    consumer actor before iter_batches). Returns 0.0 if missing.
    """
    m = re.search(r"'output_splitter_overhead_time':\s*([0-9.eE+-]+)", stats)
    return float(m.group(1)) if m else 0.0


# ----------------------------------------------------------------------------
# Consumer actor + driver glue.
# ----------------------------------------------------------------------------

@ray.remote
class ConsumingActor:
    """Mirrors the upstream test's ConsumingActor.

    Differences from upstream:
    - Default num_cpus=1 (set via .options()) — on a single-node devpod
      we can't SPREAD across nodes, so we enforce parallel execution
      via explicit CPU resources.
    - `consume` returns the row count, the iterator's stats() text,
      and start/end timestamps for cross-actor skew measurement.
    """

    def consume(
        self,
        split,
        max_rows_to_read: Optional[int] = None,
    ) -> Dict[str, Any]:
        # Enable verbose stats so OutputSplitter's _extra_metrics
        # surface in the iterator's stats text (R2 signal).
        try:
            from ray.data.context import DataContext
            DataContext.get_current().verbose_stats_logs = True
        except Exception:
            pass

        rows_read = 0
        t_start = time.perf_counter()
        for batch in split.iter_batches():
            rows_read += len(batch["label"])
            if max_rows_to_read is not None and rows_read >= max_rows_to_read:
                break
        t_end = time.perf_counter()

        try:
            stats_text = split.stats()
        except Exception as exc:
            stats_text = f"<stats() raised: {exc!r}>"

        return {
            "rows_read": rows_read,
            "consume_seconds": t_end - t_start,
            "stats_text": stats_text,
            "t_start": t_start,
            "t_end": t_end,
        }

    def get_location(self) -> str:
        return ray.get_runtime_context().get_node_id()


def run_streaming_split_once(
    ds: "ray.data.Dataset",
    *,
    num_workers: int,
    equal: bool,
    early_stop: bool,
    consumer_options: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """One full streaming_split run: spawn N consumer actors, split,
    consume, return per-rep stats.

    All N consumers see the same global coordinator overhead (it's a
    single global counter); we parse the stats text from one of them.
    """
    consumer_options = consumer_options or {"num_cpus": 1}
    consumers = [
        ConsumingActor.options(**consumer_options).remote()
        for _ in range(num_workers)
    ]
    locality_hints = ray.get(
        [actor.get_location.remote() for actor in consumers]
    )

    num_rows = ds.count()
    if early_stop:
        max_rows_per_worker = num_rows // 2 // num_workers
    else:
        max_rows_per_worker = None

    splits = ds.streaming_split(
        num_workers, equal=equal, locality_hints=locality_hints
    )
    t0 = time.perf_counter()
    futures = [
        consumers[i].consume.remote(split, max_rows_per_worker)
        for i, split in enumerate(splits)
    ]
    results = ray.get(futures)
    consume_wall = time.perf_counter() - t0

    total_rows = sum(r["rows_read"] for r in results)
    consume_seconds = [r["consume_seconds"] for r in results]
    end_ts = [r["t_end"] for r in results]
    actor_skew = max(end_ts) - min(end_ts)

    one_stats = results[0]["stats_text"]
    coord_overhead = parse_streaming_split_coord_seconds(one_stats)
    splitter_overhead = parse_output_splitter_overhead_seconds(one_stats)

    for c in consumers:
        try:
            ray.kill(c)
        except Exception:
            pass

    return {
        "consume_wall_s": round(consume_wall, 3),
        "consume_max_s": round(max(consume_seconds), 3),
        "consume_min_s": round(min(consume_seconds), 3),
        "actor_skew_s": round(actor_skew, 3),
        "coord_overhead_s": round(coord_overhead, 3),
        "splitter_overhead_s": round(splitter_overhead, 4),
        "num_workers": num_workers,
        "equal": int(equal),
        "early_stop": int(early_stop),
        "num_rows_total": num_rows,
        "rows_consumed": total_rows,
    }


# ----------------------------------------------------------------------------
# Rep harness
# ----------------------------------------------------------------------------

@dataclass
class RepResult:
    name: str
    seconds: float
    spilled_bytes: int
    driver_rss_before: int
    driver_rss_after: int
    driver_maxrss: int
    extra: Dict[str, Any] = field(default_factory=dict)


def run_reps(
    name: str,
    fn: Callable[[], Optional[Dict[str, Any]]],
    n_reps: int = 3,
    warmup: int = 1,
) -> List[RepResult]:
    results: List[RepResult] = []
    total = warmup + n_reps
    for i in range(total):
        gc.collect()
        rss_before = driver_rss_bytes()
        spilled_before = get_spilled_bytes_total()
        t0 = time.perf_counter()
        extra = fn() or {}
        dt = time.perf_counter() - t0
        spilled_after = get_spilled_bytes_total()
        rss_after = driver_rss_bytes()
        max_rss = driver_maxrss_bytes()
        spilled = (
            max(0, spilled_after - spilled_before)
            if spilled_before >= 0 and spilled_after >= 0
            else -1
        )
        rep = RepResult(
            name=f"{name}#{i}{'_warm' if i < warmup else ''}",
            seconds=dt,
            spilled_bytes=spilled,
            driver_rss_before=rss_before,
            driver_rss_after=rss_after,
            driver_maxrss=max_rss,
            extra=dict(extra),
        )
        results.append(rep)
        tag = "warmup" if i < warmup else f"rep {i - warmup + 1}/{n_reps}"
        print(
            f"[{name}] {tag}: {dt:.2f}s  "
            f"spilled={spilled / 1e9:.3f}GB  "
            f"rss_delta={(rss_after - rss_before) / 1e6:.1f}MB  "
            f"maxrss={max_rss / 1e9:.2f}GB  "
            f"extra={extra}"
        )
        sys.stdout.flush()
    return results


def summarize(reps: List[RepResult], skip_warmup: int = 1) -> Dict[str, Any]:
    measured = reps[skip_warmup:]
    times = [r.seconds for r in measured]
    extras = [r.extra for r in measured]

    extra_keys: set = set()
    for e in extras:
        extra_keys.update(k for k, v in e.items() if isinstance(v, (int, float)))
    extra_means: Dict[str, Any] = {}
    for k in extra_keys:
        vals = [e[k] for e in extras if k in e and isinstance(e[k], (int, float))]
        if vals:
            extra_means[f"{k}_mean"] = round(statistics.fmean(vals), 4)

    return {
        "n_reps": len(measured),
        "wall_seconds_mean": statistics.fmean(times) if times else float("nan"),
        "wall_seconds_stdev": statistics.pstdev(times) if len(times) > 1 else 0.0,
        "wall_seconds_min": min(times) if times else float("nan"),
        "wall_seconds_max": max(times) if times else float("nan"),
        "wall_seconds_p50": (
            sorted(times)[len(times) // 2] if times else float("nan")
        ),
        "spilled_gb_mean": (
            statistics.fmean([r.spilled_bytes for r in measured]) / 1e9
            if measured
            else 0.0
        ),
        "driver_maxrss_gb_max": (
            max(r.driver_maxrss for r in measured) / 1e9 if measured else 0.0
        ),
        "rss_delta_mb_mean": (
            statistics.fmean(
                [(r.driver_rss_after - r.driver_rss_before) / 1e6 for r in measured]
            )
            if measured
            else 0.0
        ),
        **extra_means,
        "extras": extras,
    }


def print_summary(name: str, summary: Dict[str, Any]) -> None:
    print(f"\n=== {name} summary ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    sys.stdout.flush()
