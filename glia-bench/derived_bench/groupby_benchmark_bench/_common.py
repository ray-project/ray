"""Shared utilities for groupby_benchmark-derived local benchmarks.

The upstream test (release/nightly_tests/dataset/groupby_benchmark.py) reads
a TPC-H lineitem Parquet directory from S3 and runs
`Dataset.groupby(cols).mean(...)` (aggregate path) or
`Dataset.groupby(cols).map_groups(...)` (map_groups path) under either the
sort-shuffle-pull-based or hash-shuffle strategy.  These benchmarks engage
the same operating regimes locally without S3.

Each benchmark imports from this module so the data generator, signal
capture, and rep harness live in one place.
"""

from __future__ import annotations

import gc
import json
import os
import resource
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence

import numpy as np
import psutil
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyarrow import types

import ray
from ray._private.internal_api import (
    get_memory_info_reply,
    get_state_from_address,
)


# ----------------------------------------------------------------------------
# Deterministic synthetic TPC-H lineitem Parquet generator.
#
# We mirror the schema and per-column entropy of the columns the upstream
# benchmark reads (column02 = high-cardinality, column05 = float aggregated,
# column08/13/14 = three string columns whose Cartesian product is 84
# distinct combinations to match the upstream "84 groups" cell).
#
# Per-file seed is deterministic, so cross-rep, cross-process layout is
# identical.  A previously-generated dataset is reused if its row count and
# file count match.
# ----------------------------------------------------------------------------

_DATA_ROOT = Path(os.environ.get("GROUPBY_BENCH_DATA_ROOT", "/tmp/groupby_bench_data"))


def _build_table(n_rows: int, seed: int) -> pa.Table:
    rng = np.random.default_rng(seed)
    col00 = np.arange(n_rows, dtype=np.int64)
    col01 = rng.integers(0, 200_000, size=n_rows, dtype=np.int64)
    high_card = min(n_rows, 2_000_000)
    col02 = rng.integers(0, high_card, size=n_rows, dtype=np.int64)
    col03 = rng.integers(1, 8, size=n_rows, dtype=np.int32)
    col04 = rng.uniform(1.0, 50.0, size=n_rows).astype(np.float64)
    col05 = rng.uniform(1000.0, 100_000.0, size=n_rows).astype(np.float64)
    col06 = rng.uniform(0.0, 0.10, size=n_rows).astype(np.float64)
    col07 = rng.uniform(0.0, 0.08, size=n_rows).astype(np.float64)

    flags = np.array(["A", "N", "R"])
    col08 = flags[rng.integers(0, 3, size=n_rows)]
    col09 = np.where(col08 == "N", "O", "F")

    col10 = np.full(n_rows, "1992-01-01", dtype="<U10")
    col11 = col10
    col12 = col10

    instructs = np.array(["DELIVER IN PERSON", "COLLECT COD", "TAKE BACK RETURN", "NONE"])
    col13 = instructs[rng.integers(0, 4, size=n_rows)]

    modes = np.array(["AIR", "TRUCK", "SHIP", "RAIL", "MAIL", "FOB", "REG AIR"])
    col14 = modes[rng.integers(0, 7, size=n_rows)]

    col15 = np.full(n_rows, "lineitem-cmnt-xx", dtype="<U16")

    return pa.table(
        {
            "column00": col00,
            "column01": col01,
            "column02": col02,
            "column03": col03,
            "column04": col04,
            "column05": col05,
            "column06": col06,
            "column07": col07,
            "column08": pa.array(col08),
            "column09": pa.array(col09),
            "column10": pa.array(col10),
            "column11": pa.array(col11),
            "column12": pa.array(col12),
            "column13": pa.array(col13),
            "column14": pa.array(col14),
            "column15": pa.array(col15),
        }
    )


def ensure_dataset(rows: int, files: int, seed: int = 20260427) -> Path:
    """Generate (idempotently) a synthetic lineitem Parquet directory and
    return its path.  Reuses an existing directory when the row/file counts
    match.
    """
    name = f"sf_{rows}_{files}_{seed}"
    out = _DATA_ROOT / name
    sentinel = out / ".manifest.json"

    if sentinel.exists():
        try:
            manifest = json.loads(sentinel.read_text())
            if (
                manifest.get("rows") == rows
                and manifest.get("files") == files
                and manifest.get("seed") == seed
            ):
                return out
        except Exception:
            pass

    out.mkdir(parents=True, exist_ok=True)
    rows_per_file = rows // files
    rem = rows - rows_per_file * files
    print(f"[gen] creating dataset rows={rows} files={files} -> {out}")
    sys.stdout.flush()

    for i in range(files):
        n = rows_per_file + (1 if i < rem else 0)
        tbl = _build_table(n, seed + i)
        pq.write_table(tbl, out / f"part-{i:04d}.snappy.parquet", compression="snappy")

    sentinel.write_text(json.dumps({"rows": rows, "files": files, "seed": seed}))
    return out


# ----------------------------------------------------------------------------
# Consume functions matching the upstream benchmark.
# ----------------------------------------------------------------------------

def normalize_table(table: pa.Table) -> pa.Table:
    """The map_groups UDF used by the upstream benchmark."""
    cols = []
    for name in table.column_names:
        c = table[name]
        if not types.is_floating(c.type):
            cols.append(c)
            continue
        cols.append(pc.divide(pc.subtract(c, pc.mean(c)), pc.stddev(c)))
    return pa.Table.from_arrays(cols, schema=table.schema)


def consume_aggregate(grouped) -> None:
    grouped.mean("column05").materialize()


def consume_map_groups(grouped) -> None:
    ds = grouped.map_groups(normalize_table, batch_format="pyarrow")
    for _ in ds.iter_internal_ref_bundles():
        pass


# ----------------------------------------------------------------------------
# Signal capture helpers
# ----------------------------------------------------------------------------

def get_spilled_bytes_total() -> int:
    """Cluster-wide spilled bytes from the GCS memory summary. Returns -1
    on any error so callers can fall through cleanly."""
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
# Signature-forwarding patches.
#
# These are opt-in: callers pass `patch_sample=True` to install_patches() to
# get per-rep timing of `SortTaskSpec.sample_boundaries` (R5).  The patch
# uses *args, **kwargs so it survives upstream signature changes.
# ----------------------------------------------------------------------------

@dataclass
class SamplePatchState:
    calls: int = 0
    total_seconds: float = 0.0


def install_sample_boundaries_patch():
    """Time `SortTaskSpec.sample_boundaries` (driver-side; dispatches the
    sample remote tasks and blocks waiting for them).

    Structural assumption: `SortTaskSpec` exposes a static method
    `sample_boundaries`.
    """
    from ray.data._internal.planner.exchange import sort_task_spec as _sts

    state = SamplePatchState()
    orig = _sts.SortTaskSpec.sample_boundaries

    def wrapped(*args, **kwargs):
        t0 = time.perf_counter()
        try:
            return orig(*args, **kwargs)
        finally:
            state.calls += 1
            state.total_seconds += time.perf_counter() - t0

    _sts.SortTaskSpec.sample_boundaries = staticmethod(wrapped)

    def restore():
        _sts.SortTaskSpec.sample_boundaries = staticmethod(orig)

    return state, restore


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
    fn: Callable[[], Dict[str, Any]],
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
            extra=extra,
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
    spills = [r.spilled_bytes for r in measured if r.spilled_bytes >= 0]
    return {
        "n_reps": len(measured),
        "wall_seconds_mean": statistics.fmean(times) if times else float("nan"),
        "wall_seconds_stdev": statistics.pstdev(times) if len(times) > 1 else 0.0,
        "wall_seconds_min": min(times) if times else float("nan"),
        "wall_seconds_max": max(times) if times else float("nan"),
        "spilled_gb_mean": statistics.fmean(spills) / 1e9 if spills else 0.0,
        "spilled_gb_max": max(spills) / 1e9 if spills else 0.0,
        "driver_maxrss_gb_max": (
            max(r.driver_maxrss for r in measured) / 1e9 if measured else 0.0
        ),
        "extras": [r.extra for r in measured],
    }


def print_summary(name: str, summary: Dict[str, Any]) -> None:
    print(f"\n=== {name} summary ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    sys.stdout.flush()


# ----------------------------------------------------------------------------
# Bench entry helper.  Each run.py uses build_and_run() to share the boring
# parts (init Ray, ensure data, set strategy, run reps, print summary).
# ----------------------------------------------------------------------------

def build_and_run(
    *,
    name: str,
    rows: int,
    files: int,
    consume: str,
    shuffle_strategy: str,
    group_by: Sequence[str],
    override_num_blocks: int,
    object_store_memory_bytes: int | None = None,
    max_hash_aggregators: int | None = None,
    hash_shuffle_parallelism: int | None = None,
    n_reps: int = 3,
    warmup: int = 1,
    num_cpus: int = 24,
    patch_sample: bool = False,
) -> Dict[str, Any]:
    """Init Ray, ensure data, run the configured workload n_reps times."""

    if max_hash_aggregators is not None:
        os.environ["RAY_DATA_MAX_HASH_SHUFFLE_AGGREGATORS"] = str(max_hash_aggregators)

    data_dir = ensure_dataset(rows, files)

    init_kwargs: Dict[str, Any] = {
        "num_cpus": num_cpus,
        "include_dashboard": False,
        "ignore_reinit_error": True,
        "log_to_driver": False,
    }
    if object_store_memory_bytes is not None:
        # /dev/shm on this devpod is 8 GB; opt into /tmp tmpfs when caller
        # sets a cap larger than that, or any time at all (Ray will only
        # actually use /tmp when the request can't fit /dev/shm).
        os.environ.setdefault("RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE", "1")
        init_kwargs["object_store_memory"] = object_store_memory_bytes

    ray.init(**init_kwargs)
    try:
        from ray.data import DataContext
        from ray.data.context import ShuffleStrategy

        ctx = DataContext.get_current()
        ctx.shuffle_strategy = ShuffleStrategy(shuffle_strategy)
        if hash_shuffle_parallelism is not None:
            ctx.default_hash_shuffle_parallelism = hash_shuffle_parallelism

        consume_fn = {
            "aggregate": consume_aggregate,
            "map_groups": consume_map_groups,
        }[consume]

        sample_state = None
        sample_restore = None
        if patch_sample:
            sample_state, sample_restore = install_sample_boundaries_patch()

        def fn() -> Dict[str, Any]:
            grouped = (
                ray.data.read_parquet(
                    str(data_dir), override_num_blocks=override_num_blocks
                )
                .groupby(list(group_by))
            )
            consume_fn(grouped)
            extra: Dict[str, Any] = {}
            if sample_state is not None:
                extra["sample_calls"] = sample_state.calls
                extra["sample_total_s"] = round(sample_state.total_seconds, 3)
                # Reset between reps so we measure per-rep cost.
                sample_state.calls = 0
                sample_state.total_seconds = 0.0
            return extra

        try:
            reps = run_reps(name, fn, n_reps=n_reps, warmup=warmup)
        finally:
            if sample_restore is not None:
                sample_restore()

        summary = summarize(reps, skip_warmup=warmup)
        print_summary(name, summary)
        return summary
    finally:
        ray.shutdown()
