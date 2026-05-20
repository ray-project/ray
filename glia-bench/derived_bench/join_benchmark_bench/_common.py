"""Shared utilities for join_benchmark-derived local benchmarks.

The upstream test (release/nightly_tests/dataset/join_benchmark.py) calls
`Dataset.join(...).count()` on TPC-H lineitem ⋈ orders at SF100 from S3.
These benchmarks engage the same operating regimes locally without S3,
substituting deterministic synthetic TPC-H-shaped Parquet.
"""

from __future__ import annotations

import gc
import json
import os
import re
import resource
import statistics
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import psutil
import pyarrow as pa
import pyarrow.parquet as pq

import ray
from ray._private.internal_api import (
    get_memory_info_reply,
    get_state_from_address,
)


# ----------------------------------------------------------------------------
# Deterministic synthetic TPC-H-shaped lineitem + orders Parquet generator.
#
# The upstream join_benchmark joins lineitem.l_orderkey (~SF*6M rows, name
# "column00") to orders.o_orderkey (~SF*1.5M rows, name "column0") at SF100.
# We mirror the join semantics: left has 4:1 multiplicity to right, both
# keys are int64, and output cardinality differs across join types in the
# way upstream relies on.
# ----------------------------------------------------------------------------

_DATA_ROOT = Path(os.environ.get("JOIN_BENCH_DATA_ROOT", "/tmp/join_bench_data"))


def _build_lineitem_table(n_rows: int, n_orders: int, seed: int) -> pa.Table:
    rng = np.random.default_rng(seed)
    column00 = rng.integers(0, n_orders, size=n_rows, dtype=np.int64)
    column01 = rng.integers(0, 200_000, size=n_rows, dtype=np.int64)
    column02 = rng.integers(0, 200_000, size=n_rows, dtype=np.int64)
    column03 = rng.integers(1, 8, size=n_rows, dtype=np.int32)
    column04 = rng.uniform(1.0, 50.0, size=n_rows).astype(np.float64)
    column05 = rng.uniform(1000.0, 100_000.0, size=n_rows).astype(np.float64)
    column06 = rng.uniform(0.0, 0.10, size=n_rows).astype(np.float64)
    column07 = rng.uniform(0.0, 0.08, size=n_rows).astype(np.float64)
    flags = np.array(["A", "N", "R"])
    column08 = flags[rng.integers(0, 3, size=n_rows)]
    column09 = np.where(column08 == "N", "O", "F")
    column10 = np.full(n_rows, "1992-01-01", dtype="<U10")
    column11 = column10
    column12 = column10
    instructs = np.array(
        ["DELIVER IN PERSON", "COLLECT COD", "TAKE BACK RETURN", "NONE"]
    )
    column13 = instructs[rng.integers(0, 4, size=n_rows)]
    modes = np.array(["AIR", "TRUCK", "SHIP", "RAIL", "MAIL", "FOB", "REG AIR"])
    column14 = modes[rng.integers(0, 7, size=n_rows)]
    column15 = np.full(n_rows, "lineitem-cmnt-xx", dtype="<U16")

    return pa.table(
        {
            "column00": column00,
            "column01": column01,
            "column02": column02,
            "column03": column03,
            "column04": column04,
            "column05": column05,
            "column06": column06,
            "column07": column07,
            "column08": pa.array(column08),
            "column09": pa.array(column09),
            "column10": pa.array(column10),
            "column11": pa.array(column11),
            "column12": pa.array(column12),
            "column13": pa.array(column13),
            "column14": pa.array(column14),
            "column15": pa.array(column15),
        }
    )


def _build_orders_table(n_orders: int, start: int, seed: int) -> pa.Table:
    rng = np.random.default_rng(seed)
    n = n_orders
    column0 = np.arange(start, start + n, dtype=np.int64)
    column1 = rng.integers(0, 1_500_000, size=n, dtype=np.int64)
    column2 = np.array(["O", "F", "P"])[rng.integers(0, 3, size=n)]
    column3 = rng.uniform(1_000.0, 500_000.0, size=n).astype(np.float64)
    column4 = np.full(n, "1992-01-01", dtype="<U10")
    column5 = np.array(["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"])[
        rng.integers(0, 5, size=n)
    ]
    column6 = np.full(n, "Clerk#000000001", dtype="<U16")
    column7 = rng.integers(0, 5, size=n, dtype=np.int32)
    column8 = np.full(n, "orders-comment-xx", dtype="<U18")

    return pa.table(
        {
            "column0": column0,
            "column1": column1,
            "column2": pa.array(column2),
            "column3": column3,
            "column4": pa.array(column4),
            "column5": pa.array(column5),
            "column6": pa.array(column6),
            "column7": column7,
            "column8": pa.array(column8),
        }
    )


def ensure_join_dataset(
    *,
    n_left: int,
    n_files_left: int,
    n_right: int,
    n_files_right: int,
    seed: int = 20260427,
) -> Tuple[Path, Path]:
    """Idempotently generate synthetic lineitem and orders directories."""
    name = f"j_l{n_left}_lf{n_files_left}_r{n_right}_rf{n_files_right}_s{seed}"
    base = _DATA_ROOT / name
    left_dir = base / "lineitem"
    right_dir = base / "orders"
    sentinel = base / ".manifest.json"

    if sentinel.exists():
        try:
            m = json.loads(sentinel.read_text())
            if (
                m.get("n_left") == n_left
                and m.get("n_files_left") == n_files_left
                and m.get("n_right") == n_right
                and m.get("n_files_right") == n_files_right
                and m.get("seed") == seed
            ):
                return left_dir, right_dir
        except Exception:
            pass

    left_dir.mkdir(parents=True, exist_ok=True)
    right_dir.mkdir(parents=True, exist_ok=True)
    print(
        f"[gen] creating join dataset n_left={n_left} n_files_left={n_files_left} "
        f"n_right={n_right} n_files_right={n_files_right} -> {base}"
    )
    sys.stdout.flush()

    rows_per_right = n_right // n_files_right
    rem_r = n_right - rows_per_right * n_files_right
    start = 0
    for i in range(n_files_right):
        n = rows_per_right + (1 if i < rem_r else 0)
        tbl = _build_orders_table(n, start, seed + 100 + i)
        pq.write_table(
            tbl, right_dir / f"part-{i:04d}.snappy.parquet", compression="snappy"
        )
        start += n

    rows_per_left = n_left // n_files_left
    rem_l = n_left - rows_per_left * n_files_left
    for i in range(n_files_left):
        n = rows_per_left + (1 if i < rem_l else 0)
        tbl = _build_lineitem_table(n, n_right, seed + 200 + i)
        pq.write_table(
            tbl, left_dir / f"part-{i:04d}.snappy.parquet", compression="snappy"
        )

    sentinel.write_text(
        json.dumps(
            {
                "n_left": n_left,
                "n_files_left": n_files_left,
                "n_right": n_right,
                "n_files_right": n_files_right,
                "seed": seed,
            }
        )
    )
    return left_dir, right_dir


# ----------------------------------------------------------------------------
# Signal capture
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
# RSS-sampling thread for aggregator actors.
#
# Aggregator processes are launched as Ray actors of class
# `HashShuffleAggregator`; Ray sets the cmdline so it includes that name.
# We sample every `interval` seconds and record the maximum RSS observed
# across all matching processes during the rep window.  This is exogenous
# (just /proc) and is the primary R2 signal.
#
# Detection assumption: aggregator process cmdline includes one of
# `HashShuffleAggregator`, `JoinAggregator`, or `Aggregator`.  We match on
# any of these tokens to be robust to future renames.
# ----------------------------------------------------------------------------


_AGG_PATTERNS = [
    re.compile(r"HashShuffleAggregator"),
    re.compile(r"JoinAggregator"),
]


@dataclass
class AggregatorRssSample:
    peak_rss_bytes: int = 0
    peak_n_aggregators: int = 0
    samples: int = 0
    sum_total_rss_bytes: int = 0
    # Per-process peak (max RSS observed for any single aggregator at any
    # sample point) — this is the relevant R2 number.
    max_single_proc_rss_bytes: int = 0


class AggregatorRssSampler:
    def __init__(self, interval_s: float = 0.25):
        self.interval_s = interval_s
        self.state = AggregatorRssSample()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _matches(self, cmdline: List[str]) -> bool:
        joined = " ".join(cmdline) if cmdline else ""
        for pat in _AGG_PATTERNS:
            if pat.search(joined):
                return True
        return False

    def _sample_once(self) -> None:
        # Fast /proc walker: avoid psutil.process_iter() overhead which can
        # easily exceed the sampling interval on a busy host with thousands
        # of zombies.  Read /proc/<pid>/comm (cheap, 16 bytes) to filter,
        # then /proc/<pid>/status for VmRSS.  Skip zombies.
        try:
            total = 0
            n = 0
            local_max = 0
            try:
                pids = [p for p in os.listdir("/proc") if p.isdigit()]
            except Exception:
                pids = []
            for pid in pids:
                try:
                    with open(f"/proc/{pid}/comm", "r") as f:
                        comm = f.read().strip()
                    if "HashShuffl" not in comm and "JoinAggreg" not in comm:
                        continue
                    rss_kb = 0
                    is_zombie = False
                    with open(f"/proc/{pid}/status", "r") as f:
                        for line in f:
                            if line.startswith("State:"):
                                if "Z" in line:
                                    is_zombie = True
                                    break
                            elif line.startswith("VmRSS:"):
                                rss_kb = int(line.split()[1])
                                break
                    if is_zombie or rss_kb == 0:
                        continue
                    rss = rss_kb * 1024
                    total += rss
                    n += 1
                    if rss > local_max:
                        local_max = rss
                except (FileNotFoundError, ProcessLookupError, PermissionError):
                    continue
                except Exception:
                    continue
            self.state.samples += 1
            self.state.sum_total_rss_bytes += total
            if total > self.state.peak_rss_bytes:
                self.state.peak_rss_bytes = total
            if n > self.state.peak_n_aggregators:
                self.state.peak_n_aggregators = n
            if local_max > self.state.max_single_proc_rss_bytes:
                self.state.max_single_proc_rss_bytes = local_max
        except Exception:
            pass

    def _run(self) -> None:
        while not self._stop.is_set():
            self._sample_once()
            self._stop.wait(self.interval_s)

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> AggregatorRssSample:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        return self.state

    def reset(self) -> None:
        self.state = AggregatorRssSample()


# ----------------------------------------------------------------------------
# Rep harness
# ----------------------------------------------------------------------------


@dataclass
class RepResult:
    name: str
    seconds: float
    spilled_bytes: int
    out_rows: int
    agg_peak_total_rss: int
    agg_peak_single_rss: int
    agg_peak_n: int
    driver_rss_before: int
    driver_rss_after: int
    driver_maxrss: int
    extra: Dict[str, Any] = field(default_factory=dict)


def run_reps(
    name: str,
    fn: Callable[[AggregatorRssSampler], Dict[str, Any]],
    n_reps: int = 3,
    warmup: int = 1,
) -> List[RepResult]:
    results: List[RepResult] = []
    total = warmup + n_reps
    for i in range(total):
        gc.collect()
        sampler = AggregatorRssSampler(interval_s=0.1)
        sampler.start()
        rss_before = driver_rss_bytes()
        spilled_before = get_spilled_bytes_total()
        t0 = time.perf_counter()
        try:
            extra = fn(sampler) or {}
        finally:
            agg_state = sampler.stop()
        dt = time.perf_counter() - t0
        spilled_after = get_spilled_bytes_total()
        rss_after = driver_rss_bytes()
        max_rss = driver_maxrss_bytes()
        spilled = (
            max(0, spilled_after - spilled_before)
            if spilled_before >= 0 and spilled_after >= 0
            else -1
        )
        out_rows = int(extra.pop("out_rows", -1))
        rep = RepResult(
            name=f"{name}#{i}{'_warm' if i < warmup else ''}",
            seconds=dt,
            spilled_bytes=spilled,
            out_rows=out_rows,
            agg_peak_total_rss=agg_state.peak_rss_bytes,
            agg_peak_single_rss=agg_state.max_single_proc_rss_bytes,
            agg_peak_n=agg_state.peak_n_aggregators,
            driver_rss_before=rss_before,
            driver_rss_after=rss_after,
            driver_maxrss=max_rss,
            extra=extra,
        )
        results.append(rep)
        tag = "warmup" if i < warmup else f"rep {i - warmup + 1}/{n_reps}"
        print(
            f"[{name}] {tag}: {dt:.2f}s  "
            f"out_rows={out_rows}  "
            f"spilled={spilled / 1e9:.3f}GB  "
            f"agg_peak_total={agg_state.peak_rss_bytes / 1e9:.2f}GB  "
            f"agg_peak_single={agg_state.max_single_proc_rss_bytes / 1e9:.2f}GB  "
            f"agg_n={agg_state.peak_n_aggregators}  "
            f"maxrss={max_rss / 1e9:.2f}GB  "
            f"extra={extra}"
        )
        sys.stdout.flush()
    return results


def summarize(reps: List[RepResult], skip_warmup: int = 1) -> Dict[str, Any]:
    measured = reps[skip_warmup:]
    times = [r.seconds for r in measured]
    spills = [r.spilled_bytes for r in measured if r.spilled_bytes >= 0]
    out_rows = [r.out_rows for r in measured if r.out_rows >= 0]
    agg_total = [r.agg_peak_total_rss for r in measured]
    agg_single = [r.agg_peak_single_rss for r in measured]
    return {
        "n_reps": len(measured),
        "wall_seconds_mean": statistics.fmean(times) if times else float("nan"),
        "wall_seconds_stdev": statistics.pstdev(times) if len(times) > 1 else 0.0,
        "wall_seconds_min": min(times) if times else float("nan"),
        "wall_seconds_max": max(times) if times else float("nan"),
        "spilled_gb_mean": statistics.fmean(spills) / 1e9 if spills else 0.0,
        "spilled_gb_max": max(spills) / 1e9 if spills else 0.0,
        "out_rows_set": sorted(set(out_rows)),
        "agg_peak_total_gb_mean": statistics.fmean(agg_total) / 1e9 if agg_total else 0.0,
        "agg_peak_total_gb_max": max(agg_total) / 1e9 if agg_total else 0.0,
        "agg_peak_single_gb_max": max(agg_single) / 1e9 if agg_single else 0.0,
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
# Bench entry helper.
# ----------------------------------------------------------------------------


def build_and_run(
    *,
    name: str,
    n_left: int,
    n_files_left: int,
    n_right: int,
    n_files_right: int,
    join_type: str,
    num_partitions: int,
    object_store_memory_bytes: Optional[int] = None,
    n_reps: int = 3,
    warmup: int = 1,
    num_cpus: int = 24,
    max_hash_shuffle_aggregators: Optional[int] = None,
    extra_init_kwargs: Optional[Dict[str, Any]] = None,
    capture_stats: bool = True,
) -> Dict[str, Any]:
    """Init Ray, ensure data, run the join workload n_reps times.

    R3-targeting benchmarks pass `object_store_memory_bytes` smaller than
    the shuffle working set; combined with `RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1`
    that pushes plasma onto /tmp.  Object store sizes above /dev/shm (8 GB
    on this devpod) similarly fall back to /tmp.
    """

    if max_hash_shuffle_aggregators is not None:
        os.environ["RAY_DATA_MAX_HASH_SHUFFLE_AGGREGATORS"] = str(
            max_hash_shuffle_aggregators
        )

    left_dir, right_dir = ensure_join_dataset(
        n_left=n_left,
        n_files_left=n_files_left,
        n_right=n_right,
        n_files_right=n_files_right,
    )

    init_kwargs: Dict[str, Any] = {
        "num_cpus": num_cpus,
        "include_dashboard": False,
        "ignore_reinit_error": True,
        "log_to_driver": False,
    }
    if object_store_memory_bytes is not None:
        os.environ.setdefault("RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE", "1")
        init_kwargs["object_store_memory"] = object_store_memory_bytes
    if extra_init_kwargs:
        init_kwargs.update(extra_init_kwargs)

    ray.init(**init_kwargs)
    try:
        def fn(sampler: AggregatorRssSampler) -> Dict[str, Any]:
            left_ds = ray.data.read_parquet(str(left_dir))
            right_ds = ray.data.read_parquet(str(right_dir))
            joined = left_ds.join(
                right_ds,
                num_partitions=num_partitions,
                on=["column00"],
                right_on=["column0"],
                join_type=join_type,
            )
            n_rows = joined.count()
            extra: Dict[str, Any] = {"out_rows": n_rows}
            if capture_stats:
                # Pull operator-level stats from the materialised dataset
                # via the runtime metrics string.  We grep for the Join
                # operator's avg max USS line — the simplest stable hook.
                try:
                    s = joined.stats()
                    # Look for "Join(...)" block and "max USS" line therein.
                    join_idx = s.find("Join(")
                    if join_idx >= 0:
                        chunk = s[join_idx : join_idx + 4000]
                        m = re.search(
                            r"USS.*?:\s*([0-9\.]+)\s*MiB.*?max\s*([0-9\.]+)\s*MiB",
                            chunk,
                            re.IGNORECASE | re.DOTALL,
                        )
                        if m:
                            extra["join_uss_mean_mib"] = float(m.group(1))
                            extra["join_uss_max_mib"] = float(m.group(2))
                except Exception:
                    pass
            return extra

        reps = run_reps(name, fn, n_reps=n_reps, warmup=warmup)
        summary = summarize(reps, skip_warmup=warmup)
        print_summary(name, summary)
        return summary
    finally:
        ray.shutdown()
