"""Shared utilities for sort_benchmark-derived benchmarks.

Each benchmark imports from this module so the regime measurement primitives
(deterministic data source, signature-forwarding patches, signal capture,
rep harness) live in one place.
"""

from __future__ import annotations

import gc
import os
import resource
import statistics
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Tuple

import numpy as np
import psutil

import ray
from ray._private.internal_api import (
    get_memory_info_reply,
    get_state_from_address,
)
from ray.data._internal.util import _check_pyarrow_version
from ray.data.block import Block, BlockMetadata
from ray.data.datasource import Datasource, ReadTask


# ----------------------------------------------------------------------------
# Deterministic int + binary datasource (mirrors upstream RandomIntRowDatasource
# but seeded so the workload is reproducible across reps and processes).
# ----------------------------------------------------------------------------

class DeterministicIntRowDatasource(Datasource):
    """An Arrow datasource with a fixed-size binary payload column and a
    deterministic int64 key column. Each block uses an independent seeded
    numpy RNG, so partition contents do not depend on task scheduling order.
    """

    def __init__(self, seed: int = 0) -> None:
        self._seed = seed

    def prepare_read(
        self, parallelism: int, n: int, row_size_bytes: int
    ) -> List[ReadTask]:
        _check_pyarrow_version()
        import pyarrow

        seed = self._seed
        block_size = max(1, n // parallelism)
        rng = np.random.default_rng(seed)
        # Single payload row, repeated within each block. This matches the
        # upstream test's behavior of constructing one bytes blob per block.
        row = rng.bytes(row_size_bytes)

        schema = pyarrow.schema(
            [
                pyarrow.field("c_0", pyarrow.int64()),
                # Fixed-size binary avoids Arrow list-offset overflow when
                # the per-block row count is large.
                pyarrow.field("c_1", pyarrow.binary(row_size_bytes)),
            ]
        )

        read_tasks: List[ReadTask] = []
        i = 0
        block_idx = 0
        while i < n:
            count = min(block_size, n - i)
            meta = BlockMetadata(
                num_rows=count,
                size_bytes=count * (8 + row_size_bytes),
                input_files=None,
                exec_stats=None,
            )
            block_seed = (seed + 1) * 1_000_003 + block_idx

            def make_block(count: int = count, block_seed: int = block_seed) -> Block:
                gen = np.random.default_rng(block_seed)
                keys = gen.integers(
                    low=0,
                    high=np.iinfo(np.int64).max,
                    size=(count,),
                    dtype=np.int64,
                )
                return pyarrow.Table.from_arrays(
                    [keys, [row for _ in range(count)]], schema=schema
                )

            read_tasks.append(
                ReadTask(lambda mb=make_block: [mb()], meta, schema=schema)
            )
            i += block_size
            block_idx += 1

        return read_tasks


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
    # ru_maxrss is in KiB on Linux.
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024)


# ----------------------------------------------------------------------------
# Signature-forwarding patches.
#
# These patches use *args, **kwargs to survive upstream signature changes.
# Both patched callables are invoked from the driver process — that's why
# patching works (replacing the class attribute reaches the call site). We
# do NOT patch the static methods that ray.remote()-wraps (e.g.,
# `SortTaskSpec.reduce`, `ShuffleTaskSpec.map`) because those run on remote
# workers; state mutations there don't propagate back to the driver, so
# such patches would silently see zero events.
# ----------------------------------------------------------------------------

@dataclass
class SamplePatchState:
    calls: int = 0
    total_seconds: float = 0.0


def install_sample_boundaries_patch() -> Tuple[SamplePatchState, Callable[[], None]]:
    """Time `SortTaskSpec.sample_boundaries` (driver-side; dispatches the
    sample remote tasks and blocks waiting for them). Useful when sampling
    is the optimization target.

    Structural assumption: `SortTaskSpec` exposes a static method named
    `sample_boundaries`.
    """
    from ray.data._internal.planner.exchange import sort_task_spec as _sts

    state = SamplePatchState()
    orig = _sts.SortTaskSpec.sample_boundaries

    def wrapped(*args: Any, **kwargs: Any):
        t0 = time.perf_counter()
        try:
            return orig(*args, **kwargs)
        finally:
            state.calls += 1
            state.total_seconds += time.perf_counter() - t0

    _sts.SortTaskSpec.sample_boundaries = staticmethod(wrapped)

    def restore() -> None:
        _sts.SortTaskSpec.sample_boundaries = staticmethod(orig)

    return state, restore


@dataclass
class SchedulerPatchState:
    last_input_blocks: int = 0
    last_output_blocks: int = 0


def install_pull_scheduler_patch() -> Tuple[SchedulerPatchState, Callable[[], None]]:
    """Capture (input_num_blocks, output_num_blocks) from
    `PullBasedShuffleTaskScheduler.execute`. The product is the M*N driver-
    side object-ref bookkeeping load.

    Structural assumption: `PullBasedShuffleTaskScheduler.execute` is the
    method invoked once per all-to-all op when push-based shuffle is off.
    """
    from ray.data._internal.planner.exchange import (
        pull_based_shuffle_task_scheduler as _p,
    )

    state = SchedulerPatchState()
    orig = _p.PullBasedShuffleTaskScheduler.execute

    def wrapped(self, refs, output_num_blocks, *args, **kwargs):
        try:
            inb = sum(len(r.block_refs) for r in refs)
        except Exception:
            inb = -1
        state.last_input_blocks = inb
        state.last_output_blocks = output_num_blocks
        return orig(self, refs, output_num_blocks, *args, **kwargs)

    _p.PullBasedShuffleTaskScheduler.execute = wrapped

    def restore() -> None:
        _p.PullBasedShuffleTaskScheduler.execute = orig

    return state, restore


# ----------------------------------------------------------------------------
# Stats parsing helpers (built-in metric extraction from ds.stats()).
# ----------------------------------------------------------------------------

def parse_op_total_seconds(stats: str, op_pattern: str) -> float:
    """Find a sub-operator block matching `op_pattern` and return its
    'Remote wall time: ... X.XXs total' value in seconds.
    """
    import re

    in_block = False
    total_seconds = 0.0
    for line in stats.splitlines():
        if re.search(op_pattern, line):
            in_block = True
            continue
        if in_block:
            m = re.search(r"Remote wall time:.* ([0-9.]+)([smµun]+) total", line)
            if m:
                val = float(m.group(1))
                unit = m.group(2)
                if unit.startswith("ms"):
                    val /= 1000
                elif unit.startswith("us") or unit.startswith("µs"):
                    val /= 1_000_000
                total_seconds = val
                break
            if line.startswith("Suboperator ") or line.startswith("Operator "):
                break
    return total_seconds


def parse_sort_op_seconds(stats: str) -> float:
    """Top-level Sort operator wall time from `ds.stats()`."""
    import re

    m = re.search(r"Operator \d+ Sort: executed in ([0-9.]+)s", stats)
    return float(m.group(1)) if m else 0.0


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
    extra: Dict[str, Any]


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
        extra = fn()
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
            extra=extra or {},
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
    return {
        "n_reps": len(measured),
        "wall_seconds_mean": statistics.fmean(times) if times else float("nan"),
        "wall_seconds_stdev": statistics.pstdev(times) if len(times) > 1 else 0.0,
        "wall_seconds_min": min(times) if times else float("nan"),
        "wall_seconds_max": max(times) if times else float("nan"),
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
                [
                    (r.driver_rss_after - r.driver_rss_before) / 1e6
                    for r in measured
                ]
            )
            if measured
            else 0.0
        ),
        "extras": [r.extra for r in measured],
    }


def print_summary(name: str, summary: Dict[str, Any]) -> None:
    print(f"\n=== {name} summary ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    sys.stdout.flush()
