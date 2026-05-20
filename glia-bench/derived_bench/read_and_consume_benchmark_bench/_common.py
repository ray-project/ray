"""Shared utilities for read_and_consume_benchmark-derived benchmarks.

Provides:
- Synthetic local data generators (parquet, JPEG). Deterministically
  seeded so reps repeat byte-identically.
- Signal capture helpers (rss, max-rss, spilled-bytes, cpus-busy via
  /proc-based per-process sampling).
- A signature-forwarding patch for `Dataset.iter_internal_ref_bundles`
  that times the iteration loop and snapshots the streaming executor
  before upstream wipes its handle on iter exit.
- Stats parsing helpers (operator wall time from `ds.stats()`, per-op
  backpressure metrics from the streaming executor topology).
- Rep harness (warmup + N reps with summary).
"""

from __future__ import annotations

import gc
import os
import re
import resource
import statistics
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import psutil

import ray
from ray._private.internal_api import (
    get_memory_info_reply,
    get_state_from_address,
)


# ----------------------------------------------------------------------------
# Synthetic data generation. All deterministic given the seed.
# ----------------------------------------------------------------------------

def make_parquet_dataset(
    out_dir: str,
    *,
    num_files: int,
    rows_per_file: int,
    payload_bytes: int = 64,
    extra_int_columns: int = 0,
    seed: int = 0,
    overwrite: bool = False,
) -> str:
    """Generate `num_files` parquet files, each with a 64-bit int key,
    a fixed-size binary payload, and `extra_int_columns` extra int64 cols.

    File contents depend only on (seed, file_idx), so identical
    invocations across reps reuse the same files (idempotent generation).
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    os.makedirs(out_dir, exist_ok=True)
    fields = [pa.field("c_0", pa.int64()), pa.field("c_1", pa.binary(payload_bytes))]
    fields += [pa.field(f"c_{i + 2}", pa.int64()) for i in range(extra_int_columns)]
    schema = pa.schema(fields)

    written = 0
    for i in range(num_files):
        path = os.path.join(out_dir, f"part-{i:05d}.parquet")
        if os.path.exists(path) and not overwrite:
            continue
        rng = np.random.default_rng(seed * 10_007 + i)
        keys = rng.integers(0, np.iinfo(np.int64).max, size=rows_per_file, dtype=np.int64)
        # Per-row distinct payload bytes (high-entropy → poor compression
        # → on-disk size ~= raw size, decode work non-trivial).
        payload_blob = rng.bytes(rows_per_file * payload_bytes)
        payload_list = [
            payload_blob[j * payload_bytes:(j + 1) * payload_bytes]
            for j in range(rows_per_file)
        ]
        arrays = [pa.array(keys), pa.array(payload_list, type=pa.binary(payload_bytes))]
        for j in range(extra_int_columns):
            arrays.append(pa.array(
                rng.integers(0, 2**31, size=rows_per_file, dtype=np.int64)
            ))
        table = pa.Table.from_arrays(arrays, schema=schema)
        # Use SNAPPY so decode CPU is non-trivial but fast.
        pq.write_table(table, path, compression="snappy")
        written += 1
    return out_dir


def make_image_dataset(
    out_dir: str,
    *,
    num_files: int,
    width: int = 256,
    height: int = 256,
    seed: int = 0,
    overwrite: bool = False,
) -> str:
    """Generate `num_files` random-content JPEG files.

    Random pixel content is incompressible, so each file is roughly
    width*height*3 bytes after JPEG encode (JPEG of high-entropy noise
    is ~80%+ of raw). That keeps per-file decode time meaningful.
    """
    from PIL import Image

    os.makedirs(out_dir, exist_ok=True)
    for i in range(num_files):
        path = os.path.join(out_dir, f"img-{i:05d}.jpg")
        if os.path.exists(path) and not overwrite:
            continue
        rng = np.random.default_rng(seed * 7919 + i)
        arr = rng.integers(0, 256, size=(height, width, 3), dtype=np.uint8)
        Image.fromarray(arr, mode="RGB").save(path, format="JPEG", quality=85)
    return out_dir


# ----------------------------------------------------------------------------
# Signal capture helpers
# ----------------------------------------------------------------------------

def get_spilled_bytes_total() -> int:
    """Cluster-wide spilled bytes from the GCS memory summary. -1 on error."""
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


class CPUSampler:
    """Background CPU% sampler. Captures cpu_percent of the *current
    process tree* (driver + ray workers spawned in this session) at
    fixed cadence in a daemon thread. Used to detect R2's "CPUs pegged
    for the duration" pattern.

    Reports a normalized utilization in [0, num_cpus_target]. We use
    psutil's per-process cpu_percent (which reports % of one CPU per
    process), summed across the current process tree, then divided by
    100 to get a "CPUs busy" count. Compare to the configured
    `num_cpus` ceiling: ratio near 1.0 means R2 is pegged.
    """

    def __init__(self, interval_s: float = 0.25, num_cpus_target: int = 1) -> None:
        self._interval = interval_s
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._target = num_cpus_target
        # Cache of psutil.Process objects we already identified as 'us'.
        # `process_iter` is O(host procs) per call, so only run the
        # expensive scan once per `_rescan_every` ticks.
        self._procs: Dict[int, psutil.Process] = {}
        self._rescan_counter = 0
        # Re-scan host process list every Nth tick. The scan is O(host
        # procs) which is huge on shared K8s hosts (~40k procs in our
        # case). Doing it once at start + once near end suffices.
        self._rescan_every = 32
        # Map cpu_percent (which is host-relative on Linux) to "CPUs
        # busy across the host" by multiplying by host cpu count.
        self._host_cpus = psutil.cpu_count(logical=True) or 1
        self.samples: List[float] = []

    def _is_ray_proc(self, name: str) -> bool:
        return "ray::" in name or "raylet" in name or "plasma_store" in name

    def _rescan_procs(self) -> None:
        """Re-scan for new Ray worker processes via /proc.

        psutil's `process_iter` is O(host procs) because it constructs
        Process objects for every PID on the box. On shared K8s hosts
        that's ~40k procs, which makes a single scan take seconds.

        We use /proc/<pid>/comm for cheap name-only filtering, then
        only construct Process objects for the matches.
        """
        # Drop dead ones.
        dead = [pid for pid, p in self._procs.items() if not p.is_running()]
        for pid in dead:
            self._procs.pop(pid, None)

        # Find candidates by name from /proc.
        try:
            for entry in os.listdir("/proc"):
                if not entry.isdigit():
                    continue
                pid = int(entry)
                if pid in self._procs:
                    continue
                try:
                    with open(f"/proc/{pid}/comm", "rb") as f:
                        name = f.read().rstrip(b"\n").decode(errors="replace")
                except OSError:
                    continue
                if pid != os.getpid() and not self._is_ray_proc(name):
                    continue
                try:
                    p = psutil.Process(pid)
                    p.cpu_percent(interval=None)  # prime
                    self._procs[pid] = p
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        except Exception:
            pass

    def _sample_cpus_busy(self) -> float:
        """Sum per-process cpu_percent across our cached driver + Ray
        workers. Per-process cpu_percent returns 0..100 per CPU, so
        total/100 = CPUs busy (independent of host cpu count).
        """
        # On hosts with very large process tables, `process_iter` is
        # slow. Rescan once at the start (priming, in __enter__),
        # again at sample 1 (workers may have spawned by then), then
        # only every `_rescan_every` ticks afterwards.
        if self._rescan_counter == 1 or self._rescan_counter % self._rescan_every == 0:
            self._rescan_procs()
        self._rescan_counter += 1

        total = 0.0
        dead = []
        for pid, p in self._procs.items():
            try:
                total += p.cpu_percent(interval=None)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                dead.append(pid)
        for pid in dead:
            self._procs.pop(pid, None)
        return total / 100.0

    def __enter__(self) -> "CPUSampler":
        self._stop.clear()
        self.samples = []
        self._procs.clear()
        self._rescan_counter = 0
        # Prime cpu_percent on all matching processes.
        self._rescan_procs()

        def _loop() -> None:
            while not self._stop.is_set():
                # Sleep first so each sample covers the interval.
                time.sleep(self._interval)
                self.samples.append(self._sample_cpus_busy())

        self._thread = threading.Thread(target=_loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def summary(self) -> Dict[str, float]:
        if not self.samples:
            return {"cpus_busy_mean": 0.0, "cpus_busy_p50": 0.0,
                    "cpus_busy_p90": 0.0, "cpus_busy_max": 0.0,
                    "cpus_busy_target_ratio_p90": 0.0, "n_cpu_samples": 0}
        s = sorted(self.samples)
        n = len(s)
        p90 = s[min(n - 1, int(0.9 * n))]
        return {
            "cpus_busy_mean": round(statistics.fmean(s), 2),
            "cpus_busy_p50": round(s[n // 2], 2),
            "cpus_busy_p90": round(p90, 2),
            "cpus_busy_max": round(max(s), 2),
            "cpus_busy_target_ratio_p90": (
                round(p90 / self._target, 3) if self._target else 0.0
            ),
            "n_cpu_samples": n,
        }


# ----------------------------------------------------------------------------
# Signature-forwarding patches.
#
# Each patch uses *args, **kwargs to survive upstream signature changes.
# All patched callables are invoked from the driver process; patching the
# class attribute reaches the call site. Patches return a state object +
# a `restore()` callable.
# ----------------------------------------------------------------------------

@dataclass
class IterPatchState:
    """Tracks the time spent inside the iter_internal_ref_bundles
    iterator's `__next__`, plus the count of bundles yielded. Also
    captures the dataset's `_current_executor` snapshot taken before
    the executor is shut down (so we can read backpressure metrics
    after iter completes).
    """
    n_bundles: int = 0
    iter_seconds: float = 0.0
    captured_executor: Any = None


def install_iter_bundles_patch() -> Tuple[IterPatchState, Callable[[], None]]:
    """Wrap `Dataset.iter_internal_ref_bundles` to time each `next()`
    call from the driver loop, count bundles, and capture the
    streaming executor handle before the synchronize_progress_bar
    in upstream wipes it.

    Structural assumption: `Dataset.iter_internal_ref_bundles` returns
    an iterator we can wrap, and `Dataset._current_executor` is set
    by `_execute_to_iterator` before iteration begins.
    """
    from ray.data import dataset as _ds

    state = IterPatchState()
    orig = _ds.Dataset.iter_internal_ref_bundles

    def wrapped(self, *args: Any, **kwargs: Any):
        inner = orig(self, *args, **kwargs)
        # Snapshot the executor *before* iteration drains, so we can
        # read its topology metrics afterwards.
        state.captured_executor = self._current_executor

        def gen():
            # If self._current_executor was set lazily, capture again on
            # first iteration.
            if state.captured_executor is None:
                state.captured_executor = self._current_executor
            while True:
                t0 = time.perf_counter()
                try:
                    item = next(inner)
                except StopIteration:
                    return
                state.iter_seconds += time.perf_counter() - t0
                state.n_bundles += 1
                yield item

        return gen()

    _ds.Dataset.iter_internal_ref_bundles = wrapped

    def restore() -> None:
        _ds.Dataset.iter_internal_ref_bundles = orig

    return state, restore


# ----------------------------------------------------------------------------
# Stats parsing helpers (built-in metric extraction from ds.stats()).
# ----------------------------------------------------------------------------

def parse_op_total_seconds(stats: str, op_pattern: str) -> float:
    """Find the first 'Operator' or 'Suboperator' block whose header
    matches `op_pattern` and return the 'Remote wall time: ... X.XXs total'.
    """
    in_block = False
    total_seconds = 0.0
    for line in stats.splitlines():
        if not in_block and re.search(op_pattern, line):
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
            if line.startswith("Operator ") or line.startswith("Suboperator "):
                break
    return total_seconds


def collect_op_backpressure_metrics(ds_or_executor: Any) -> Dict[str, Any]:
    """Surface per-op backpressure + obj-store-mem metrics from the most
    recent executor run. The streaming executor exposes
    `_topology` (a Dict[op, OpState]), and each op carries `_metrics`
    (OpRuntimeMetrics). We extract:
      - task_submission_backpressure_time (seconds an op was blocked
        from launching new tasks because its outputs piled up)
      - task_output_backpressure_time
      - obj_store_mem_used (snapshot at end of run)
      - num_outputs_total
    Returns a flat dict keyed by op name.

    Accepts either a Dataset (reads `_current_executor`) or a
    StreamingExecutor directly (so callers can capture and pass in a
    snapshot before iter completes — upstream clears the executor
    handle on iter exit).

    Structural assumption: the executor is a `StreamingExecutor` with
    a `_topology` attribute mapping each PhysicalOperator to its OpState.
    Both attributes are private; if upstream renames them, this helper
    returns {} silently.
    """
    out: Dict[str, Any] = {}
    try:
        ex = ds_or_executor
        if hasattr(ex, "_current_executor"):
            ex = ex._current_executor
        if ex is None:
            return out
        topo = getattr(ex, "_topology", None) or {}
        for op, _ in topo.items():
            try:
                m = op._metrics
                name = op.__class__.__name__
                # The first matching op wins; if multiple we tag by index.
                key = name
                while key in out:
                    key = f"{name}#{len(out)}"
                out[key] = {
                    "submission_bp_s": round(
                        getattr(m, "task_submission_backpressure_time", 0) or 0, 3
                    ),
                    "output_bp_s": round(
                        getattr(m, "task_output_backpressure_time", 0) or 0, 3
                    ),
                    "obj_store_mem_used_mb": round(
                        (getattr(m, "obj_store_mem_used", 0) or 0) / 1e6, 1
                    ),
                    "num_outputs_total": int(
                        getattr(m, "num_outputs_total", 0) or 0
                    ),
                    "num_tasks_finished": int(
                        getattr(m, "num_tasks_finished", 0) or 0
                    ),
                }
            except Exception:
                continue
    except Exception:
        return out
    return out


def parse_top_operator_seconds(stats: str, op_name_pattern: str) -> float:
    """Parse the 'Operator N OpName...: ... in X.YZs' wall time line.

    The 2.55 stats text uses 'produced in X.XXs' for read ops and
    'executed in X.XXs' for sort/aggregate ops. Match either.
    """
    m = re.search(
        rf"Operator \d+\s+{op_name_pattern}[^:]*:.*?(?:produced|executed) in ([0-9.]+)s",
        stats,
    )
    return float(m.group(1)) if m else 0.0


def parse_op_remote_total_seconds(stats: str, op_name_pattern: str) -> float:
    """Find 'Operator N OpName...:' header matching pattern, then return
    the 'Remote wall time: ... X.XXs total' for that operator's block.

    More reliable than per-operator wall when the operator overlaps
    others in streaming execution.
    """
    in_block = False
    found_seconds = 0.0
    for line in stats.splitlines():
        if not in_block:
            if re.search(
                rf"^Operator \d+\s+{op_name_pattern}", line
            ):
                in_block = True
            continue
        # We're inside the block
        m = re.search(r"Remote wall time:.* ([0-9.]+)([smµun]+) total", line)
        if m:
            val = float(m.group(1))
            unit = m.group(2)
            if unit.startswith("ms"):
                val /= 1000
            elif unit.startswith("us") or unit.startswith("µs"):
                val /= 1_000_000
            found_seconds = val
            break
        if line.startswith("Operator "):
            break
    return found_seconds


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
    sample_cpu: bool = True,
    num_cpus_target: int = 1,
) -> List[RepResult]:
    results: List[RepResult] = []
    total = warmup + n_reps
    for i in range(total):
        gc.collect()
        rss_before = driver_rss_bytes()
        spilled_before = get_spilled_bytes_total()

        sampler = CPUSampler(num_cpus_target=num_cpus_target) if sample_cpu else None
        if sampler is not None:
            sampler.__enter__()
        t0 = time.perf_counter()
        try:
            extra = fn() or {}
        finally:
            if sampler is not None:
                sampler.__exit__(None, None, None)
        dt = time.perf_counter() - t0

        spilled_after = get_spilled_bytes_total()
        rss_after = driver_rss_bytes()
        max_rss = driver_maxrss_bytes()
        spilled = (
            max(0, spilled_after - spilled_before)
            if spilled_before >= 0 and spilled_after >= 0
            else -1
        )
        merged: Dict[str, Any] = dict(extra)
        if sampler is not None:
            merged.update(sampler.summary())
        rep = RepResult(
            name=f"{name}#{i}{'_warm' if i < warmup else ''}",
            seconds=dt,
            spilled_bytes=spilled,
            driver_rss_before=rss_before,
            driver_rss_after=rss_after,
            driver_maxrss=max_rss,
            extra=merged,
        )
        results.append(rep)
        tag = "warmup" if i < warmup else f"rep {i - warmup + 1}/{n_reps}"
        cpu_keys = {"cpus_busy_mean", "cpus_busy_p50", "cpus_busy_p90",
                    "cpus_busy_max", "cpus_busy_target_ratio_p90", "n_cpu_samples"}
        cpu_str = (
            f" cpus_p90={merged.get('cpus_busy_p90', 0):.1f}/"
            f"{num_cpus_target}({merged.get('cpus_busy_target_ratio_p90', 0):.2f})"
            if sample_cpu else ""
        )
        print(
            f"[{name}] {tag}: {dt:.2f}s  "
            f"spilled={spilled / 1e9:.3f}GB  "
            f"rss_delta={(rss_after - rss_before) / 1e6:.1f}MB  "
            f"maxrss={max_rss / 1e9:.2f}GB{cpu_str}  "
            f"extra={ {k: v for k, v in merged.items() if k not in cpu_keys} }"
        )
        sys.stdout.flush()
    return results


def summarize(reps: List[RepResult], skip_warmup: int = 1) -> Dict[str, Any]:
    measured = reps[skip_warmup:]
    times = [r.seconds for r in measured]
    cpus_p90 = [r.extra.get("cpus_busy_p90", 0.0) for r in measured]
    cpus_ratio = [r.extra.get("cpus_busy_target_ratio_p90", 0.0) for r in measured]
    cpu_keys = {"cpus_busy_mean", "cpus_busy_p50", "cpus_busy_p90",
                "cpus_busy_max", "cpus_busy_target_ratio_p90", "n_cpu_samples"}
    extras = [{k: v for k, v in r.extra.items() if k not in cpu_keys}
              for r in measured]
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
                [(r.driver_rss_after - r.driver_rss_before) / 1e6 for r in measured]
            )
            if measured
            else 0.0
        ),
        "cpus_busy_p90_mean": (
            round(statistics.fmean(cpus_p90), 2) if cpus_p90 else 0.0
        ),
        "cpus_busy_target_ratio_p90_mean": (
            round(statistics.fmean(cpus_ratio), 3) if cpus_ratio else 0.0
        ),
        "extras": extras,
    }


def print_summary(name: str, summary: Dict[str, Any]) -> None:
    print(f"\n=== {name} summary ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    sys.stdout.flush()
