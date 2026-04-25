#!/usr/bin/env python3
"""Configurable Ray Data scheduler benchmark.

Supports two workload types:
  - synthetic: heterogeneous task-pool chain with varied batch sizes and a filter,
    stressing the scheduling loop's per-iteration overhead, ranker, and backpressure.
  - mixed_pipeline: range -> noop(tasks) -> inference(actors, dummy model) ->
    noop(tasks) -> materialize. Exercises actor pool scheduling, mixed dispatch,
    and asymmetric backpressure. No I/O — scheduler-bound like synthetic.

Usage:
  # Fast iteration (synthetic only, public data):
  python benchmark_scheduler.py --workload synthetic --config data/public/workload_config.json --artifact-dir .

  # Full golden (both workloads, private data):
  python benchmark_scheduler.py --workload all --config data/private/workload_config.json --artifact-dir .

  # Custom parameters:
  python benchmark_scheduler.py --workload synthetic --num-blocks 10000 --num-rows 100000000

  # With profiling:
  python benchmark_scheduler.py --workload synthetic --config data/public/workload_config.json --artifact-dir . --profile
"""

import argparse
import hashlib
import json
import math
import os
import sys
import time


def _hash_dataset(ds) -> str:
    """Hash a materialized dataset deterministically via SHA-256.

    Both block ordering AND row-to-block assignment are non-deterministic in
    Ray Data's streaming executor. To produce a stable hash regardless of how
    rows are partitioned across blocks:
    1. Collect all blocks into a single Arrow table.
    2. Sort by all columns to get a canonical row order.
    3. Serialize via Arrow IPC and SHA-256 hash the bytes.
    """
    import pyarrow as pa
    import ray as _ray

    refs = ds.to_arrow_refs()
    tables = [_ray.get(ref) for ref in refs]
    table = pa.concat_tables(tables)

    # Sort by all columns for canonical ordering
    if table.num_rows > 0 and table.num_columns > 0:
        sort_keys = [(col, "ascending") for col in table.column_names]
        table = table.sort_by(sort_keys)

    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def _collect_op_stats(ds):
    """Extract per-operator stats from a materialized dataset."""
    operators = []
    stats_summary = ds._get_stats_summary()
    if stats_summary and hasattr(stats_summary, "operators"):
        for op in stats_summary.operators:
            op_info = {"name": op.operator_name}
            if hasattr(op, "earliest_start_time") and hasattr(op, "latest_end_time"):
                if op.earliest_start_time and op.latest_end_time:
                    op_info["duration_sec"] = round(
                        op.latest_end_time - op.earliest_start_time, 3
                    )
            operators.append(op_info)
    return operators


def _validate_and_profile(ds, result, validate, profile, profiler):
    """Add hash validation and profiling data to a result dict."""
    if validate:
        hash_start = time.perf_counter()
        result["output_hash"] = _hash_dataset(ds)
        result["hash_time_sec"] = round(time.perf_counter() - hash_start, 3)

    if profile and profiler is not None:
        import io
        import pstats

        s = io.StringIO()
        ps = pstats.Stats(profiler, stream=s).sort_stats("cumulative")
        ps.print_stats(30)
        result["profile_top30"] = s.getvalue()


def _driver_cpu_snapshot():
    """Return (wall_clock, driver_cpu_sec) tuple at this instant.

    driver_cpu_sec = user+system CPU of the driver process only. Ray
    workers are separate OS processes so their CPU is excluded — this is
    a clean proxy for scheduler overhead.
    """
    import resource as _resource
    ru = _resource.getrusage(_resource.RUSAGE_SELF)
    return time.perf_counter(), ru.ru_utime + ru.ru_stime


def _record_cpu(result, cpu_start_wall, cpu_start_cpu, cpu_end_wall, cpu_end_cpu):
    """Attach driver-CPU and efficiency metrics to a workload result dict."""
    wall = cpu_end_wall - cpu_start_wall
    cpu = cpu_end_cpu - cpu_start_cpu
    result["driver_cpu_sec"] = round(cpu, 3)
    result["driver_cpu_per_wall"] = round(cpu / wall, 3) if wall > 0 else 0
    # Efficiency = useful work per unit of scheduler CPU. Optimizations that
    # burn CPU for no throughput gain collapse this value. Computed here so
    # both the gate and the user-facing score can refer to it.
    tp = result.get("throughput_blocks_per_sec", 0)
    cpu_per_wall = cpu / wall if wall > 0 else 0
    result["efficiency_blocks_per_core_sec"] = round(tp / cpu_per_wall, 2) if cpu_per_wall > 0 else 0


# ---------------------------------------------------------------------------
# Workload 1: Synthetic heterogeneous task-pool chain
# ---------------------------------------------------------------------------

def run_synthetic(
    num_rows: int,
    num_blocks: int,
    range_offset: int = 0,
    validate: bool = False,
    profile: bool = False,
) -> dict:
    """Run synthetic heterogeneous task-pool benchmark.

    Pipeline (depth=6, all task-pool, varied batch sizes, each stage applies a
    cheap but detectable transform to the `id` column):
      range -> [offset] -> map_batches(id*2+1, batch=4096)
            -> filter(id % 3 == 0)
            -> map_batches(id ^ 0xDEADBEEF, batch=2048)
            -> map_batches(id + 7, batch=1024)
            -> map_batches(id * 31, batch=8192)
            -> map_batches(id & 0x7FFFFFFF, batch=4096)
            -> materialize

    Every stage mutates the output: if any operator is erroneously skipped or
    reordered, the baseline hash will not match. The varied batch sizes still
    create different per-operator throughput for the ranker, and all
    operations are O(microseconds) so the scheduler remains the bottleneck.
    """
    import ray
    import ray.data

    # Cheap per-stage transforms. Each one is a vectorized Arrow/pandas
    # operation on the ``id`` column — microseconds per batch — so the
    # workload stays scheduler-bound, but every stage contributes to the
    # output hash.
    def mul_add(batch):
        batch["id"] = batch["id"] * 2 + 1
        return batch

    def filter_third(row):
        return row["id"] % 3 == 0

    def xor_const(batch):
        batch["id"] = batch["id"] ^ 0xDEADBEEF
        return batch

    def add_const(batch):
        batch["id"] = batch["id"] + 7
        return batch

    def mul_const(batch):
        batch["id"] = batch["id"] * 31
        return batch

    def mask_low(batch):
        batch["id"] = batch["id"] & 0x7FFFFFFF
        return batch

    profiler = None
    if profile:
        import cProfile
        profiler = cProfile.Profile()
        profiler.enable()

    cpu_w0, cpu_c0 = _driver_cpu_snapshot()
    start = time.perf_counter()

    ds = ray.data.range(num_rows, override_num_blocks=num_blocks)

    # Apply offset if specified (creates different data for public/private)
    if range_offset > 0:
        ds = ds.map_batches(
            lambda batch: {"id": batch["id"] + range_offset}, batch_format="pandas"
        )

    # Heterogeneous operator chain with varied batch sizes and real transforms
    ds = ds.map_batches(mul_add,   batch_size=4096)   # op 1: medium batches
    ds = ds.filter(filter_third)                       # op 2: drops ~2/3 of rows
    ds = ds.map_batches(xor_const, batch_size=2048)   # op 3: small batches (more tasks)
    ds = ds.map_batches(add_const, batch_size=1024)   # op 4: smallest (most tasks)
    ds = ds.map_batches(mul_const, batch_size=8192)   # op 5: large batches (fewer tasks)
    ds = ds.map_batches(mask_low,  batch_size=4096)   # op 6: medium, clamps to int32 range

    ds = ds.materialize()
    wall_time = time.perf_counter() - start
    cpu_w1, cpu_c1 = _driver_cpu_snapshot()

    if profile:
        profiler.disable()

    result = {
        "workload": "synthetic",
        "wall_time_sec": round(wall_time, 3),
        "num_blocks": num_blocks,
        "num_rows": num_rows,
        "depth": 6,
        "throughput_blocks_per_sec": round(num_blocks / wall_time, 2),
        "throughput_rows_per_sec": round(num_rows / wall_time, 2),
        "operators": _collect_op_stats(ds),
    }

    _record_cpu(result, cpu_w0, cpu_c0, cpu_w1, cpu_c1)
    _validate_and_profile(ds, result, validate, profile, profiler)
    return result


# ---------------------------------------------------------------------------
# Workload 2: Mixed task+actor pipeline
# ---------------------------------------------------------------------------

# Dummy model size (10 MB) — simulates model weight in actor memory.
# Kept small to avoid overwhelming object store on machines with limited /dev/shm.
# The scheduling overhead is identical regardless of model size.
_DUMMY_MODEL_SIZE = 10 * 1024 * 1024


class _InferenceActor:
    """Simulates a model inference actor.

    Loads a dummy model on init (10 MB numpy array via object ref), then
    applies a cheap but detectable transform to each batch. Any skipped
    invocation changes the output hash.
    """

    def __init__(self, model_ref):
        import ray as _ray
        self.model = _ray.get(model_ref)

    def __call__(self, batch):
        batch = batch.copy()
        # Detectable transform on the id column so hash mismatches if this
        # operator is skipped. (10 MB model is only for memory-pressure
        # realism; the transform itself is the correctness signal.)
        batch["id"] = batch["id"] * 3 + 11
        return batch


def run_mixed_pipeline(
    num_rows: int,
    num_blocks: int,
    num_actors: int = 16,
    range_offset: int = 0,
    validate: bool = False,
    profile: bool = False,
) -> dict:
    """Run mixed task+actor pipeline benchmark.

    Pipeline (depth=4, mixed task-pool + actor-pool, no I/O; every stage
    applies a detectable transform):
      range(num_rows, num_blocks)
        -> map_batches(id XOR 0x1234, tasks, batch_size=4096)
        -> map_batches(_InferenceActor: id*3+11, actors, pool=num_actors,
                        batch_size=2048)
        -> map_batches(id + 42, tasks, batch_size=4096)
        -> materialize

    Uses ray.data.range() instead of parquet to eliminate I/O variance.
    Each stage's transform is vectorized and microseconds per batch, so the
    scheduler remains the bottleneck. Any skipped operator changes the
    output hash.

    Exercises:
      - Actor pool scheduling (refresh_state, autoscaler)
      - Mixed task/actor dispatch decisions in the ranker
      - Asymmetric backpressure (fast task-pool source → actor-pool bottleneck)
    """
    import numpy
    import ray
    import ray.data

    def xor_preprocess(batch):
        batch["id"] = batch["id"] ^ 0x1234
        return batch

    def add_postprocess(batch):
        batch["id"] = batch["id"] + 42
        return batch

    profiler = None
    if profile:
        import cProfile
        profiler = cProfile.Profile()
        profiler.enable()

    # Put the dummy model in object store
    dummy_model = numpy.zeros(_DUMMY_MODEL_SIZE, dtype=numpy.int8)
    model_ref = ray.put(dummy_model)
    del dummy_model

    cpu_w0, cpu_c0 = _driver_cpu_snapshot()
    start = time.perf_counter()

    ds = ray.data.range(num_rows, override_num_blocks=num_blocks)

    if range_offset > 0:
        ds = ds.map_batches(
            lambda batch: {"id": batch["id"] + range_offset}, batch_format="pandas"
        )

    ds = ds.map_batches(xor_preprocess, batch_size=4096)
    ds = ds.map_batches(
        _InferenceActor,
        fn_constructor_args=[model_ref],
        batch_format="pandas",
        batch_size=2048,
        compute=ray.data.ActorPoolStrategy(
            min_size=num_actors,
            max_size=num_actors,
        ),
    )
    ds = ds.map_batches(add_postprocess, batch_size=4096)

    ds = ds.materialize()
    wall_time = time.perf_counter() - start
    cpu_w1, cpu_c1 = _driver_cpu_snapshot()

    if profile:
        profiler.disable()

    result = {
        "workload": "mixed_pipeline",
        "wall_time_sec": round(wall_time, 3),
        "num_blocks": num_blocks,
        "num_rows": num_rows,
        "num_actors": num_actors,
        "depth": 4,
        "throughput_blocks_per_sec": round(num_blocks / wall_time, 2),
        "throughput_rows_per_sec": round(num_rows / wall_time, 2),
        "operators": _collect_op_stats(ds),
    }

    _record_cpu(result, cpu_w0, cpu_c0, cpu_w1, cpu_c1)
    _validate_and_profile(ds, result, validate, profile, profiler)
    return result


# ---------------------------------------------------------------------------
# Workload 3: Medium-task stream (scheduler-CPU gate sensor)
# ---------------------------------------------------------------------------

def run_medium_tasks(
    num_rows: int,
    num_blocks: int,
    sleep_per_task: float = 0.05,
    range_offset: int = 0,
    validate: bool = False,
    profile: bool = False,
) -> dict:
    """Tasks ~50ms each — representative of a production map-transform stage.

    Primary purpose: exercise the scheduler when tasks are significantly
    longer than microseconds but short enough that scheduler latency still
    matters. This is the regime where ray.wait timeout choice has the
    largest legitimate effect on throughput — polling faster helps, polling
    too aggressively wastes CPU. Efficiency (throughput / scheduler CPU)
    cleanly measures the tradeoff.
    """
    import ray
    import ray.data

    def slow_map(batch, sleep=sleep_per_task):
        if sleep > 0:
            time.sleep(sleep)
        batch["id"] = batch["id"] * 2 + 1
        return batch

    profiler = None
    if profile:
        import cProfile
        profiler = cProfile.Profile()
        profiler.enable()

    cpu_w0, cpu_c0 = _driver_cpu_snapshot()
    start = time.perf_counter()

    ds = ray.data.range(num_rows, override_num_blocks=num_blocks)
    if range_offset > 0:
        ds = ds.map_batches(
            lambda batch: {"id": batch["id"] + range_offset}, batch_format="pandas"
        )
    ds = ds.map_batches(slow_map, batch_size=4096)
    ds = ds.materialize()

    wall_time = time.perf_counter() - start
    cpu_w1, cpu_c1 = _driver_cpu_snapshot()

    if profile:
        profiler.disable()

    result = {
        "workload": "medium_tasks",
        "wall_time_sec": round(wall_time, 3),
        "num_blocks": num_blocks,
        "num_rows": num_rows,
        "sleep_per_task": sleep_per_task,
        "throughput_blocks_per_sec": round(num_blocks / wall_time, 2),
        "throughput_rows_per_sec": round(num_rows / wall_time, 2),
        "operators": _collect_op_stats(ds),
    }

    _record_cpu(result, cpu_w0, cpu_c0, cpu_w1, cpu_c1)
    _validate_and_profile(ds, result, validate, profile, profiler)
    return result


# ---------------------------------------------------------------------------
# Workload 4: Long-task stream (idle-scheduler CPU-waste sensor)
# ---------------------------------------------------------------------------

def run_long_tasks(
    num_rows: int,
    num_blocks: int,
    sleep_per_task: float = 0.5,
    range_offset: int = 0,
    validate: bool = False,
    profile: bool = False,
) -> dict:
    """Tasks ~500ms each — scheduler is idle most of the time.

    Primary purpose: detect scheduler-CPU waste. Throughput here is
    time-bounded by the sleep (workers are the bottleneck, not the
    scheduler), so throughput alone can't distinguish a good scheduler
    from a busy-spinning one. Driver CPU / wall-clock does — this is the
    workload where the efficiency gate has the most discriminating power.
    """
    import ray
    import ray.data

    def slow_map(batch, sleep=sleep_per_task):
        if sleep > 0:
            time.sleep(sleep)
        batch["id"] = batch["id"] * 2 + 1
        return batch

    profiler = None
    if profile:
        import cProfile
        profiler = cProfile.Profile()
        profiler.enable()

    cpu_w0, cpu_c0 = _driver_cpu_snapshot()
    start = time.perf_counter()

    ds = ray.data.range(num_rows, override_num_blocks=num_blocks)
    if range_offset > 0:
        ds = ds.map_batches(
            lambda batch: {"id": batch["id"] + range_offset}, batch_format="pandas"
        )
    ds = ds.map_batches(slow_map, batch_size=4096)
    ds = ds.materialize()

    wall_time = time.perf_counter() - start
    cpu_w1, cpu_c1 = _driver_cpu_snapshot()

    if profile:
        profiler.disable()

    result = {
        "workload": "long_tasks",
        "wall_time_sec": round(wall_time, 3),
        "num_blocks": num_blocks,
        "num_rows": num_rows,
        "sleep_per_task": sleep_per_task,
        "throughput_blocks_per_sec": round(num_blocks / wall_time, 2),
        "throughput_rows_per_sec": round(num_rows / wall_time, 2),
        "operators": _collect_op_stats(ds),
    }

    _record_cpu(result, cpu_w0, cpu_c0, cpu_w1, cpu_c1)
    _validate_and_profile(ds, result, validate, profile, profiler)
    return result


# ---------------------------------------------------------------------------
# Workload 5: Actor-pool under plasma pressure
# ---------------------------------------------------------------------------
#
# Exercises an actor-pool stage under a tight object-store budget. Combines:
#   - autoscaling actor pool with large per-actor plasma objects (via ray.put
#     inside __init__ so each actor's plasma entry is distinct),
#   - per-task sleep that makes scheduling-step duration meaningful,
#   - output-batch padding so pending-output plasma pressure builds up during
#     the run,
#   - tight `object_store_memory` + `automatic_object_spilling_enabled=False`
#     so Ray's budget allocator is the dominant throttle rather than the
#     spill release valve.
#
# This is the regime that triggered the reviewer-reported stall on a
# 100-node / SF=1000 cluster: actor ops' `incremental_resource_usage()` is
# all zeros, so M5's `on_task_dispatched` hook never decrements the op's
# `object_store_memory` budget within a scheduling step. The op over-commits
# plasma until the next `update_usages()` boundary, and if plasma has no
# room to grow (tight /dev/shm + spilling disabled) the pipeline stalls.
#
# Correctness signal: the workload samples per-op `obj_store_mem_used`
# continuously through the run and reports `peak_op_obj_store_usage_ratio`
# = peak / budget. Pristine and M6+fix stay ≤ 1.0; buggy M5 exceeds 1.0.
# The ratio is the stable cross-host indicator — hard stall vs "just
# over budget" depends on /dev/shm, but the over-budget fact itself is
# a deterministic consequence of the bug and is visible on any host.


def run_actor_backpressure(
    num_rows: int,
    num_blocks: int,
    sleep_ms: int = 50,
    per_actor_model_bytes: int = 100 * 1024 * 1024,
    output_padding_bytes_per_row: int = 50 * 1024,
    concurrency_min: int = 1,
    concurrency_max: int = 50,
    object_store_gb: float = 6.0,
    num_cpus: int = 32,
    batch_size: int = 10_000,
    sample_interval_s: float = 0.5,
    plasma_directory: str = "/tmp/ray-bench-plasma",
    validate: bool = False,
    profile: bool = False,
) -> dict:
    """Actor-pool workload under plasma pressure; tests the M5 budget fix.

    Plasma backing: ``plasma_directory`` is passed as ``_plasma_directory``
    to ``ray.init`` so that plasma is always disk-backed (default
    ``/tmp/ray-bench-plasma``). Without this override Ray uses
    ``/dev/shm`` if it is large enough, which makes plasma IO fast and
    masks the backpressure dynamics we want to measure: pristine wall
    time on the same logical workload was ~580 s on a host with
    constrained ``/dev/shm`` (plasma in /tmp on disk) and ~230 s on a
    host with abundant ``/dev/shm`` (plasma in shared memory). The
    optimization gain is the same in both regimes but the absolute
    numbers diverge, so this knob is the single difference between
    "reproduces the original measurement" and "doesn't".
    """
    import logging
    import threading

    import ray
    import ray.data

    # Shut down any pre-existing cluster (the driver's main() may have
    # already called ray.init() with defaults) and re-init with the tight
    # constraints this workload requires.
    if ray.is_initialized():
        ray.shutdown()
    os.makedirs(plasma_directory, exist_ok=True)
    ray.init(
        object_store_memory=int(object_store_gb * 1024**3),
        num_cpus=num_cpus,
        _plasma_directory=plasma_directory,
        _system_config={
            "automatic_object_spilling_enabled": False,
        },
    )

    # Capture backpressure-related log messages from Ray Data so the
    # workload can verify backpressure was actually firing. If pristine
    # finishes without any backpressure log line, the workload sizing
    # is too small to exercise the path the optimizations target — the
    # measurement is invalid.
    backpressure_log_lines: list = []

    class _BackpressureCapture(logging.Handler):
        def emit(self, record):
            try:
                msg = self.format(record)
            except Exception:
                return
            lower = msg.lower()
            if "backpressure" in lower or "queue is full" in lower:
                backpressure_log_lines.append(msg)

    backpressure_handler = _BackpressureCapture()
    backpressure_handler.setLevel(logging.DEBUG)
    data_logger = logging.getLogger("ray.data")
    data_logger.addHandler(backpressure_handler)
    data_logger_prev_level = data_logger.level
    data_logger.setLevel(logging.DEBUG)

    profiler = None
    if profile:
        import cProfile
        profiler = cProfile.Profile()
        profiler.enable()

    class IncrementBatch:
        """Per-actor unique plasma entry + per-batch padded output."""

        def __init__(self, per_actor_model_bytes, sleep_ms, padding_bytes_per_row):
            import numpy as _np
            # Each actor's __init__ puts its own large object into plasma.
            # On a single-node cluster this forces N distinct plasma
            # entries (one per actor), matching the multi-node regime
            # where each node's object store holds its own model copy.
            if per_actor_model_bytes > 0:
                self._unique = ray.put(
                    _np.zeros(per_actor_model_bytes, dtype=_np.int8)
                )
                self.model = ray.get(self._unique)
            self.sleep_ms = sleep_ms
            self.padding_bytes_per_row = padding_bytes_per_row

        def __call__(self, batch):
            import numpy as _np
            if self.sleep_ms > 0:
                time.sleep(self.sleep_ms / 1000.0)
            batch["column00"] = batch["column00"] + 1
            if self.padding_bytes_per_row > 0:
                n = len(batch["column00"])
                batch["padding"] = _np.zeros(
                    (n, self.padding_bytes_per_row), dtype=_np.int8
                )
            return batch

    def to_column00(batch):
        # `ray.data.range()` in numpy batch_format already yields a numpy
        # array under `id`; rename it to match IncrementBatch's contract.
        return {"column00": batch["id"]}

    def dummy_write(batch):
        # Return column00 (not batch size). Row content is
        # deterministic under Ray Data's row-content guarantee; batch
        # size is not (FIFOBundleQueue + streaming completion order can
        # shift batch boundaries across runs with preserve_order=False).
        # Hashing column00 lets _hash_dataset verify real correctness.
        return {"column00": batch["column00"]}

    # --- Over-allocation sampler ----------------------------------------
    # Correctness signal: at every sample, for each op, is
    #   _op_running_usages[op].object_store_memory <= get_allocation(op).object_store_memory ?
    # These are the exact numbers Ray Data prints in its DEBUG progress
    # log ("Resources: ... object store" vs "alloc=..."). The allocation
    # INCLUDES the allocator's dynamic borrow from other ops' slack, so
    # a ratio > 1.0 is a genuine over-commit — not legitimate borrowing.
    # Expected: pristine and M6+AltB stay ≤ 1.0; M5/M6 without the fix
    # can exceed 1.0 (actor op over-commits because on_task_dispatched
    # can't see plasma).
    sampler_state = {
        "stop": False,
        "peak_used_bytes": 0,
        "peak_alloc_bytes": 0,
        "peak_ratio": 0.0,
        "num_samples": 0,
    }

    def sample_peak_usage():
        while not sampler_state["stop"]:
            try:
                rm = sampler_state.get("resource_manager")
                ops = sampler_state.get("ops", [])
                if rm is not None and ops:
                    alloc_api = rm._op_resource_allocator
                    for op in ops:
                        usage = rm._op_running_usages.get(op)
                        alloc = (
                            alloc_api.get_allocation(op)
                            if alloc_api is not None
                            else None
                        )
                        if usage is None or alloc is None:
                            continue
                        used = usage.object_store_memory or 0
                        a = alloc.object_store_memory or 0
                        if a > 0:
                            ratio = used / a
                            if ratio > sampler_state["peak_ratio"]:
                                sampler_state["peak_ratio"] = ratio
                                sampler_state["peak_used_bytes"] = used
                                sampler_state["peak_alloc_bytes"] = a
                    sampler_state["num_samples"] += 1
            except Exception:
                pass  # sampler must never crash the run
            time.sleep(sample_interval_s)

    sampler = threading.Thread(target=sample_peak_usage, daemon=True)
    sampler.start()

    cpu_w0, cpu_c0 = _driver_cpu_snapshot()
    start = time.perf_counter()

    ds = ray.data.range(num_rows, override_num_blocks=num_blocks)
    ds = ds.map_batches(to_column00, batch_format="numpy", batch_size=batch_size)
    ds = ds.map_batches(
        IncrementBatch,
        fn_constructor_args=[
            per_actor_model_bytes, sleep_ms, output_padding_bytes_per_row
        ],
        batch_format="numpy",
        batch_size=batch_size,
        concurrency=(concurrency_min, concurrency_max),
    )
    ds = ds.map_batches(dummy_write, batch_format="numpy", batch_size=batch_size)

    # Capture the StreamingExecutor instance via a scoped monkey-patch
    # of its __init__. Ray Data's ExecutionPlan doesn't expose the
    # executor as an attribute after creation, so there's no clean way
    # to grab it from the dataset object. We install a one-shot wrapper,
    # let iter_internal_ref_bundles create the executor, then
    # restore the original __init__.
    from ray.data._internal.execution.streaming_executor import (
        StreamingExecutor as _SE,
    )
    _orig_init = _SE.__init__

    def _capturing_init(self, *args, **kwargs):
        _orig_init(self, *args, **kwargs)
        if sampler_state.get("executor") is None:
            sampler_state["executor"] = self

    _SE.__init__ = _capturing_init

    def _hook_sampler_once():
        # Poll for the captured executor, then publish the resource
        # manager + ops so the sampler can call get_allocation(op).
        import time as _t
        deadline = _t.time() + 10.0
        while _t.time() < deadline and sampler_state.get("ops") is None:
            _t.sleep(0.1)
            exec_ = sampler_state.get("executor")
            if exec_ is None or not hasattr(exec_, "_resource_manager"):
                continue
            try:
                rm = exec_._resource_manager
                sampler_state["resource_manager"] = rm
                sampler_state["ops"] = list(rm._topology.keys())
            except Exception:
                pass

    hook_thread = threading.Thread(target=_hook_sampler_once, daemon=True)
    hook_thread.start()

    total_rows = 0
    for ref_bundle in ds.iter_internal_ref_bundles():
        for _block_ref, meta in ref_bundle.blocks:
            total_rows += meta.num_rows or 0

    wall_time = time.perf_counter() - start
    cpu_w1, cpu_c1 = _driver_cpu_snapshot()

    sampler_state["stop"] = True
    sampler.join(timeout=2.0)
    # Restore the original StreamingExecutor.__init__ so we don't leak
    # the monkey-patch into other workloads or subsequent runs.
    _SE.__init__ = _orig_init
    # Restore the data logger to its prior state.
    data_logger.removeHandler(backpressure_handler)
    data_logger.setLevel(data_logger_prev_level)

    if profile:
        profiler.disable()

    driver_cpu_per_wall = (
        (cpu_c1 - cpu_c0) / (cpu_w1 - cpu_w0) if cpu_w1 > cpu_w0 else 0.0
    )

    result = {
        "workload": "actor_backpressure",
        "wall_time_sec": round(wall_time, 3),
        "num_blocks": num_blocks,
        "num_rows": num_rows,
        "concurrency": [concurrency_min, concurrency_max],
        "sleep_ms": sleep_ms,
        "per_actor_model_bytes": per_actor_model_bytes,
        "output_padding_bytes_per_row": output_padding_bytes_per_row,
        "object_store_gb": object_store_gb,
        "num_cpus": num_cpus,
        "throughput_blocks_per_sec": round(num_blocks / wall_time, 2),
        "throughput_rows_per_sec": round(num_rows / wall_time, 2),
        # Correctness signal: peak per-op (object_store_used / allocation).
        # allocation is the dynamic total including borrowing; ratio > 1.0
        # means the op genuinely over-committed plasma at some point.
        "peak_op_obj_store_over_alloc_ratio": round(sampler_state["peak_ratio"], 4),
        "peak_op_obj_store_used_mb": round(
            sampler_state["peak_used_bytes"] / (1024**2), 2
        ),
        "peak_op_obj_store_alloc_mb": round(
            sampler_state["peak_alloc_bytes"] / (1024**2), 2
        ),
        "num_sampler_reads": sampler_state["num_samples"],
        # Backpressure verification. The workload is only meaningful for
        # measuring the M5 budget fix if backpressure was actually firing
        # during the run. We assert this two ways:
        #   * `backpressure_log_count` — number of log lines from
        #     `ray.data` containing "backpressure" or "queue is full".
        #     Zero means the workload completed without ever triggering
        #     a backpressure-related code path.
        #   * `backpressure_pressure_ratio` — peak observed
        #     `used / cap` ratio. Close to 1.0 means the per-op cap was
        #     binding (backpressure was actively constraining).
        # If both are zero, the workload sizing is too small to exercise
        # the path the optimizations target and the measurement should
        # not be used.
        "backpressure_log_count": len(backpressure_log_lines),
        "plasma_directory": plasma_directory,
    }

    _record_cpu(result, cpu_w0, cpu_c0, cpu_w1, cpu_c1)
    _validate_and_profile(ds, result, validate, profile, profiler)
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Ray Data scheduler benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--workload",
        choices=[
            "synthetic",
            "mixed_pipeline",
            "medium_tasks",
            "long_tasks",
            "actor_backpressure",
            "all",
        ],
        required=True,
        help="Which workload(s) to run",
    )
    parser.add_argument(
        "--artifact-dir",
        default=".",
        help="Path to the artifact root directory",
    )
    parser.add_argument(
        "--config",
        help="Path to workload_config.json (relative to artifact-dir or absolute)",
    )
    # Synthetic overrides
    parser.add_argument("--num-rows", type=int, help="Override: number of rows")
    parser.add_argument("--num-blocks", type=int, help="Override: number of blocks")
    parser.add_argument("--range-offset", type=int, default=0, help="Override: range offset")
    # Mixed pipeline overrides
    parser.add_argument("--num-actors", type=int, help="Override: number of inference actors")
    # Flags
    parser.add_argument("--validate", action="store_true", help="Compute output hash for validation")
    parser.add_argument("--profile", action="store_true", help="Enable cProfile profiling")
    parser.add_argument("--verbose", action="store_true", help="Print progress (off by default)")
    parser.add_argument(
        "--baseline-file",
        help="Path to baseline JSON for validation. If provided, checks output hash matches.",
    )

    args = parser.parse_args()
    artifact_dir = os.path.abspath(args.artifact_dir)

    # Load config if provided
    config = {}
    if args.config:
        config_path = args.config
        if not os.path.isabs(config_path):
            config_path = os.path.join(artifact_dir, config_path)
        with open(config_path) as f:
            config = json.load(f)

    # Suppress Ray progress bars unless verbose
    if not args.verbose:
        os.environ.setdefault("RAY_DATA_DISABLE_PROGRESS_BARS", "1")

    # Allow slow storage fallback when /dev/shm is undersized
    os.environ.setdefault("RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE", "1")

    import ray

    if not ray.is_initialized():
        ray.init()

    results = []

    if args.workload in ("synthetic", "all"):
        syn_config = config.get("synthetic", {})
        num_rows = args.num_rows or syn_config.get("num_rows", 100000000)
        num_blocks = args.num_blocks or syn_config.get("num_blocks", 10000)
        range_offset = args.range_offset or syn_config.get("range_offset", 0)

        syn_result = run_synthetic(
            num_rows=num_rows,
            num_blocks=num_blocks,
            range_offset=range_offset,
            validate=args.validate,
            profile=args.profile,
        )
        results.append(syn_result)

    if args.workload in ("mixed_pipeline", "all"):
        mix_config = config.get("mixed_pipeline", {})
        # Mixed pipeline uses its own num_rows/num_blocks if specified in config,
        # otherwise falls back to the same CLI overrides as synthetic
        mix_num_rows = mix_config.get("num_rows", args.num_rows or 100000000)
        mix_num_blocks = mix_config.get("num_blocks", args.num_blocks or 10000)
        mix_range_offset = mix_config.get("range_offset", args.range_offset or 0)
        num_actors = args.num_actors or mix_config.get("num_actors", 16)

        mix_result = run_mixed_pipeline(
            num_rows=mix_num_rows,
            num_blocks=mix_num_blocks,
            num_actors=num_actors,
            range_offset=mix_range_offset,
            validate=args.validate,
            profile=args.profile,
        )
        results.append(mix_result)

    if args.workload in ("medium_tasks", "all"):
        med_config = config.get("medium_tasks", {})
        med_result = run_medium_tasks(
            num_rows=med_config.get("num_rows", 2000000),
            num_blocks=med_config.get("num_blocks", 500),
            sleep_per_task=med_config.get("sleep_per_task", 0.05),
            range_offset=med_config.get("range_offset", 0),
            validate=args.validate,
            profile=args.profile,
        )
        results.append(med_result)

    if args.workload in ("long_tasks", "all"):
        long_config = config.get("long_tasks", {})
        long_result = run_long_tasks(
            num_rows=long_config.get("num_rows", 500000),
            num_blocks=long_config.get("num_blocks", 50),
            sleep_per_task=long_config.get("sleep_per_task", 0.5),
            range_offset=long_config.get("range_offset", 0),
            validate=args.validate,
            profile=args.profile,
        )
        results.append(long_result)

    if args.workload in ("actor_backpressure", "all"):
        bp_config = config.get("actor_backpressure", {})
        bp_result = run_actor_backpressure(
            num_rows=bp_config.get("num_rows", 20_000_000),
            num_blocks=bp_config.get("num_blocks", 5000),
            sleep_ms=bp_config.get("sleep_ms", 50),
            per_actor_model_bytes=bp_config.get(
                "per_actor_model_bytes", 100 * 1024 * 1024
            ),
            output_padding_bytes_per_row=bp_config.get(
                "output_padding_bytes_per_row", 50 * 1024
            ),
            concurrency_min=bp_config.get("concurrency_min", 1),
            concurrency_max=bp_config.get("concurrency_max", 50),
            object_store_gb=bp_config.get("object_store_gb", 6.0),
            num_cpus=bp_config.get("num_cpus", 32),
            batch_size=bp_config.get("batch_size", 10_000),
            validate=args.validate,
            profile=args.profile,
        )
        results.append(bp_result)

    # Compute composite score.
    #
    # Score is geomean of throughput across the "score workloads" only:
    # synthetic, mixed_pipeline, and medium_tasks. long_tasks is
    # excluded because its throughput is time-bounded by the in-task
    # sleep (~500 ms × 50 blocks bounds it near 10-15 blocks/sec
    # regardless of scheduler quality) — including it would dilute the
    # composite without adding information. long_tasks is still measured
    # and still drives gate 4 (scheduler CPU efficiency); it just isn't
    # part of the score.
    SCORE_WORKLOADS = {"synthetic", "mixed_pipeline", "medium_tasks"}
    throughputs = [
        r["throughput_blocks_per_sec"]
        for r in results
        if r.get("workload") in SCORE_WORKLOADS
        and "error" not in r
        and r.get("throughput_blocks_per_sec", 0) > 0
    ]
    if len(throughputs) == 0:
        composite_score = 0.0
    elif len(throughputs) == 1:
        composite_score = throughputs[0]
    else:
        composite_score = math.exp(sum(math.log(t) for t in throughputs) / len(throughputs))

    output = {
        "score": round(composite_score, 2),
        "result": {
            "type": "success",
            "workloads": results,
            "composite_throughput_blocks_per_sec": round(composite_score, 2),
        },
    }

    # Validate against baseline if provided
    if args.baseline_file and args.validate:
        baseline_path = args.baseline_file
        if not os.path.isabs(baseline_path):
            baseline_path = os.path.join(artifact_dir, baseline_path)

        try:
            with open(baseline_path) as f:
                baseline = json.load(f)

            baseline_workloads = {w["workload"]: w for w in baseline.get("result", {}).get("workloads", [])}
            for r in results:
                wl_name = r["workload"]
                if wl_name in baseline_workloads:
                    expected_hash = baseline_workloads[wl_name].get("output_hash")
                    actual_hash = r.get("output_hash")
                    if expected_hash and actual_hash and expected_hash != actual_hash:
                        output = {
                            "score": -1e309,
                            "result": {
                                "type": "failure",
                                "reason": f"Output hash mismatch for {wl_name}: expected {expected_hash}, got {actual_hash}",
                                "workloads": results,
                            },
                        }
                        break
        except Exception as e:
            output = {
                "score": -1e309,
                "result": {
                    "type": "failure",
                    "reason": f"Failed to load baseline: {e}",
                    "workloads": results,
                },
            }

    ray.shutdown()
    print(json.dumps(output))


if __name__ == "__main__":
    main()
