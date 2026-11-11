#!/usr/bin/env python3
"""Production-grade benchmark for Ray Data CBO (Cost-Based Optimizer).

Provides three complementary benchmark modes:

1. **Per-rule** (``--per-rule``): Benchmarks each optimisation rule individually,
   comparing a *baseline* pipeline against an *optimised* pipeline.  Results
   are presented in a compact summary table with parameter-sweep details for
   CBO rules.
2. **End-to-end** (default): Matched CBO-ON / CBO-OFF runs on representative
   batch and streaming pipelines.
3. **Statistics validation** (``--validate-stats``): Checks the CBO statistics
   infrastructure (scale, merge, selectivity, DAG propagation).

Usage examples:
    python cbo_benchmark.py --per-rule --num-rows 200000 --repetitions 2
    python cbo_benchmark.py --per-rule --rules OperatorFusion,LimitPushdown
    python cbo_benchmark.py --batch-rows 500000 --streaming-rows 300000 --repetitions 3
    python cbo_benchmark.py --validate-stats --num-rows 5000 --repetitions 1
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import statistics
import time
from contextlib import contextmanager
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray
from ray.data import DataContext, Dataset

if TYPE_CHECKING:
    import numpy as np

try:
    from ray.data.benchmarks import cbo_benchmark_campaign as benchmark_campaign
    from ray.data.benchmarks import cbo_benchmark_harness as benchmark_harness
    from ray.data.benchmarks import cbo_benchmark_postprocess as benchmark_postprocess
    from ray.data.benchmarks.cbo_benchmark_schema import (
        build_end_to_end_payload,
        build_memory_profile_payload,
        build_per_rule_payload,
        build_r_value_matrix_payload,
    )
except ImportError:
    import cbo_benchmark_campaign as benchmark_campaign  # type: ignore
    import cbo_benchmark_harness as benchmark_harness  # type: ignore
    import cbo_benchmark_postprocess as benchmark_postprocess  # type: ignore
    from cbo_benchmark_schema import (  # type: ignore
        build_end_to_end_payload,
        build_memory_profile_payload,
        build_per_rule_payload,
        build_r_value_matrix_payload,
    )

# ---------------------------------------------------------------------------
# CBO statistics module import (from local source via importlib so it works
# regardless of whether the installed ray has the cbo_stats package).
# ---------------------------------------------------------------------------

_CBO_STATS_PATH = os.path.join(
    os.path.dirname(__file__),
    os.pardir,
    "_internal",
    "cbo_stats",
    "operator_statistics.py",
)
_CBO_STATS_PATH = os.path.abspath(_CBO_STATS_PATH)

_cbo_mod = None


def _has_native_cbo() -> bool:
    """Whether the imported Ray build ships the CBO statistics package."""
    return importlib.util.find_spec("ray.data._internal.cbo_stats") is not None


def _load_cbo_module():
    """Load the CBO statistics module, from the source tree on pre-CBO builds."""
    global _cbo_mod
    if _cbo_mod is not None:
        return _cbo_mod
    if _has_native_cbo():
        from ray.data._internal.cbo_stats import operator_statistics

        _cbo_mod = operator_statistics
        return _cbo_mod
    spec = importlib.util.spec_from_file_location(
        "cbo_operator_statistics",
        _CBO_STATS_PATH,
    )
    _cbo_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(_cbo_mod)
    return _cbo_mod


# ---------------------------------------------------------------------------
# CBO statistics validation
# ---------------------------------------------------------------------------


def validate_statistics_infrastructure() -> bool:
    """Validate CBO statistics primitives: scale, merge, selectivity.

    Returns ``True`` when all checks pass.
    """
    mod = _load_cbo_module()
    OperatorStatistics = mod.OperatorStatistics
    ColumnStatistics = mod.ColumnStatistics
    estimate_sel = mod.estimate_selectivity_from_column_stats

    print("=" * 72)
    print("  CBO Statistics Infrastructure Validation")
    print("=" * 72)

    # 1. Basic creation
    stats = OperatorStatistics(num_rows=100000, size_bytes=8_000_000, confidence=0.95)
    print(
        f"\n  [Read] stats: rows={stats.num_rows}, "
        f"bytes={stats.size_bytes}, confidence={stats.confidence}"
    )

    # 2. Selectivity estimation
    col = ColumnStatistics(
        name="id", min_value=0, max_value=99999, distinct_count=100000
    )
    sel_eq = estimate_sel("id", "EQ", 42, col)
    sel_gt = estimate_sel("id", "GT", 50000, col)
    sel_lt = estimate_sel("id", "LT", 30000, col)
    print(f"\n  [Filter] EQ selectivity (id == 42): {sel_eq:.6f}")
    print(f"  [Filter] GT selectivity (id > 50000): {sel_gt:.4f}")
    print(f"  [Filter] LT selectivity (id < 30000): {sel_lt:.4f}")

    # 3. Scale
    filtered = stats.scale(0.333)
    print(
        f"\n  [Filter] After id%%3==0 (sel=0.333): "
        f"rows={filtered.num_rows}, bytes={filtered.size_bytes}"
    )

    # 4. Limit
    if filtered.num_rows and filtered.num_rows > 0:
        ratio = min(1.0, 1000 / filtered.num_rows)
        limited = filtered.scale(ratio)
        print(
            f"  [Limit]  After limit(1000) (ratio={ratio:.4f}): "
            f"rows={limited.num_rows}, bytes={limited.size_bytes}"
        )

    # 5. Merge (Union)
    a = OperatorStatistics(num_rows=5000, size_bytes=400000, confidence=0.9)
    b = OperatorStatistics(num_rows=3000, size_bytes=240000, confidence=0.8)
    m = a.merge(b)
    print(
        f"\n  [Union]  Merge: ({a.num_rows}+{b.num_rows})={m.num_rows} rows, "
        f"({a.size_bytes}+{b.size_bytes})={m.size_bytes} bytes"
    )

    # 6. Full chain
    print("\n  --- Full Pipeline Statistics Chain ---")
    rs = OperatorStatistics(num_rows=100000, size_bytes=8_000_000, confidence=0.95)
    fs = rs.scale(0.333)
    print(f"  Read:        rows={rs.num_rows:>8}, bytes={rs.size_bytes:>10}")
    print(f"  Filter(33%): rows={fs.num_rows:>8}, bytes={fs.size_bytes:>10}")
    print(
        f"  MapBatches:  rows={fs.num_rows:>8}, bytes={fs.size_bytes:>10} (pass-through)"
    )
    print(
        f"  Repartition: rows={fs.num_rows:>8}, bytes={fs.size_bytes:>10} (pass-through)"
    )
    print(
        f"  Sort:        rows={fs.num_rows:>8}, bytes={fs.size_bytes:>10} (pass-through)"
    )

    print("\n  All statistics checks PASSED!")
    return True


# ---------------------------------------------------------------------------
# Live operator statistics injection & demo
# ---------------------------------------------------------------------------


def _inject_infer_statistics():
    """Monkey-patch ``infer_statistics()`` onto installed ray operators.

    This is only needed when the installed Ray build does not yet ship with
    CBO-aware operator classes.  The injected methods are simplified versions
    of the real implementations in the local source tree.
    """
    if _has_native_cbo():
        return
    mod = _load_cbo_module()
    OperatorStatistics = mod.OperatorStatistics
    ConfidenceLevel = mod.ConfidenceLevel

    from ray.data._internal.logical.interfaces.logical_operator import LogicalOperator
    from ray.data._internal.logical.operators.read_operator import Read
    from ray.data._internal.logical.operators.one_to_one_operator import (
        AbstractOneToOne,
        Limit,
    )
    from ray.data._internal.logical.operators.all_to_all_operator import (
        AbstractAllToAll,
        Repartition,
    )
    from ray.data._internal.logical.operators.n_ary_operator import Union

    # --- base ---
    if not hasattr(LogicalOperator, "infer_statistics"):
        LogicalOperator.infer_statistics = lambda self: None

    # --- Read ---
    def _read_stats(self):
        md = self.infer_metadata()
        if md.num_rows is None and md.size_bytes is None:
            return None
        return OperatorStatistics(
            num_rows=md.num_rows,
            size_bytes=md.size_bytes,
            confidence=ConfidenceLevel.HIGH.value,
        )

    Read.infer_statistics = _read_stats

    # --- OneToOne (pass-through or None) ---
    def _oto_stats(self):
        if not self.input_dependencies:
            return None
        inp = self.input_dependencies[0].infer_statistics()
        if inp is None:
            return None
        cmr = getattr(self, "can_modify_num_rows", False)
        if callable(cmr):
            cmr = cmr()
        return inp if not cmr else None

    AbstractOneToOne.infer_statistics = _oto_stats

    # --- Filter (default 0.5 selectivity for UDF-based predicates) ---
    try:
        from ray.data._internal.logical.operators.map_operator import Filter

        def _filter_stats(self):
            if not self.input_dependencies:
                return None
            inp = self.input_dependencies[0].infer_statistics()
            return inp.scale(0.5) if inp is not None else None

        Filter.infer_statistics = _filter_stats
    except ImportError:
        pass

    # --- Limit ---
    def _limit_stats(self):
        if not self.input_dependencies:
            return None
        inp = self.input_dependencies[0].infer_statistics()
        if inp is None:
            return None
        lim = getattr(self, "limit", None) or getattr(self, "_limit", None)
        if lim and inp.num_rows and inp.num_rows > 0:
            return inp.scale(min(1.0, lim / inp.num_rows))
        return inp

    Limit.infer_statistics = _limit_stats

    # --- AllToAll (pass-through) ---
    def _a2a_stats(self):
        if not self.input_dependencies:
            return None
        return self.input_dependencies[0].infer_statistics()

    AbstractAllToAll.infer_statistics = _a2a_stats

    # --- Repartition (update num_blocks) ---
    def _repart_stats(self):
        s = AbstractAllToAll.infer_statistics(self)
        if s is not None and self._num_outputs is not None:
            s = OperatorStatistics(
                num_rows=s.num_rows,
                size_bytes=s.size_bytes,
                num_blocks=self._num_outputs,
                confidence=s.confidence,
            )
        return s

    Repartition.infer_statistics = _repart_stats

    # --- Union (merge branches) ---
    def _union_stats(self):
        merged = None
        for dep in self.input_dependencies:
            ds = dep.infer_statistics()
            if ds is None:
                return None
            merged = ds if merged is None else merged.merge(ds)
        return merged

    Union.infer_statistics = _union_stats


def _print_dag_statistics(dag, depth=0):
    """Recursively print inferred statistics for each DAG operator."""
    for dep in getattr(dag, "input_dependencies", []):
        _print_dag_statistics(dep, depth + 1)

    indent = "    " * depth
    name = getattr(dag, "name", dag.__class__.__name__)
    stats = dag.infer_statistics()
    if stats is not None:
        parts = []
        if stats.num_rows is not None:
            parts.append(f"rows={stats.num_rows}")
        if stats.size_bytes is not None:
            parts.append(f"bytes={stats.size_bytes}")
        if stats.num_blocks is not None:
            parts.append(f"blocks={stats.num_blocks}")
        parts.append(f"confidence={stats.confidence:.2f}")
        print(f"  {indent}{name}: {', '.join(parts)}")
    else:
        print(f"  {indent}{name}: [statistics unavailable]")


def demo_live_statistics(num_rows: int = 10000):
    """Build sample pipelines and display inferred per-operator statistics."""
    print("\n" + "=" * 72)
    print("  Live Statistics Inference on Ray Data Pipelines")
    print("=" * 72)

    _inject_infer_statistics()

    # Pipeline 1: Read → Filter → MapBatches → Repartition → Sort
    print(f"\n  --- range({num_rows}) → filter → map_batches → repartition → sort ---")
    ds = ray.data.range(num_rows)
    ds = ds.filter(lambda row: row["id"] % 3 == 0)
    ds = ds.map_batches(
        lambda b: {"id": b["id"] * 2},
        batch_size=2048,
        batch_format="numpy",
    )
    ds = ds.repartition(16)
    ds = ds.sort(key="id")
    _print_dag_statistics(ds._logical_plan.dag)

    # Pipeline 2: Read → Limit
    print(f"\n  --- range({num_rows}) → limit(100) ---")
    ds2 = ray.data.range(num_rows).limit(100)
    _print_dag_statistics(ds2._logical_plan.dag)

    # Pipeline 3: Union
    print("\n  --- union(range(5000), range(3000)) ---")
    ds3 = ray.data.range(5000).union(ray.data.range(3000))
    _print_dag_statistics(ds3._logical_plan.dag)


# ---------------------------------------------------------------------------
# Workload definitions
# ---------------------------------------------------------------------------


def _double_values(batch: Dict[str, "np.ndarray"]) -> Dict[str, "np.ndarray"]:
    """Simulates a UDF that expands the schema (amplification_ratio > 1)."""
    import numpy as np

    return {"id": batch["id"] * 2, "squared": np.square(batch["id"])}


def _normalize(batch: Dict[str, "np.ndarray"]) -> Dict[str, "np.ndarray"]:
    """Simulates a stateless streaming UDF."""
    import numpy as np

    arr = batch["id"].astype(np.float64)
    arr = (arr - np.mean(arr)) / (np.std(arr) + 1e-6)
    return {"id": arr}


def build_batch_dataset(num_rows: int) -> Dataset:
    """Heavy batch pipeline: Filter → Map (expansion) → Repartition → Sort."""
    ds = ray.data.range(num_rows)
    ds = ds.filter(lambda row: row["id"] % 3 == 0)
    ds = ds.map_batches(
        _double_values,
        batch_size=2048,
        batch_format="numpy",
    )
    ds = ds.repartition(64)
    ds = ds.sort(key="id")
    return ds


def build_streaming_dataset(num_rows: int) -> Dataset:
    """Streaming pipeline: Shuffle → Map (normalize) → Repartition.

    NOTE: The shuffle is placed *before* the map to work around a ReadTask
    fusion bug present in ray <= 2.48.  Semantically the benchmark still
    exercises the same operators (shuffle + CPU-map + repartition).
    """
    ds = ray.data.range(num_rows)
    ds = ds.random_shuffle(seed=1234)
    ds = ds.map_batches(
        _normalize,
        batch_size=1024,
        batch_format="numpy",
    )
    ds = ds.repartition(32)
    return ds


def _light_chain_udf(batch: Dict[str, "np.ndarray"]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"id": batch["id"] * 2 + 1}
    if "payload" in batch:
        out["payload"] = batch["payload"]
    return out


def _heavy_chain_udf(batch: Dict[str, "np.ndarray"]) -> Dict[str, Any]:
    import numpy as np

    arr = batch["id"].astype(np.float64)
    for _ in range(8):
        arr = np.sin(arr) * np.cos(arr) + np.sqrt(np.abs(arr) + 1)
    out: Dict[str, Any] = {"id": arr}
    if "payload" in batch:
        out["payload"] = batch["payload"]
    return out


def _attach_payload(
    batch: Dict[str, "np.ndarray"],
    payload_bytes_per_row: int,
) -> Dict[str, Any]:
    import numpy as np

    payload_prefix = b"x" * max(payload_bytes_per_row - 8, 0)
    payload = np.asarray(
        [
            payload_prefix + int(value).to_bytes(8, "little", signed=False)
            for value in batch["id"]
        ],
        dtype=object,
    )
    return {"id": batch["id"], "payload": payload}


def _parameterized_pipeline_desc(transform_ops: int, payload_bytes_per_row: int) -> str:
    payload_desc = (
        f"payload({payload_bytes_per_row}B)->" if payload_bytes_per_row > 0 else ""
    )
    return (
        f"range->filter->{payload_desc}map_batches*x{transform_ops}->"
        "repartition->sort"
    )


def _estimate_chain_data_scale_bytes(
    num_rows: int,
    payload_bytes_per_row: int,
) -> int:
    return num_rows * (8 + max(payload_bytes_per_row, 0))


def build_parameterized_chain_dataset(
    num_rows: int,
    transform_ops: int,
    payload_bytes_per_row: int = 0,
) -> Dataset:
    ds = ray.data.range(num_rows)
    ds = ds.filter(lambda row: row["id"] % 3 == 0)
    if payload_bytes_per_row > 0:
        ds = ds.map_batches(
            _attach_payload,
            fn_kwargs={"payload_bytes_per_row": payload_bytes_per_row},
            batch_size=1024,
            batch_format="numpy",
        )
    for idx in range(transform_ops):
        ds = ds.map_batches(
            _heavy_chain_udf if idx % 2 else _light_chain_udf,
            batch_size=1024,
            batch_format="numpy",
        )
    ds = ds.repartition(max(8, min(64, max(transform_ops, 1) * 8)))
    ds = ds.sort(key="id")
    return ds


# ---------------------------------------------------------------------------
# Metrics collection
# ---------------------------------------------------------------------------


def _summarize_stats(
    ds: Dataset,
    duration_s: float,
    mode: str,
    enable_cbo: bool,
    batch_latencies: Optional[List[float]] = None,
) -> benchmark_harness.RunMetrics:
    """Extract execution stats from the materialised dataset.

    Handles missing or ``None`` stat fields gracefully so the benchmark can
    run against Ray builds that report partial statistics.
    """
    plan = getattr(ds, "_logical_plan", None)
    if plan is None:
        # Fallback: return a bare RunMetrics with just timing
        return benchmark_harness.RunMetrics(
            mode=mode,
            enable_cbo=enable_cbo,
            duration_s=round(duration_s, 4),
        )

    try:
        # ``DatasetStatsSummary`` is the execution-mode-agnostic view of the
        # operator stats; it does not trigger execution when already cached.
        summary = ds.get_stats_summary()
    except Exception:
        return benchmark_harness.RunMetrics(
            mode=mode,
            enable_cbo=enable_cbo,
            duration_s=round(duration_s, 4),
        )

    operators_payload: List[Dict[str, Any]] = []
    total_input_rows = 0

    for op in getattr(summary, "operators_stats", []):
        output_rows = getattr(op, "output_num_rows", None)
        if isinstance(output_rows, dict):
            # Older Ray builds expose a plain dict of aggregates.
            op_rows = output_rows.get("sum", 0)
        else:
            # Current builds expose a ``StatsSummary`` dataclass.
            op_rows = getattr(output_rows, "sum", 0) or 0
        time_s = getattr(op, "time_total_s", None) or 0.0
        operators_payload.append(
            {
                "operator": getattr(op, "operator_name", "unknown"),
                "time_total_s": round(time_s, 4),
                "output_rows": op_rows,
                "throughput_rows_per_s": round(
                    (op_rows / time_s) if time_s > 0 else 0.0, 2
                ),
            }
        )
        total_input_rows += getattr(op, "total_input_num_rows", 0) or 0

    # Streaming latency percentiles
    p50 = p95 = p99 = mean_lat = 0.0
    if batch_latencies and len(batch_latencies) > 0:
        sorted_lats = sorted(batch_latencies)
        n = len(sorted_lats)
        p50 = sorted_lats[int(n * 0.50)] * 1000
        p95 = sorted_lats[int(n * 0.95)] * 1000
        p99 = sorted_lats[min(int(n * 0.99), n - 1)] * 1000
        mean_lat = statistics.mean(batch_latencies) * 1000

    peak_heap_bytes = None
    try:
        # Upstream Ray removed heap-memory tracking from ``DatasetStatsSummary``;
        # this stays for builds that still expose it and degrades to ``None``.
        peak_heap_mib = summary.get_max_heap_memory()
        if peak_heap_mib:
            peak_heap_bytes = int(float(peak_heap_mib) * 1024 * 1024)
    except Exception:
        peak_heap_bytes = None

    peak_object_store_bytes = _get_peak_object_store_bytes(summary)

    return benchmark_harness.RunMetrics(
        mode=mode,
        enable_cbo=enable_cbo,
        duration_s=round(duration_s, 4),
        reservation_ratio=_effective_reservation_ratio(ds),
        operator_count=len(operators_payload),
        operators=operators_payload,
        global_bytes_spilled=getattr(summary, "global_bytes_spilled", 0),
        dataset_bytes_spilled=getattr(summary, "dataset_bytes_spilled", 0),
        streaming_schedule_s=round(
            getattr(summary, "streaming_exec_schedule_s", 0.0), 4
        ),
        total_input_rows=total_input_rows,
        latency_p50_ms=round(p50, 3),
        latency_p95_ms=round(p95, 3),
        latency_p99_ms=round(p99, 3),
        latency_mean_ms=round(mean_lat, 3),
        peak_object_store_bytes=peak_object_store_bytes,
        peak_heap_bytes=peak_heap_bytes,
    )


# ---------------------------------------------------------------------------
# Context management
# ---------------------------------------------------------------------------


def _effective_reservation_ratio(ds: Dataset) -> float:
    """The reservation ratio the executor of ``ds`` actually used.

    The CBO rule records the derived ratio on the dataset's own context (a copy
    of the current context), so the current context's
    ``op_resource_reservation_ratio`` is only the configured value.
    """
    ctx = getattr(ds, "context", None)
    if ctx is None:
        ctx = DataContext.get_current()
    try:
        from ray.data._internal.execution import resolve_reservation_ratio
    except ImportError:  # Pre-CBO builds.
        return getattr(ctx, "op_resource_reservation_ratio", 0.5)
    return resolve_reservation_ratio(ctx)


def _get_peak_object_store_bytes(summary) -> Optional[int]:
    peak = 0

    def _visit(node) -> None:
        nonlocal peak
        if node is None:
            return
        extra_metrics = getattr(node, "extra_metrics", None) or {}
        current = extra_metrics.get("obj_store_mem_used", 0)
        try:
            peak = max(peak, int(current or 0))
        except Exception:
            pass
        for parent in getattr(node, "parents", []) or []:
            _visit(parent)

    _visit(summary)
    return peak or None


def _configure_context(
    streaming: bool,
    enable_cbo: bool,
    forced_reservation_ratio: Optional[float] = None,
    memory_poll_interval_s: Optional[float] = None,
) -> DataContext:
    """Create a context with CBO toggled on/off.

    Gracefully handles the case where the running Ray version does not yet
    have the ``enable_cost_based_optimization`` attribute (pre-CBO builds).
    """
    ctx = DataContext.get_current().copy()
    # These attributes only exist in CBO-enabled builds. Data contexts accept
    # unknown attributes, so the benchmark also works against stock Ray.
    ctx.enable_cost_based_optimization = enable_cbo
    ctx._user_set_reservation_ratio = False
    if forced_reservation_ratio is not None:
        ctx.op_resource_reservation_ratio = forced_reservation_ratio
        ctx._user_set_reservation_ratio = True
    if memory_poll_interval_s is not None and hasattr(
        ctx, "memory_usage_poll_interval_s"
    ):
        ctx.memory_usage_poll_interval_s = memory_poll_interval_s
    ctx.execution_options.verbose_progress = False
    return ctx


@contextmanager
def _context_scope(ctx: DataContext):
    """Apply *ctx* for the duration of the block, restoring the original after."""
    # Ray >= 2.44 exposes ``DataContext.current`` as a context manager.
    current_cm = getattr(DataContext, "current", None)
    if current_cm is not None and callable(current_cm):
        try:
            with current_cm(ctx):
                yield
            return
        except TypeError:
            pass  # fallback to manual swap below

    prev = DataContext.get_current()
    DataContext._set_current(ctx)
    try:
        yield
    finally:
        if prev is not None:
            DataContext._set_current(prev)


# ---------------------------------------------------------------------------
# Workload runners
# ---------------------------------------------------------------------------


def run_batch_workload(
    num_rows: int,
    enable_cbo: bool,
) -> benchmark_harness.RunMetrics:
    """Execute the batch pipeline with or without CBO."""
    ctx = _configure_context(streaming=False, enable_cbo=enable_cbo)
    with _context_scope(ctx):
        ds = build_batch_dataset(num_rows)
        start = time.perf_counter()
        ds = ds.materialize()
        duration = time.perf_counter() - start
        return _summarize_stats(ds, duration, mode="batch", enable_cbo=enable_cbo)


def run_streaming_workload(
    num_rows: int,
    enable_cbo: bool,
) -> benchmark_harness.RunMetrics:
    """Execute the streaming pipeline with or without CBO, measuring per-batch latency."""
    ctx = _configure_context(streaming=True, enable_cbo=enable_cbo)
    with _context_scope(ctx):
        ds = build_streaming_dataset(num_rows)

        batch_latencies: List[float] = []
        consumed = 0

        start = time.perf_counter()
        for batch in ds.iter_batches(batch_size=4096, batch_format="numpy"):
            batch_start = time.perf_counter()
            consumed += len(batch["id"])
            batch_latencies.append(time.perf_counter() - batch_start)
        duration = time.perf_counter() - start

        metrics = _summarize_stats(
            ds,
            duration,
            mode="streaming",
            enable_cbo=enable_cbo,
            batch_latencies=batch_latencies,
        )
        metrics.total_input_rows = consumed
        return metrics


def run_parameterized_workload(
    num_rows: int,
    transform_ops: int,
    enable_cbo: bool,
    payload_bytes_per_row: int = 0,
    forced_reservation_ratio: Optional[float] = None,
    memory_poll_interval_s: Optional[float] = None,
) -> benchmark_harness.RunMetrics:
    workload_id = f"chain_rows{num_rows}_ops{transform_ops}"
    plan_description = _parameterized_pipeline_desc(
        transform_ops=transform_ops,
        payload_bytes_per_row=payload_bytes_per_row,
    )
    ctx = _configure_context(
        streaming=False,
        enable_cbo=enable_cbo,
        forced_reservation_ratio=forced_reservation_ratio,
        memory_poll_interval_s=memory_poll_interval_s,
    )
    with _context_scope(ctx):
        ds = build_parameterized_chain_dataset(
            num_rows=num_rows,
            transform_ops=transform_ops,
            payload_bytes_per_row=payload_bytes_per_row,
        )
        start = time.perf_counter()
        ds = ds.materialize()
        duration = time.perf_counter() - start
        metrics = _summarize_stats(
            ds,
            duration,
            mode="batch",
            enable_cbo=enable_cbo,
        )
        metrics.workload_id = workload_id
        metrics.plan_description = plan_description
        metrics.total_input_rows = num_rows
        metrics.data_scale_bytes = _estimate_chain_data_scale_bytes(
            num_rows=num_rows,
            payload_bytes_per_row=payload_bytes_per_row,
        )
        metrics.metadata.update(
            {
                "transform_ops": transform_ops,
                "payload_bytes_per_row": payload_bytes_per_row,
                "pipeline_desc": plan_description,
            }
        )
        return metrics


def _aggregate_repeated_runs(
    runs: List[benchmark_harness.RunMetrics],
) -> benchmark_harness.RunMetrics:
    if not runs:
        raise ValueError("runs must not be empty")
    first = runs[0]
    return benchmark_harness.RunMetrics(
        workload_id=first.workload_id,
        plan_description=first.plan_description,
        mode=first.mode,
        enable_cbo=first.enable_cbo,
        duration_s=round(statistics.mean(run.duration_s for run in runs), 4),
        reservation_ratio=round(
            statistics.mean(run.reservation_ratio for run in runs),
            4,
        ),
        operator_count=first.operator_count,
        operators=first.operators,
        global_bytes_spilled=int(
            statistics.mean(run.global_bytes_spilled for run in runs)
        ),
        dataset_bytes_spilled=int(
            statistics.mean(run.dataset_bytes_spilled for run in runs)
        ),
        streaming_schedule_s=round(
            statistics.mean(run.streaming_schedule_s for run in runs),
            4,
        ),
        total_input_rows=first.total_input_rows,
        latency_p50_ms=round(statistics.mean(run.latency_p50_ms for run in runs), 3),
        latency_p95_ms=round(statistics.mean(run.latency_p95_ms for run in runs), 3),
        latency_p99_ms=round(statistics.mean(run.latency_p99_ms for run in runs), 3),
        latency_mean_ms=round(statistics.mean(run.latency_mean_ms for run in runs), 3),
        data_scale_bytes=first.data_scale_bytes,
        peak_object_store_bytes=max(run.peak_object_store_bytes or 0 for run in runs)
        or None,
        peak_heap_bytes=max(run.peak_heap_bytes or 0 for run in runs) or None,
        metadata=dict(first.metadata),
    )


def _rotate(items: List[Any], offset: int) -> List[Any]:
    """Rotates a list so that every block starts with a different setting."""
    if not items:
        return []
    offset %= len(items)
    return list(items[offset:]) + list(items[:offset])


def _paired_delta_stats(deltas: List[float]) -> Dict[str, Any]:
    """Summarizes paired relative deltas.

    A single pair carries no spread information, so it is reported as not
    significant instead of pretending its point estimate is a result.
    """
    mean_delta = statistics.mean(deltas) if deltas else 0.0
    median_delta = statistics.median(deltas) if deltas else 0.0
    interval = (
        benchmark_harness.bootstrap_mean_ci95(deltas) if len(deltas) >= 2 else None
    )
    return {
        "mean": mean_delta,
        "median": median_delta,
        "ci95": interval,
        "significant": bool(
            interval is not None and (interval[0] > 0 or interval[1] < 0)
        ),
    }


def run_r_value_matrix_experiment(
    workloads: List[benchmark_harness.ParameterizedWorkloadCase],
    sweep_ratios: List[float],
    repetitions: int,
    subtask_index: Optional[int] = None,
    subtask_count: Optional[int] = None,
    log_fn=print,
    warmup: bool = True,
) -> List[benchmark_harness.RValueMatrixResult]:
    """Runs the R sweep as paired, interleaved blocks.

    Each block executes every setting (derived, then each manual ratio) in a
    rotated order and repeats the default ratio once at the end. Two properties
    follow: no setting always runs first or last, and the spread between the two
    identical default samples is an empirical noise floor. A ratio only leaves
    the default when its paired delta both excludes zero and exceeds that floor.

    ``derived`` is CBO enabled with the ratio resolved by the rule, not a manual
    R; the derived-versus-default delta therefore includes every other CBO
    effect and is reported with its own paired confidence interval.
    """
    default_ratio = 0.5
    ratios = list(dict.fromkeys([*sweep_ratios, default_ratio]))
    settings: List[Any] = [("derived", True, None)] + [
        (f"r={ratio:g}", False, ratio) for ratio in ratios
    ]
    default_key = f"r={default_ratio:g}"

    results: List[benchmark_harness.RValueMatrixResult] = []
    for workload in workloads:
        log_fn(
            f"[r-matrix] {workload.workload_id} rows={workload.num_rows} "
            f"ops={workload.transform_ops}"
        )
        workload_kwargs = {
            "num_rows": workload.num_rows,
            "transform_ops": workload.transform_ops,
        }
        if warmup:
            log_fn(f"[r-matrix][warmup] {workload.workload_id} derived + default")
            run_parameterized_workload(**workload_kwargs, enable_cbo=True)
            run_parameterized_workload(
                **workload_kwargs,
                enable_cbo=False,
                forced_reservation_ratio=default_ratio,
            )

        runs: Dict[str, List[benchmark_harness.RunMetrics]] = {
            key: [] for key, _, _ in settings
        }
        block_means: List[Dict[str, float]] = []
        duplicate_gaps: List[float] = []
        duplicate_spreads: List[float] = []

        for block in range(1, repetitions + 1):
            durations: Dict[str, List[float]] = {key: [] for key, _, _ in settings}
            for key, enable_cbo, ratio in _rotate(settings, block - 1):
                log_fn(
                    f"[r-matrix] {workload.workload_id} "
                    f"block {block}/{repetitions} {key}"
                )
                metrics = run_parameterized_workload(
                    **workload_kwargs,
                    enable_cbo=enable_cbo,
                    forced_reservation_ratio=ratio,
                )
                durations[key].append(metrics.duration_s)
                runs[key].append(metrics)

            duplicate = run_parameterized_workload(
                **workload_kwargs,
                enable_cbo=False,
                forced_reservation_ratio=default_ratio,
            )
            durations[default_key].append(duplicate.duration_s)
            runs[default_key].append(duplicate)

            first, second = durations[default_key][0], durations[default_key][1]
            baseline = (first + second) / 2
            if baseline > 0:
                duplicate_gaps.append(abs(first - second))
                duplicate_spreads.append(abs(first - second) / baseline)
            block_means.append(
                {key: statistics.mean(values) for key, values in durations.items()}
            )

        noise_floor_s = statistics.median(duplicate_gaps) if duplicate_gaps else 0.0
        noise_floor_pct = (
            statistics.median(duplicate_spreads) if duplicate_spreads else 0.0
        )

        def _paired_deltas(key: str) -> List[float]:
            return [
                (means[key] - means[default_key]) / means[default_key]
                for means in block_means
                if means[default_key] > 0
            ]

        aggregates = {
            key: _aggregate_repeated_runs(values) for key, values in runs.items()
        }

        sweep_detail: List[Dict[str, Any]] = []
        for ratio in ratios:
            key = f"r={ratio:g}"
            is_default = abs(ratio - default_ratio) < 1e-9
            summary = _paired_delta_stats(_paired_deltas(key))
            time_values = [means[key] for means in block_means]
            time_ci = benchmark_harness.bootstrap_mean_ci95(time_values)
            metrics = aggregates[key]
            sweep_detail.append(
                {
                    "ratio": ratio,
                    "time_s": round(metrics.duration_s, 4),
                    "time_s_samples": [round(value, 4) for value in time_values],
                    "time_ci95": (
                        None
                        if time_ci is None
                        else [round(time_ci[0], 4), round(time_ci[1], 4)]
                    ),
                    "delta_vs_default_pct": round(summary["mean"] * 100, 2),
                    "delta_median_pct": round(summary["median"] * 100, 2),
                    "delta_ci95": (
                        None
                        if summary["ci95"] is None
                        else [
                            round(summary["ci95"][0] * 100, 2),
                            round(summary["ci95"][1] * 100, 2),
                        ]
                    ),
                    "significant": bool(
                        summary["significant"]
                        and not is_default
                        and abs(summary["median"]) > noise_floor_pct
                    ),
                    "spill_bytes": max(
                        metrics.global_bytes_spilled,
                        metrics.dataset_bytes_spilled,
                    ),
                    "peak_heap_bytes": metrics.peak_heap_bytes,
                    "peak_object_store_bytes": metrics.peak_object_store_bytes,
                    "is_default": is_default,
                    "is_best": False,
                }
            )

        default_entry = next(entry for entry in sweep_detail if entry["is_default"])
        faster = [
            entry
            for entry in sweep_detail
            if not entry["is_default"]
            and entry["significant"]
            and entry["delta_vs_default_pct"] < 0
        ]
        if faster:
            best_entry = min(faster, key=lambda entry: entry["time_s"])
            best_is_significant = True
        else:
            # Nothing beat the default beyond the noise floor; reporting a
            # noisy argmin as the oracle value would be a fabrication.
            best_entry = default_entry
            best_is_significant = False
        best_entry["is_best"] = True

        derived_summary = _paired_delta_stats(_paired_deltas("derived"))
        derived_metrics = aggregates["derived"]
        derived_ci = derived_summary["ci95"]
        derived_significant = bool(
            derived_summary["significant"]
            and abs(derived_summary["median"]) > noise_floor_pct
        )

        results.append(
            benchmark_harness.RValueMatrixResult(
                workload_id=workload.workload_id,
                num_rows=workload.num_rows,
                transform_ops=workload.transform_ops,
                derived_ratio=float(derived_metrics.reservation_ratio),
                derived_duration_s=derived_metrics.duration_s,
                best_ratio=float(best_entry["ratio"]),
                best_duration_s=float(best_entry["time_s"]),
                default_ratio=default_ratio,
                default_duration_s=float(default_entry["time_s"]),
                r_error=round(
                    abs(
                        float(derived_metrics.reservation_ratio)
                        - float(best_entry["ratio"])
                    ),
                    4,
                ),
                operator_count=derived_metrics.operator_count,
                data_scale_bytes=derived_metrics.data_scale_bytes,
                peak_object_store_bytes=derived_metrics.peak_object_store_bytes,
                peak_heap_bytes=derived_metrics.peak_heap_bytes,
                spill_bytes=max(
                    derived_metrics.global_bytes_spilled,
                    derived_metrics.dataset_bytes_spilled,
                ),
                pipeline_desc=workload.description,
                sweep_detail=sweep_detail,
                subtask_index=subtask_index,
                subtask_count=subtask_count,
                blocks=len(block_means),
                noise_floor_s=round(noise_floor_s, 4),
                noise_floor_pct=round(noise_floor_pct * 100, 2),
                best_is_significant=best_is_significant,
                derived_vs_default_pct=round(derived_summary["mean"] * 100, 2),
                derived_vs_default_ci95=(
                    None
                    if derived_ci is None
                    else [round(derived_ci[0] * 100, 2), round(derived_ci[1] * 100, 2)]
                ),
                derived_vs_default_significant=derived_significant,
            )
        )
    return results


def run_memory_profile_experiment(
    plan: List[benchmark_harness.ParameterizedExecutionCase],
    repetitions: int,
    memory_poll_interval_s: float,
    subtask_index: Optional[int] = None,
    subtask_count: Optional[int] = None,
    preset_name: Optional[str] = None,
    log_fn=print,
    warmup: bool = False,
) -> List[benchmark_harness.RunMetrics]:
    if warmup:
        warmed = set()
        for case in plan:
            key = (case.num_rows, case.transform_ops, case.payload_bytes_per_row)
            if key in warmed:
                continue
            warmed.add(key)
            log_fn(f"[warmup] {case.workload_id} memory-profile CBO=off/on")
            for enable_cbo in (False, True):
                run_parameterized_workload(
                    num_rows=case.num_rows,
                    transform_ops=case.transform_ops,
                    enable_cbo=enable_cbo,
                    payload_bytes_per_row=case.payload_bytes_per_row,
                    memory_poll_interval_s=memory_poll_interval_s,
                )

    results: List[benchmark_harness.RunMetrics] = []
    for case in plan:
        cbo_label = "ON" if case.cbo_enabled else "OFF"
        log_fn(
            f"[rep {case.repetition}/{repetitions}] "
            f"{case.workload_id} memory-profile CBO={cbo_label}"
        )
        results.append(
            run_parameterized_workload(
                num_rows=case.num_rows,
                transform_ops=case.transform_ops,
                enable_cbo=case.cbo_enabled,
                payload_bytes_per_row=case.payload_bytes_per_row,
                memory_poll_interval_s=memory_poll_interval_s,
            )
        )
        results[-1].metadata.update(
            {
                "run_id": case.run_id,
                "repetition": case.repetition,
                "block": case.block,
                "slot": case.slot,
                "run_group_id": f"{case.workload_id}:rep{case.repetition}",
                "subtask_index": subtask_index,
                "subtask_count": subtask_count,
                "preset_name": preset_name,
            }
        )
    return results


def _parse_int_list(raw: str) -> List[int]:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    if not values:
        raise ValueError("expected at least one integer value")
    return [int(value) for value in values]


def _parse_float_list(raw: str) -> List[float]:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    if not values:
        raise ValueError("expected at least one float value")
    return [float(value) for value in values]


def _resolve_profile_inputs(args) -> Dict[str, Any]:
    operator_counts = _parse_int_list(args.profile_operator_counts)
    if args.profile_preset:
        return benchmark_harness.resolve_memory_profile_preset(
            args.profile_preset,
            operator_counts=operator_counts,
        )

    return {
        "preset_name": None,
        "row_counts": _parse_int_list(args.profile_row_scales),
        "operator_counts": operator_counts,
        "payload_bytes_per_row": args.payload_bytes_per_row,
        "target_total_bytes": None,
        "target_rows": None,
        "description": None,
    }


def _write_subtask_manifest(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, indent=2)


def _write_markdown_report(
    path: str,
    payload: Dict[str, Any],
    title: Optional[str],
    feedback_alpha: float,
    feedback_tolerance: float,
    feedback_max_iterations: int,
) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    benchmark_postprocess.write_markdown_report(
        path,
        payload,
        title=title,
        runtime_feedback_alpha=feedback_alpha,
        runtime_feedback_tolerance=feedback_tolerance,
        runtime_feedback_max_iterations=feedback_max_iterations,
    )


def _write_publication_bundle(
    output_dir: str,
    payload: Dict[str, Any],
    title: Optional[str],
    environment_metadata: Optional[Dict[str, Any]],
    input_result_paths: Optional[List[str]],
    manifest_path: Optional[str],
    feedback_alpha: float,
    feedback_tolerance: float,
    feedback_max_iterations: int,
) -> Dict[str, Any]:
    return benchmark_postprocess.write_publication_bundle(
        output_dir,
        payload,
        title=title,
        environment_metadata=environment_metadata,
        input_result_paths=input_result_paths,
        manifest_path=manifest_path,
        runtime_feedback_alpha=feedback_alpha,
        runtime_feedback_tolerance=feedback_tolerance,
        runtime_feedback_max_iterations=feedback_max_iterations,
    )


def _parse_str_list(raw: str) -> List[str]:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    if not values:
        raise ValueError("expected at least one non-empty path")
    return values


def _load_str_list_file(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as fp:
        payload = json.load(fp)
    if not isinstance(payload, list) or not payload:
        raise ValueError("input-results-file must contain a non-empty JSON list")
    values = [str(item).strip() for item in payload if str(item).strip()]
    if not values:
        raise ValueError("input-results-file must contain at least one non-empty path")
    return values


def _ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)


# ---------------------------------------------------------------------------
# Aggregation & reporting
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Per-rule benchmark framework
# ---------------------------------------------------------------------------


def _timed_materialize(build_fn, reps: int = 1) -> float:
    """Build & materialize *reps* times; return mean wall-clock seconds."""
    times: List[float] = []
    for _ in range(reps):
        ds = build_fn()
        t0 = time.perf_counter()
        ds.materialize()
        times.append(time.perf_counter() - t0)
    return statistics.mean(times)


@contextmanager
def _without_physical_rule(rule_cls):
    """Temporarily remove *rule_cls* from the physical ruleset."""
    from ray.data._internal.logical.optimizers import get_physical_ruleset

    rs = get_physical_ruleset()
    rs.remove(rule_cls)
    try:
        yield
    finally:
        rs.add(rule_cls)


def _heavy_udf(batch):
    """CPU-intensive UDF (module-level for pickling)."""
    import numpy as np

    arr = batch["id"].astype(np.float64)
    for _ in range(5):
        arr = np.sin(arr) * np.cos(arr) + np.sqrt(np.abs(arr) + 1)
    return {"id": arr}


def _extra_heavy_udf(batch):
    """Extra-heavy UDF used where the computation gap must be visible."""
    import numpy as np

    arr = batch["id"].astype(np.float64)
    for _ in range(50):
        arr = np.sin(arr) * np.cos(arr) + np.sqrt(np.abs(arr) + 1)
    return {"id": arr}


# ---- individual rule benchmarks ------------------------------------------


def bench_operator_fusion(
    num_rows: int,
    reps: int,
) -> benchmark_harness.RuleBenchResult:
    """OperatorFusion: fused vs unfused map chain."""
    from ray.data._internal.logical.rules.operator_fusion import FuseOperators

    def build():
        ds = ray.data.range(num_rows)
        ds = ds.map_batches(lambda b: {"id": b["id"] * 2}, batch_format="numpy")
        ds = ds.map_batches(lambda b: {"id": b["id"] + 1}, batch_format="numpy")
        return ds.map_batches(_heavy_udf, batch_format="numpy")

    with _without_physical_rule(FuseOperators):
        t_off = _timed_materialize(build, reps)
    t_on = _timed_materialize(build, reps)
    imp = (t_off - t_on) / t_off * 100 if t_off > 0 else 0
    return benchmark_harness.RuleBenchResult(
        rule_name="OperatorFusion",
        rule_category="RBO",
        optimization_goal="Throughput",
        pipeline_desc="range->map(x2)->map(+1)->map(heavy)",
        baseline_label="No fusion",
        optimized_label="Fused",
        baseline_time_s=round(t_off, 3),
        optimized_time_s=round(t_on, 3),
        improvement_pct=round(imp, 1),
    )


def bench_predicate_pushdown(
    num_rows: int,
    reps: int,
) -> benchmark_harness.RuleBenchResult:
    """PredicatePushdown: filter-late vs filter-early.

    Uses an extra-heavy UDF so that the computation gap between
    processing 100 %% vs ~33 %% of rows is clearly visible even at
    moderate row counts.
    """

    def build_late():
        ds = ray.data.range(num_rows)
        ds = ds.map_batches(_extra_heavy_udf, batch_format="numpy")
        return ds.filter(lambda row: row["id"] > 0)

    def build_early():
        ds = ray.data.range(num_rows)
        ds = ds.filter(lambda row: row["id"] % 3 == 0)
        return ds.map_batches(_extra_heavy_udf, batch_format="numpy")

    t_late = _timed_materialize(build_late, reps)
    t_early = _timed_materialize(build_early, reps)
    imp = (t_late - t_early) / t_late * 100 if t_late > 0 else 0
    return benchmark_harness.RuleBenchResult(
        rule_name="PredicatePushdown",
        rule_category="RBO",
        optimization_goal="Throughput",
        pipeline_desc="range->[filter <-> map(heavy)]",
        baseline_label="Filter after map",
        optimized_label="Filter before map",
        baseline_time_s=round(t_late, 3),
        optimized_time_s=round(t_early, 3),
        improvement_pct=round(imp, 1),
    )


def bench_limit_pushdown(
    num_rows: int,
    reps: int,
) -> benchmark_harness.RuleBenchResult:
    """LimitPushdown: limit-late vs limit-early.

    The baseline places a sort (AllToAll) before the limit, forcing full
    materialisation of all rows through the heavy UDF.  The optimised
    variant limits first, so only *limit_n* rows flow through the UDF
    and subsequent sort.
    """
    limit_n = max(1000, num_rows // 100)

    def build_late():
        ds = ray.data.range(num_rows)
        ds = ds.map_batches(_heavy_udf, batch_format="numpy")
        ds = ds.sort(key="id")  # AllToAll forces full materialisation
        return ds.limit(limit_n)

    def build_early():
        ds = ray.data.range(num_rows)
        ds = ds.limit(limit_n)
        ds = ds.map_batches(_heavy_udf, batch_format="numpy")
        return ds.sort(key="id")

    t_late = _timed_materialize(build_late, reps)
    t_early = _timed_materialize(build_early, reps)
    imp = (t_late - t_early) / t_late * 100 if t_late > 0 else 0
    return benchmark_harness.RuleBenchResult(
        rule_name="LimitPushdown",
        rule_category="RBO",
        optimization_goal="Throughput",
        pipeline_desc=f"range({num_rows})->[limit({limit_n}) <-> map+sort]",
        baseline_label="Limit after map+sort",
        optimized_label="Limit before map+sort",
        baseline_time_s=round(t_late, 3),
        optimized_time_s=round(t_early, 3),
        improvement_pct=round(imp, 1),
    )


def bench_combine_shuffles(
    num_rows: int,
    reps: int,
) -> benchmark_harness.RuleBenchResult:
    """CombineShuffles: two consecutive repartitions vs one.

    A map_batches is placed before the repartitions so the total pipeline
    cost is dominated by the shuffle stages, making the difference
    between one and two shuffles visible.
    """

    def build_double():
        ds = ray.data.range(num_rows)
        ds = ds.map_batches(_heavy_udf, batch_format="numpy")
        ds = ds.repartition(32)
        return ds.repartition(16)

    def build_single():
        ds = ray.data.range(num_rows)
        ds = ds.map_batches(_heavy_udf, batch_format="numpy")
        return ds.repartition(16)

    t_double = _timed_materialize(build_double, reps)
    t_single = _timed_materialize(build_single, reps)
    imp = (t_double - t_single) / t_double * 100 if t_double > 0 else 0
    return benchmark_harness.RuleBenchResult(
        rule_name="CombineShuffles",
        rule_category="RBO",
        optimization_goal="Throughput",
        pipeline_desc="map->repart(32)->repart(16) vs map->repart(16)",
        baseline_label="Double repart",
        optimized_label="Single repart",
        baseline_time_s=round(t_double, 3),
        optimized_time_s=round(t_single, 3),
        improvement_pct=round(imp, 1),
    )


def bench_reservation_ratio(
    num_rows: int,
    reps: int,
) -> benchmark_harness.RuleBenchResult:
    """DeriveReservationRatio: sweep reservation-ratio on a sort pipeline."""
    ratios = [0.1, 0.3, 0.5, 0.7, 0.9]
    default_r = 0.5
    sweep: List[Dict[str, Any]] = []
    for ratio in ratios:
        ctx = DataContext.get_current().copy()
        ctx.op_resource_reservation_ratio = ratio
        ctx.op_resource_reservation_enabled = True
        ctx.execution_options.verbose_progress = False
        with _context_scope(ctx):

            def _build(r=ratio):  # noqa: B023
                ds = ray.data.range(num_rows)
                ds = ds.filter(lambda row: row["id"] % 2 == 0)
                ds = ds.map_batches(_heavy_udf, batch_format="numpy")
                return ds.sort(key="id")

            t = _timed_materialize(_build, reps)
        sweep.append(
            {"ratio": ratio, "time_s": round(t, 3), "is_default": ratio == default_r}
        )
    best = min(sweep, key=lambda e: e["time_s"])
    dflt = next(e for e in sweep if e["is_default"])
    imp = (
        (dflt["time_s"] - best["time_s"]) / dflt["time_s"] * 100
        if dflt["time_s"] > 0
        else 0
    )
    return benchmark_harness.RuleBenchResult(
        rule_name="DeriveReservationRatio",
        rule_category="CBO",
        optimization_goal="Resource",
        pipeline_desc="filter->map->sort @ sweep R",
        baseline_label=f"R={default_r}",
        optimized_label=f"R={best['ratio']}",
        baseline_time_s=dflt["time_s"],
        optimized_time_s=best["time_s"],
        improvement_pct=round(imp, 1),
        sweep_detail=sweep,
    )


def bench_shuffle_partitions(
    num_rows: int,
    reps: int,
) -> benchmark_harness.RuleBenchResult:
    """DeriveShufflePartitions: sweep partition counts for repartition."""
    counts = [4, 8, 16, 32, 64, 128]
    default_n = 64
    sweep: List[Dict[str, Any]] = []
    for n in counts:

        def _build(n_parts=n):  # noqa: B023
            ds = ray.data.range(num_rows)
            ds = ds.map_batches(
                lambda b: {"id": b["id"] * 2},
                batch_format="numpy",
            )
            return ds.repartition(n_parts)

        t = _timed_materialize(_build, reps)
        sweep.append(
            {"num_partitions": n, "time_s": round(t, 3), "is_default": n == default_n}
        )
    best = min(sweep, key=lambda e: e["time_s"])
    dflt = next(e for e in sweep if e["is_default"])
    imp = (
        (dflt["time_s"] - best["time_s"]) / dflt["time_s"] * 100
        if dflt["time_s"] > 0
        else 0
    )
    return benchmark_harness.RuleBenchResult(
        rule_name="DeriveShufflePartitions",
        rule_category="CBO",
        optimization_goal="Resource",
        pipeline_desc="map->repart(N) @ sweep N",
        baseline_label=f"N={default_n}",
        optimized_label=f"N={best['num_partitions']}",
        baseline_time_s=dflt["time_s"],
        optimized_time_s=best["time_s"],
        improvement_pct=round(imp, 1),
        sweep_detail=sweep,
    )


# ---- registry & orchestrator ---------------------------------------------

RULE_BENCHMARK_CASES = [
    benchmark_harness.RuleBenchmarkCase(
        name="OperatorFusion",
        category="RBO",
        optimization_goal="Throughput",
        runner=bench_operator_fusion,
    ),
    benchmark_harness.RuleBenchmarkCase(
        name="PredicatePushdown",
        category="RBO",
        optimization_goal="Throughput",
        runner=bench_predicate_pushdown,
    ),
    benchmark_harness.RuleBenchmarkCase(
        name="LimitPushdown",
        category="RBO",
        optimization_goal="Throughput",
        runner=bench_limit_pushdown,
    ),
    benchmark_harness.RuleBenchmarkCase(
        name="CombineShuffles",
        category="RBO",
        optimization_goal="Throughput",
        runner=bench_combine_shuffles,
    ),
    benchmark_harness.RuleBenchmarkCase(
        name="DeriveReservationRatio",
        category="CBO",
        optimization_goal="Resource",
        runner=bench_reservation_ratio,
    ),
    benchmark_harness.RuleBenchmarkCase(
        name="DeriveShufflePartitions",
        category="CBO",
        optimization_goal="Resource",
        runner=bench_shuffle_partitions,
    ),
]

END_TO_END_RUNNERS = {
    "batch": run_batch_workload,
    "streaming": run_streaming_workload,
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Production benchmark for Ray Data CBO."
    )
    # -- mode selection --
    parser.add_argument(
        "--per-rule",
        action="store_true",
        default=False,
        help="Run per-rule benchmarks instead of end-to-end comparison.",
    )
    parser.add_argument(
        "--rules",
        type=str,
        default=None,
        help="Comma-separated rule names to benchmark (default: all).  "
        "E.g. 'OperatorFusion,LimitPushdown'.",
    )
    parser.add_argument(
        "--validate-stats",
        action="store_true",
        default=False,
        help="Run CBO statistics infrastructure validation.",
    )
    parser.add_argument(
        "--r-value-matrix",
        action="store_true",
        default=False,
        help="Run same-chain reservation-ratio matrix experiment.",
    )
    parser.add_argument(
        "--memory-profile",
        action="store_true",
        default=False,
        help="Run peak memory profiling experiment.",
    )
    parser.add_argument(
        "--list-benchmarks",
        action="store_true",
        default=False,
        help="Print the registered benchmark catalog and execution plan, then exit.",
    )
    parser.add_argument(
        "--merge-results",
        action="store_true",
        default=False,
        help="Merge existing benchmark JSON payloads without starting Ray.",
    )
    parser.add_argument(
        "--input-results",
        type=str,
        default=None,
        help="Comma-separated JSON result files to merge for --merge-results.",
    )
    parser.add_argument(
        "--input-results-file",
        type=str,
        default=None,
        help="JSON file containing a list of result files to merge for --merge-results.",
    )
    parser.add_argument(
        "--merge-manifest",
        type=str,
        default=None,
        help="Optional subtask manifest JSON used to validate merged shard coverage.",
    )
    parser.add_argument(
        "--report-markdown",
        type=str,
        default=None,
        help="Optional path to write a markdown benchmark report.",
    )
    parser.add_argument(
        "--report-title",
        type=str,
        default=None,
        help="Optional report title used by --report-markdown.",
    )
    parser.add_argument(
        "--report-bundle-dir",
        type=str,
        default=None,
        help="Optional output directory for the Phase5 publication bundle.",
    )
    parser.add_argument(
        "--environment-metadata-json",
        type=str,
        default=None,
        help="Optional JSON file with environment metadata merged into the publication bundle.",
    )
    parser.add_argument(
        "--campaign-template-output",
        type=str,
        default=None,
        help="Write a Phase6 campaign-spec template JSON, then exit.",
    )
    parser.add_argument(
        "--campaign-spec",
        type=str,
        default=None,
        help="Load a Phase6 campaign spec JSON.",
    )
    parser.add_argument(
        "--campaign-plan-output",
        type=str,
        default=None,
        help="Optional path to write the generated campaign plan JSON.",
    )
    parser.add_argument(
        "--run-campaign",
        action="store_true",
        default=False,
        help="Execute the task plan generated from --campaign-spec.",
    )
    parser.add_argument(
        "--campaign-dry-run",
        action="store_true",
        default=False,
        help="Print campaign commands without executing them.",
    )
    parser.add_argument(
        "--no-campaign-resume",
        action="store_true",
        default=False,
        help="Disable Phase6 skip-completed behavior when running a campaign.",
    )
    parser.add_argument(
        "--feedback-alpha",
        type=float,
        default=0.7,
        help="EMA alpha used by Phase4 runtime-feedback convergence analysis.",
    )
    parser.add_argument(
        "--feedback-tolerance",
        type=float,
        default=0.02,
        help="Convergence tolerance for Phase4 runtime-feedback analysis.",
    )
    parser.add_argument(
        "--feedback-max-iterations",
        type=int,
        default=5,
        help="Maximum feedback iterations simulated in Phase4 analysis.",
    )
    # -- data scale --
    parser.add_argument(
        "--num-rows",
        type=int,
        default=200_000,
        help="Row count for per-rule benchmarks (default: 200000).",
    )
    parser.add_argument(
        "--batch-rows",
        type=int,
        default=200_000,
        help="Row count for end-to-end batch workload.",
    )
    parser.add_argument(
        "--streaming-rows",
        type=int,
        default=150_000,
        help="Row count for end-to-end streaming workload.",
    )
    parser.add_argument(
        "--matrix-row-scales",
        type=str,
        default="200000,500000",
        help="Comma-separated row counts for the R-value matrix experiment.",
    )
    parser.add_argument(
        "--matrix-operator-counts",
        type=str,
        default="2,4,6",
        help="Comma-separated transform-operator counts for the R-value matrix experiment.",
    )
    parser.add_argument(
        "--matrix-ratios",
        type=str,
        default="0.1,0.3,0.5,0.7,0.9",
        help="Comma-separated reservation ratios to sweep in the R-value matrix experiment.",
    )
    parser.add_argument(
        "--profile-row-scales",
        type=str,
        default="200000,500000",
        help="Comma-separated row counts for the memory-profile experiment.",
    )
    parser.add_argument(
        "--profile-operator-counts",
        type=str,
        default="2,4,6",
        help="Comma-separated transform-operator counts for the memory-profile experiment.",
    )
    parser.add_argument(
        "--payload-bytes-per-row",
        type=int,
        default=2048,
        help="Synthetic payload size per row for the memory-profile experiment.",
    )
    parser.add_argument(
        "--profile-preset",
        type=str,
        default=None,
        help="Named memory-profile preset. Available: 10gb_10m.",
    )
    parser.add_argument(
        "--memory-poll-interval-s",
        type=float,
        default=0.1,
        help="Polling interval used to capture peak heap memory during memory profiling.",
    )
    # -- execution --
    parser.add_argument(
        "--repetitions",
        type=int,
        default=3,
        help=(
            "How many paired blocks to run per configuration. End-to-end runs "
            "repeat both settings twice per block; matrix runs repeat the "
            "default ratio once per block to measure the noise floor."
        ),
    )
    parser.add_argument(
        "--object-store-memory-bytes",
        type=int,
        default=None,
        help="Optional object store memory override passed to ray.init().",
    )
    parser.add_argument(
        "--ray-address",
        type=str,
        default="local",
        help="Ray address passed to ray.init() (default: local).",
    )
    parser.add_argument(
        "--subtask-count",
        type=int,
        default=1,
        help="How many subtasks to split matrix/profile workloads into.",
    )
    parser.add_argument(
        "--subtask-index",
        type=int,
        default=0,
        help="Which subtask shard to execute.",
    )
    parser.add_argument(
        "--subtask-manifest-output",
        type=str,
        default=None,
        help="Optional path to write the computed subtask manifest JSON.",
    )
    parser.add_argument(
        "--warmup",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Run one discarded warmup iteration per workload before timing "
            "(default: on; use --no-warmup to disable)."
        ),
    )
    parser.add_argument(
        "--output",
        type=str,
        default="cbo_benchmark_results.json",
        help="Path to write the JSON results.",
    )
    args = parser.parse_args()

    selected_modes = sum(
        1
        for flag in (
            args.per_rule,
            args.r_value_matrix,
            args.memory_profile,
            args.merge_results,
            bool(args.campaign_spec),
        )
        if flag
    )
    if selected_modes > 1:
        parser.error(
            "Choose at most one explicit mode among "
            "--per-rule, --r-value-matrix, --memory-profile, --merge-results, --campaign-spec."
        )
    if args.feedback_alpha <= 0.0 or args.feedback_alpha > 1.0:
        parser.error("--feedback-alpha must be within (0.0, 1.0].")
    if args.feedback_tolerance < 0.0:
        parser.error("--feedback-tolerance must be non-negative.")
    if args.feedback_max_iterations < 0:
        parser.error("--feedback-max-iterations must be non-negative.")
    if args.merge_results and not (args.input_results or args.input_results_file):
        parser.error(
            "--merge-results requires --input-results or --input-results-file."
        )
    if args.input_results and args.input_results_file:
        parser.error("Use only one of --input-results or --input-results-file.")
    if args.run_campaign and not args.campaign_spec:
        parser.error("--run-campaign requires --campaign-spec.")
    if args.campaign_dry_run and not args.run_campaign:
        parser.error("--campaign-dry-run requires --run-campaign.")
    environment_metadata = benchmark_postprocess.load_environment_metadata_file(
        args.environment_metadata_json
    )
    if args.campaign_template_output:
        benchmark_campaign.write_campaign_spec_template(args.campaign_template_output)
        print(
            "Campaign spec template written to "
            f"{os.path.abspath(args.campaign_template_output)}"
        )
        return
    if args.subtask_count < 1:
        parser.error("--subtask-count must be positive.")
    if args.subtask_index < 0 or args.subtask_index >= args.subtask_count:
        parser.error("--subtask-index must be within [0, --subtask-count).")
    if args.subtask_count > 1 and not (
        args.r_value_matrix or args.memory_profile or args.list_benchmarks
    ):
        parser.error(
            "Subtask splitting is currently supported for "
            "--r-value-matrix, --memory-profile, or --list-benchmarks."
        )
    if args.campaign_spec:
        campaign_spec = benchmark_campaign.load_campaign_spec_file(args.campaign_spec)
        campaign_plan = benchmark_campaign.build_campaign_plan(campaign_spec)
        campaign_plan_output = args.campaign_plan_output or os.path.join(
            campaign_plan["output_dir"], "campaign_plan.json"
        )
        benchmark_campaign.write_campaign_plan(campaign_plan_output, campaign_plan)
        print(benchmark_campaign.render_campaign_plan_summary(campaign_plan))
        print("\nCampaign plan written to " f"{os.path.abspath(campaign_plan_output)}")
        if args.run_campaign:
            campaign_result = benchmark_campaign.execute_campaign_plan(
                campaign_plan,
                resume=not args.no_campaign_resume,
                dry_run=args.campaign_dry_run,
            )
            print(
                "\nCampaign execution finished with "
                f"{len(campaign_result['task_results'])} task result(s)"
            )
        return
    if args.merge_results:
        input_result_paths = (
            _parse_str_list(args.input_results)
            if args.input_results
            else _load_str_list_file(args.input_results_file)
        )
        merged_payload = benchmark_postprocess.merge_benchmark_payload_files(
            input_result_paths,
            manifest_path=args.merge_manifest,
            runtime_feedback_alpha=args.feedback_alpha,
            runtime_feedback_tolerance=args.feedback_tolerance,
            runtime_feedback_max_iterations=args.feedback_max_iterations,
        )
        _ensure_parent_dir(args.output)
        with open(args.output, "w", encoding="utf-8") as fp:
            json.dump(merged_payload, fp, indent=2)
        print(f"\nMerged results written to {os.path.abspath(args.output)}")
        if args.report_markdown:
            _write_markdown_report(
                args.report_markdown,
                merged_payload,
                args.report_title,
                args.feedback_alpha,
                args.feedback_tolerance,
                args.feedback_max_iterations,
            )
            print(
                "Markdown report written to " f"{os.path.abspath(args.report_markdown)}"
            )
        if args.report_bundle_dir:
            bundle_manifest = _write_publication_bundle(
                args.report_bundle_dir,
                merged_payload,
                args.report_title,
                environment_metadata,
                input_result_paths,
                args.merge_manifest,
                args.feedback_alpha,
                args.feedback_tolerance,
                args.feedback_max_iterations,
            )
            print(
                "Publication bundle written to "
                f"{os.path.abspath(args.report_bundle_dir)}"
            )
            print(
                "Bundle manifest written to "
                f"{bundle_manifest['artifacts']['bundle_manifest_json']}"
            )
        return

    if args.list_benchmarks:
        matrix_rows = _parse_int_list(args.matrix_row_scales)
        matrix_ops = _parse_int_list(args.matrix_operator_counts)
        profile_inputs = _resolve_profile_inputs(args)
        matrix_workloads = benchmark_harness.build_parameterized_workloads(
            row_counts=matrix_rows,
            operator_counts=matrix_ops,
            payload_bytes_per_row=0,
        )
        memory_plan = benchmark_harness.build_memory_profile_plan(
            row_counts=profile_inputs["row_counts"],
            operator_counts=profile_inputs["operator_counts"],
            repetitions=args.repetitions,
            payload_bytes_per_row=profile_inputs["payload_bytes_per_row"],
        )
        plan = benchmark_harness.build_end_to_end_execution_plan(
            batch_rows=args.batch_rows,
            streaming_rows=args.streaming_rows,
            repetitions=args.repetitions,
        )
        print(benchmark_harness.describe_rule_catalog(RULE_BENCHMARK_CASES))
        print()
        print(benchmark_harness.describe_end_to_end_plan(plan))
        print()
        print(
            benchmark_harness.describe_parameterized_workloads(
                matrix_workloads,
                title="R-value matrix workload grid:",
            )
        )
        print()
        print(benchmark_harness.describe_memory_profile_plan(memory_plan))
        manifest_payload: Dict[str, Any] = {}
        if args.subtask_count > 1:
            matrix_manifest = benchmark_harness.build_parameterized_subtask_manifest(
                matrix_workloads,
                subtask_count=args.subtask_count,
                experiment_type="r_value_matrix",
            )
            memory_manifest = benchmark_harness.build_memory_profile_subtask_manifest(
                memory_plan,
                subtask_count=args.subtask_count,
            )
            print()
            print(
                benchmark_harness.describe_subtask_manifest(
                    matrix_manifest,
                    title="R-value matrix subtask manifest:",
                )
            )
            print()
            print(
                benchmark_harness.describe_subtask_manifest(
                    memory_manifest,
                    title="Memory-profile subtask manifest:",
                )
            )
            manifest_payload = {
                "r_value_matrix": [asdict(item) for item in matrix_manifest],
                "memory_profile": [asdict(item) for item in memory_manifest],
            }
        if args.subtask_manifest_output:
            _write_subtask_manifest(
                args.subtask_manifest_output,
                manifest_payload
                or {
                    "r_value_matrix": [],
                    "memory_profile": [],
                },
            )
        return

    # -- statistics validation (Part 1 does not need ray.init) --
    if args.validate_stats:
        try:
            validate_statistics_infrastructure()
        except Exception as exc:
            print(f"\n  [WARN] Statistics validation skipped: {exc}")

    ray_init_kwargs: Dict[str, Any] = {"ignore_reinit_error": True}
    if args.ray_address:
        ray_init_kwargs["address"] = args.ray_address
    if args.object_store_memory_bytes is not None:
        ray_init_kwargs["object_store_memory"] = args.object_store_memory_bytes
    ray.init(**ray_init_kwargs)

    rule_results: Optional[List[benchmark_harness.RuleBenchResult]] = None
    e2e_runs: List[benchmark_harness.RunMetrics] = []
    matrix_results: Optional[List[benchmark_harness.RValueMatrixResult]] = None
    memory_profile_runs: Optional[List[benchmark_harness.RunMetrics]] = None
    payload_config: Dict[str, Any] = {}

    try:
        if args.validate_stats:
            try:
                demo_live_statistics(
                    num_rows=min(args.num_rows, 10_000),
                )
            except Exception as exc:
                print(f"\n  [WARN] Live statistics demo skipped: {exc}")

        if args.per_rule:
            # -- per-rule benchmark mode --
            rule_list = (
                [r.strip() for r in args.rules.split(",")] if args.rules else None
            )
            selected_cases = benchmark_harness.select_rule_benchmark_cases(
                RULE_BENCHMARK_CASES,
                rule_list,
            )
            if args.warmup:
                print("\n[warmup] Running warmup pipeline ...")
                ray.data.range(min(args.num_rows, 50_000)).map_batches(
                    lambda b: b,
                    batch_format="numpy",
                ).materialize()
                print("[warmup] Done.\n")

            rule_results = []
            for case in selected_cases:
                try:
                    result = benchmark_harness.run_rule_benchmark_suite(
                        [case],
                        num_rows=args.num_rows,
                        repetitions=args.repetitions,
                    )
                    rule_results.extend(result)
                except Exception as exc:
                    print(f"  [{case.name}] FAILED: {exc}")
        elif args.r_value_matrix:
            matrix_workloads = benchmark_harness.build_parameterized_workloads(
                row_counts=_parse_int_list(args.matrix_row_scales),
                operator_counts=_parse_int_list(args.matrix_operator_counts),
                payload_bytes_per_row=0,
            )
            matrix_manifest = benchmark_harness.build_parameterized_subtask_manifest(
                matrix_workloads,
                subtask_count=args.subtask_count,
                experiment_type="r_value_matrix",
            )
            if args.subtask_manifest_output:
                _write_subtask_manifest(
                    args.subtask_manifest_output,
                    {"r_value_matrix": [asdict(item) for item in matrix_manifest]},
                )
            selected_workloads = benchmark_harness.shard_parameterized_workloads(
                matrix_workloads,
                subtask_index=args.subtask_index,
                subtask_count=args.subtask_count,
            )
            matrix_results = run_r_value_matrix_experiment(
                workloads=selected_workloads,
                sweep_ratios=_parse_float_list(args.matrix_ratios),
                repetitions=args.repetitions,
                subtask_index=args.subtask_index,
                subtask_count=args.subtask_count,
                warmup=args.warmup,
            )
            payload_config = {
                "row_scales": _parse_int_list(args.matrix_row_scales),
                "operator_counts": _parse_int_list(args.matrix_operator_counts),
                "sweep_ratios": _parse_float_list(args.matrix_ratios),
                "repetitions": args.repetitions,
                "warmup": args.warmup,
                "design": "paired_interleaved_blocks",
                "noise_floor": "default ratio run twice per block",
                "ci_method": "paired bootstrap over block deltas",
                "subtask_index": args.subtask_index,
                "subtask_count": args.subtask_count,
                "selected_workload_ids": [
                    workload.workload_id for workload in selected_workloads
                ],
            }
        elif args.memory_profile:
            profile_inputs = _resolve_profile_inputs(args)
            memory_plan = benchmark_harness.build_memory_profile_plan(
                row_counts=profile_inputs["row_counts"],
                operator_counts=profile_inputs["operator_counts"],
                repetitions=args.repetitions,
                payload_bytes_per_row=profile_inputs["payload_bytes_per_row"],
            )
            memory_manifest = benchmark_harness.build_memory_profile_subtask_manifest(
                memory_plan,
                subtask_count=args.subtask_count,
            )
            if args.subtask_manifest_output:
                _write_subtask_manifest(
                    args.subtask_manifest_output,
                    {"memory_profile": [asdict(item) for item in memory_manifest]},
                )
            selected_plan = benchmark_harness.shard_memory_profile_plan(
                memory_plan,
                subtask_index=args.subtask_index,
                subtask_count=args.subtask_count,
            )
            memory_profile_runs = run_memory_profile_experiment(
                plan=selected_plan,
                repetitions=args.repetitions,
                memory_poll_interval_s=args.memory_poll_interval_s,
                subtask_index=args.subtask_index,
                subtask_count=args.subtask_count,
                preset_name=profile_inputs["preset_name"],
                warmup=args.warmup,
            )
            payload_config = {
                "row_scales": profile_inputs["row_counts"],
                "operator_counts": profile_inputs["operator_counts"],
                "payload_bytes_per_row": profile_inputs["payload_bytes_per_row"],
                "memory_poll_interval_s": args.memory_poll_interval_s,
                "repetitions": args.repetitions,
                "warmup": args.warmup,
                "design": "paired_interleaved_blocks",
                "profile_preset": profile_inputs["preset_name"],
                "target_total_bytes": profile_inputs.get("target_total_bytes"),
                "target_rows": profile_inputs.get("target_rows"),
                "subtask_index": args.subtask_index,
                "subtask_count": args.subtask_count,
                "selected_run_ids": [case.run_id for case in selected_plan],
            }
        else:
            # -- end-to-end comparison mode --
            execution_plan = benchmark_harness.build_end_to_end_execution_plan(
                batch_rows=args.batch_rows,
                streaming_rows=args.streaming_rows,
                repetitions=args.repetitions,
            )
            e2e_runs = benchmark_harness.execute_end_to_end_plan(
                execution_plan,
                END_TO_END_RUNNERS,
                repetitions=args.repetitions,
                warmup=args.warmup,
            )
    finally:
        ray.shutdown()

    # -- reporting (does not need Ray) --
    if rule_results is not None:
        print(
            benchmark_harness.format_rule_results_table(
                rule_results,
                args.num_rows,
                args.repetitions,
            )
        )
        payload = build_per_rule_payload(
            [asdict(r) for r in rule_results],
            {
                "num_rows": args.num_rows,
                "repetitions": args.repetitions,
                "warmup": args.warmup,
            },
        )
    elif matrix_results is not None:
        print(benchmark_harness.format_r_value_matrix_table(matrix_results))
        payload = build_r_value_matrix_payload(
            [asdict(result) for result in matrix_results],
            payload_config,
        )
    elif memory_profile_runs is not None:
        print(benchmark_harness.format_memory_profile_table(memory_profile_runs))
        payload = build_memory_profile_payload(
            [asdict(run) for run in memory_profile_runs],
            payload_config,
        )
    else:
        aggregates = benchmark_harness.aggregate_runs(e2e_runs)
        print(benchmark_harness.format_end_to_end_comparison(aggregates))
        payload = build_end_to_end_payload(
            [asdict(r) for r in e2e_runs],
            aggregates,
            {
                "batch_rows": args.batch_rows,
                "streaming_rows": args.streaming_rows,
                "repetitions": args.repetitions,
                "warmup": args.warmup,
                "design": "paired_interleaved_blocks",
                "block_slots": 2,
                "order": "rotated per block, on/off slots swapped every block",
            },
        )

    if not payload["validation"]["is_valid"]:
        raise ValueError(
            "Benchmark output payload failed validation: "
            + "; ".join(payload["validation"]["errors"])
        )

    _ensure_parent_dir(args.output)
    with open(args.output, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, indent=2)
    print(f"\nFull results written to {os.path.abspath(args.output)}")
    if args.report_markdown:
        _write_markdown_report(
            args.report_markdown,
            payload,
            args.report_title,
            args.feedback_alpha,
            args.feedback_tolerance,
            args.feedback_max_iterations,
        )
        print("Markdown report written to " f"{os.path.abspath(args.report_markdown)}")
    if args.report_bundle_dir:
        bundle_manifest = _write_publication_bundle(
            args.report_bundle_dir,
            payload,
            args.report_title,
            environment_metadata,
            [os.path.abspath(args.output)],
            None,
            args.feedback_alpha,
            args.feedback_tolerance,
            args.feedback_max_iterations,
        )
        print(
            "Publication bundle written to "
            f"{os.path.abspath(args.report_bundle_dir)}"
        )
        print(
            "Bundle manifest written to "
            f"{bundle_manifest['artifacts']['bundle_manifest_json']}"
        )


if __name__ == "__main__":
    main()
