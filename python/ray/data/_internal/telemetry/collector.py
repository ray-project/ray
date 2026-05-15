"""Ray Data telemetry collector.

Accumulates per-execution telemetry (environment, workload description,
performance) and flushes it to GCS via ``record_extra_usage_tag``.

Mirrors the module-level pattern used by ``ray.data._internal.logical.util``.
"""

import importlib.metadata
import json
import logging
import re
import threading
import time
from typing import Any, Dict, List, Optional

from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators import AbstractUDFMap, Read, Write
from ray.data._internal.logical.util import _op_name_white_list

logger = logging.getLogger(__name__)

# Module state. Mutations are serialized through ``_lock``.
_executions: Dict[str, dict] = {}
_env_cache: Optional[dict] = None
_lock = threading.Lock()

# Dependency packages whose version we always record.
_TRACKED_DEPS = ("pyarrow", "numpy", "pandas", "ray")

# Third-party libraries whose presence (and version, if present) we record.
_TRACKED_THIRD_PARTY = ("daft", "vllm", "sglang", "deepspeed")


def record_workload(executor, logical_plan) -> None:
    """Record the planning-time entry for an execution.

    Called from ``_execute_dag`` before the executor runs. The resulting entry
    sits in ``_executions`` with ``performance: None`` and is filled in by
    ``record_execution_result`` after the executor finishes (success or fail).
    Flushes eagerly so attempted executions are captured even if the process
    dies before completion.
    """
    try:
        execution_id = _execution_id_for(executor)
        entry = {
            "id": execution_id,
            "started_at": time.time(),
            "env": _collect_env(),
            "context": _collect_context(),
            "env_vars": _collect_environment_vars(),
            "workload": _collect_workload(logical_plan),
            "performance": None,
            "error": None,
            # Bookkeeping for cluster spillage delta; stripped before flush.
            "_spilled_at_start": _cluster_spilled_bytes(),
        }
        with _lock:
            _executions[execution_id] = entry
            _flush_locked()
    except Exception:
        logger.debug("Failed to record workload telemetry", exc_info=True)


def record_execution_result(executor, error: Optional[BaseException]) -> None:
    """Fill in performance, error for a previously recorded execution and flush."""
    try:
        execution_id = _execution_id_for(executor)
        with _lock:
            entry = _executions.get(execution_id)
            if entry is None:
                return
            spilled_at_start = entry.pop("_spilled_at_start", None)
            entry["performance"] = _collect_pipeline_perf(executor, spilled_at_start)
            entry["error"] = _collect_error(error)
            _flush_locked()
    except Exception:
        logger.debug("Failed to record execution result telemetry", exc_info=True)


def _execution_id_for(executor) -> str:
    """Compose a per-execution id from the executor.

    Reuses Ray Data's existing per-execution identity: the dataset UUID plus
    the run index from ``ExecutionPlan``. The full ``executor._dataset_id``
    embeds the user-supplied dataset name, which we strip to avoid leaking
    identifying info.
    """
    # Format from ExecutionPlan.get_dataset_id: "{name}_{uuid}_{run_index}".
    parts = executor._dataset_id.rsplit("_", 2)
    if len(parts) == 3:
        return f"{parts[1]}_{parts[2]}"
    return executor._dataset_id


def _flush_locked() -> None:
    """Serialize current state and push to GCS. Caller must hold ``_lock``."""
    payload = json.dumps({"executions": list(_executions.values())})
    record_extra_usage_tag(TagKey.DATA_TELEMETRY, payload)


def _collect_env() -> dict:
    """Collect process-wide environment info. Memoized after the first call."""
    global _env_cache
    if _env_cache is not None:
        return _env_cache

    deps = {pkg: _safe_version(pkg) for pkg in _TRACKED_DEPS}
    third_party = {pkg: _safe_version(pkg) for pkg in _TRACKED_THIRD_PARTY}
    _env_cache = {"deps": deps, "third_party": third_party}
    return _env_cache


def _safe_version(pkg: str) -> Optional[str]:
    try:
        return importlib.metadata.version(pkg)
    except importlib.metadata.PackageNotFoundError:
        return None
    except Exception:
        return None


def _collect_context() -> dict:
    """Whitelisted DataContext fields. Empty for v1; expanded in follow-ups."""
    return {}


def _collect_environment_vars() -> dict:
    """Whitelisted RAY_DATA_* environment variables. Empty for v1."""
    return {}


def _collect_workload(logical_plan) -> dict:
    """Anonymized plan + per-op config."""
    dag = _root(logical_plan)
    ops: List[dict] = []
    _walk_operators(dag, ops)
    return {
        "plan": "->".join(op["name"] for op in ops),
        "ops": ops,
        "input": _collect_input_layout(dag),
    }


def _root(logical_plan):
    """Return the root LogicalOperator from a LogicalPlan or operator."""
    if isinstance(logical_plan, LogicalOperator):
        return logical_plan
    return logical_plan.dag


def _walk_operators(op: LogicalOperator, out: List[dict]) -> None:
    """Post-order walk producing anonymized op names + per-op config."""
    for child in op.input_dependencies:
        _walk_operators(child, out)

    name = _anonymize_op_name(op)
    entry: Dict[str, Any] = {"name": name}
    if isinstance(op, AbstractUDFMap):
        config: Dict[str, Any] = {}
        compute = getattr(op, "compute", None)
        if compute is not None:
            config["compute"] = type(compute).__name__
        ray_remote_args = getattr(op, "ray_remote_args", None) or {}
        if "num_cpus" in ray_remote_args:
            config["num_cpus"] = ray_remote_args["num_cpus"]
        if "num_gpus" in ray_remote_args:
            config["num_gpus"] = ray_remote_args["num_gpus"]
        batch_size = getattr(op, "batch_size", None)
        if batch_size is not None:
            config["batch_size"] = batch_size
        batch_format = getattr(op, "batch_format", None)
        if batch_format is not None:
            config["batch_format"] = batch_format
        if config:
            entry["config"] = config
    out.append(entry)


def _anonymize_op_name(op: LogicalOperator) -> str:
    """Reuse the existing whitelist logic from ``logical/util.py``."""
    if isinstance(op, Read):
        name = f"Read{op.datasource.get_name()}"
        return name if name in _op_name_white_list else "ReadCustom"
    if isinstance(op, Write):
        name = f"Write{op.datasink_or_legacy_datasource.get_name()}"
        return name if name in _op_name_white_list else "WriteCustom"
    if isinstance(op, AbstractUDFMap):
        return _anonymize_op_name_str(op.name)
    return _anonymize_op_name_str(op.name)


def _anonymize_op_name_str(name: Optional[str]) -> str:
    """Anonymize an operator name string against the whitelist.

    Used for physical-operator names from ``OperatorStatsSummary`` and
    ``_topology`` where we only have a string. UDF suffixes like
    ``MapBatches(my_fn)`` are stripped before whitelist lookup.
    """
    if not name:
        return "Unknown"
    bare = re.sub(r"\(.*\)$", "", name).strip()
    return bare if bare in _op_name_white_list else "Unknown"


def _collect_input_layout(root: LogicalOperator) -> dict:
    """Aggregate input file metadata across all Read operators in the plan.

    Uses ``Read._cached_output_metadata`` so we don't trigger extra work. Only
    totals are reported in v1; per-file percentiles are a follow-up.
    """
    num_files = 0
    total_bytes = 0
    total_rows = 0
    bytes_known = False
    rows_known = False

    for op in _iter_reads(root):
        try:
            meta_with_schema = op._cached_output_metadata
            meta = meta_with_schema.metadata
        except Exception:
            continue
        if meta.input_files is not None:
            num_files += len(meta.input_files)
        if meta.size_bytes is not None:
            total_bytes += meta.size_bytes
            bytes_known = True
        if meta.num_rows is not None:
            total_rows += meta.num_rows
            rows_known = True

    return {
        "num_files": num_files,
        "total_bytes": total_bytes if bytes_known else None,
        "total_rows": total_rows if rows_known else None,
    }


def _iter_reads(op: LogicalOperator):
    if isinstance(op, Read):
        yield op
    for child in op.input_dependencies:
        yield from _iter_reads(child)


def _collect_pipeline_perf(executor, spilled_at_start: Optional[int]) -> dict:
    """Aggregate pipeline-level perf.

    ``bytes_spilled`` is a cluster-wide delta sourced from Ray core's
    ``store_stats.spilled_bytes_total`` (the same path ``plan.py:259-265``
    uses for ``global_bytes_spilled``). It approximates per-execution
    spillage well for single-tenant clusters; under concurrent workloads
    it overcounts. ``OpRuntimeMetrics.obj_store_mem_spilled`` was tried
    first but undercounted by ~5 orders of magnitude — it only increments
    when a task reads back a previously-spilled input block, missing all
    terminal-stage spillage.

    OOM / worker / node-death counts are emitted as ``None`` — see the
    plan's "Follow-ups" section for the C++-only-counter rationale.
    """
    try:
        summary = executor.get_stats().to_summary()
    except Exception:
        summary = None

    try:
        wall = summary.get_total_wall_time() if summary is not None else None
    except Exception:
        wall = None

    spilled_now = _cluster_spilled_bytes()
    if spilled_at_start is None or spilled_now is None:
        bytes_spilled = None
    else:
        bytes_spilled = max(0, spilled_now - spilled_at_start)

    return {
        "bytes_spilled": bytes_spilled,
        "total_wall_time_s": wall,
        "oom_kills": None,
        "unexpected_worker_kills": None,
        "node_deaths": None,
        "per_op": _collect_per_op_perf(executor, summary),
    }


def _collect_per_op_perf(executor, summary) -> List[dict]:
    """Per-operator stats from already-computed ``OperatorStatsSummary``.

    All values are aggregations over the **output blocks** an op produced
    during this execution — already collected by Ray Data on the hot path
    regardless of whether we read them. We only read them post-shutdown.

    Caveats: ``memory_mib`` is Linux-only (the underlying ``max_uss_bytes``
    returns 0 on macOS/Windows). ``num_tasks_failed`` counts intermediate
    task failures, including ones Ray retried successfully — it's a noisy
    proxy for OOM/worker kills.
    """
    if summary is None:
        return []

    op_metrics_by_name: Dict[str, Any] = {}
    try:
        for op in getattr(executor, "_topology", []) or []:
            try:
                op_metrics_by_name[op.name] = op.metrics
            except Exception:
                continue
    except Exception:
        logger.debug("Failed to walk executor topology for per-op metrics", exc_info=True)

    per_op: List[dict] = []
    for op_stat in getattr(summary, "operators_stats", []) or []:
        try:
            entry: Dict[str, Any] = {
                "name": _anonymize_op_name_str(op_stat.operator_name),
                "is_sub_operator": op_stat.is_sub_operator,
            }
            if op_stat.wall_time is not None:
                entry["wall_time_s"] = _stats_summary_to_dict(op_stat.wall_time)
            if op_stat.udf_time is not None:
                entry["udf_time_s"] = _stats_summary_to_dict(op_stat.udf_time)
            if op_stat.memory is not None:
                entry["memory_mib"] = _stats_summary_to_dict(op_stat.memory)

            metrics = op_metrics_by_name.get(op_stat.operator_name)
            if metrics is not None:
                entry["num_tasks_finished"] = getattr(metrics, "num_tasks_finished", None)
                entry["num_tasks_failed"] = getattr(metrics, "num_tasks_failed", None)

            per_op.append(entry)
        except Exception:
            logger.debug("Failed per-op stats collection", exc_info=True)
            continue
    return per_op


def _stats_summary_to_dict(s) -> dict:
    """Convert a ``StatsSummary`` to a compact min/mean/max dict."""
    return {"min": s.min, "mean": s.mean, "max": s.max}


def _cluster_spilled_bytes() -> Optional[int]:
    """Cluster-wide cumulative spilled bytes from Ray core's store_stats.

    Returns None on any failure — telemetry must never break execution.
    """
    try:
        import ray
        from ray._private.internal_api import (
            get_memory_info_reply,
            get_state_from_address,
        )

        reply = get_memory_info_reply(
            get_state_from_address(ray.get_runtime_context().gcs_address)
        )
        return int(reply.store_stats.spilled_bytes_total)
    except Exception:
        logger.debug("Failed to read cluster spilled bytes", exc_info=True)
        return None


def _collect_error(error: Optional[BaseException]) -> dict:
    return {"type": type(error).__name__ if error is not None else None}


def _reset_for_testing() -> None:
    """Reset module state. Tests only."""
    global _env_cache
    with _lock:
        _executions.clear()
        _env_cache = None
