"""Ray Data usage-stats collector.

Accumulates per-execution usage data (environment, workload description,
performance) and flushes it to GCS via ``record_extra_usage_tag``.
"""

import importlib.metadata
import json
import logging
import os
import re
import threading
import time
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators import AbstractUDFMap, Read, Write
from ray.data._internal.logical.util import _op_name_white_list

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor
    from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan
    from ray.data._internal.stats import DatasetStatsSummary, StatsSummary

logger = logging.getLogger(__name__)

# Bounded buffer of recent executions. OrderedDict so eviction picks the
# oldest-inserted entry
_MAX_EXECUTIONS_TO_TRACK = 100

# Module state. Mutations are serialized through ``_lock``.
_executions: "OrderedDict[str, dict]" = OrderedDict()
# Per-execution spillage recorded at start of execution to compute delta
_spillage_dict: Dict[str, Optional[int]] = {}
_env_cache: Optional[dict] = None
_lock = threading.Lock()

# Dependency packages whose version we always record.
_TRACKED_DEPS = ("pyarrow", "numpy", "pandas", "ray")

# Third-party libraries whose presence (and version, if present) we record.
_TRACKED_THIRD_PARTY = ("daft", "vllm", "sglang", "deepspeed")

# Allowlist of RAY_DATA_* env var names. Values are recorded alongside the
# name since every knob in context.py is bool/int/enum (no paths or
# credentials). New entries here should be added with that same constraint.
_ENV_VAR_WHITELIST = (
    "RAY_DATA_PUSH_BASED_SHUFFLE",
    "RAY_DATA_DEFAULT_SHUFFLE_STRATEGY",
    "RAY_DATA_MAX_HASH_SHUFFLE_AGGREGATORS",
    "RAY_DATA_EAGER_FREE",
    "RAY_DATA_DEFAULT_MIN_PARALLELISM",
    "RAY_DATA_ENABLE_TENSOR_EXTENSION_CASTING",
    "RAY_DATA_USE_ARROW_TENSOR_V2",
    "RAY_DATA_TRACE_ALLOCATIONS",
    "RAY_DATA_DISABLE_PROGRESS_BARS",
    "RAY_DATA_ENABLE_RICH_PROGRESS_BARS",
    "RAY_DATA_ENFORCE_SCHEMAS",
    "RAY_DATA_LOG_INTERNAL_STACK_TRACE_TO_STDOUT",
    "RAY_DATA_RAISE_ORIGINAL_MAP_EXCEPTION",
    "RAY_DATA_PANDAS_BLOCK_IGNORE_METADATA",
    "RAY_DATA_DEFAULT_BATCH_TO_BLOCK_ARROW_FORMAT",
    "RAY_DATA_PER_NODE_METRICS",
    "RAY_DATA_DEFAULT_WAIT_FOR_MIN_ACTORS_S",
    "RAY_DATA_OP_RESERVATION_RATIO",
    "RAY_DATA_ENABLE_OP_RESOURCE_RESERVATION",
    "RAY_DATA_MIN_HASH_SHUFFLE_AGGREGATOR_WAIT_TIME_IN_S",
    "RAY_DATA_HASH_SHUFFLE_AGGREGATOR_HEALTH_WARNING_INTERVAL_S",
    "RAY_DATA_DEFAULT_ACTOR_POOL_UTIL_UPSCALING_THRESHOLD",
    "RAY_DATA_DEFAULT_ACTOR_POOL_UTIL_DOWNSCALING_THRESHOLD",
    "RAY_DATA_ENABLE_DYNAMIC_OUTPUT_QUEUE_SIZE_BACKPRESSURE",
    "RAY_DATA_DOWNSTREAM_CAPACITY_BACKPRESSURE_RATIO",
)

# Allowlist of DataContext field names. All are primitive-typed
# (bool/int/float/str/enum) in context.py. Non-primitive configs
# (execution_options, iceberg_config, lance_config) and user-supplied
# fields (dataset_logger_id) are deliberately omitted.
_CONTEXT_WHITELIST = (
    "target_max_block_size",
    "target_min_block_size",
    "streaming_read_buffer_size",
    "enable_pandas_block",
    "actor_prefetcher_enabled",
    "use_push_based_shuffle",
    "shuffle_strategy",
    "scheduling_strategy",
    "scheduling_strategy_large_args",
    "large_args_threshold",
    "use_polars",
    "eager_free",
    "min_parallelism",
    "read_op_min_num_blocks",
    "enable_progress_bars",
)


def record_workload(
    executor: "StreamingExecutor",
    logical_plan: Union["LogicalPlan", LogicalOperator],
) -> None:
    """Record the planning-time entry for an execution.

    Called from ``_execute_dag`` before the executor runs. The resulting entry
    sits in ``_executions`` with ``performance: None`` and is filled in by
    ``record_execution_result`` after the executor finishes (success or fail).
    Flushes eagerly so attempted executions are captured even if the process
    dies before completion.

    Set ``RAY_DATA_USAGE_DISABLED=1`` to short-circuit all collection.
    """
    if os.environ.get("RAY_DATA_USAGE_DISABLED") == "1":
        return
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
        }
        spilled_at_start = _cluster_spilled_bytes()
        with _lock:
            if len(_executions) >= _MAX_EXECUTIONS_TO_TRACK:
                evicted_id, _ = _executions.popitem(last=False)
                _spillage_dict.pop(evicted_id, None)
            _executions[execution_id] = entry
            _spillage_dict[execution_id] = spilled_at_start
            payload = _serialize_locked()
        record_extra_usage_tag(TagKey.DATA_USAGE, payload)
    except Exception:
        logger.debug("Failed to record workload usage", exc_info=True)


def record_execution_result(
    executor: "StreamingExecutor",
    error: Optional[BaseException],
) -> None:
    """Fill in performance, error for a previously recorded execution and flush.

    Set ``RAY_DATA_USAGE_DISABLED=1`` to short-circuit all collection.
    """
    if os.environ.get("RAY_DATA_USAGE_DISABLED") == "1":
        return
    try:
        execution_id = _execution_id_for(executor)
        spilled_now = _cluster_spilled_bytes()
        with _lock:
            entry = _executions.get(execution_id)
            if entry is None:  # if the execution was not found (could be evicted)
                _spillage_dict.pop(execution_id, None)
                return
            spilled_at_start = _spillage_dict.pop(execution_id, None)
            entry["performance"] = _collect_pipeline_perf(
                executor, spilled_at_start, spilled_now
            )
            entry["error"] = _collect_error(error)
            payload = _serialize_locked()
        record_extra_usage_tag(TagKey.DATA_USAGE, payload)
    except Exception:
        logger.debug("Failed to record execution result usage", exc_info=True)


def _execution_id_for(executor: "StreamingExecutor") -> str:
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


def _serialize_locked() -> str:
    """Serialize current state to JSON. Caller must hold ``_lock``.

    The actual GCS write (``record_extra_usage_tag``) is done outside the
    lock by the caller — it's a synchronous gRPC and there's no reason to
    serialize concurrent flushes on it.
    """
    return json.dumps({"executions": list(_executions.values())})


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


def _collect_context() -> Dict[str, Any]:
    """Whitelisted DataContext fields.

    Re-read on every workload (no memoization) because DataContext is
    mutable mid-process — users can ``DataContext.get_current().X = Y``
    between executions.
    """
    try:
        from ray.data import DataContext  # local to avoid import cycle

        ctx = DataContext.get_current()
    except Exception:
        return {}
    result: Dict[str, Any] = {}
    for name in _CONTEXT_WHITELIST:
        try:
            value = getattr(ctx, name, None)
        except Exception:
            continue
        if value is None or isinstance(value, (bool, int, float, str)):
            result[name] = value
        else:
            # Enums (e.g. ShuffleStrategy) — coerce to a stable string form.
            result[name] = str(value)
    return result


def _collect_environment_vars() -> Dict[str, str]:
    """Whitelisted RAY_DATA_* env vars actually set in the environment.

    Records ``name -> value`` only for entries in ``_ENV_VAR_WHITELIST``
    that are set. Unset entries are omitted. Values are strings (env vars
    always are); downstream consumers parse to bool/int as needed.
    """
    result: Dict[str, str] = {}
    for name in _ENV_VAR_WHITELIST:
        value = os.environ.get(name)
        if value is not None:
            result[name] = value
    return result


def _collect_workload(
    logical_plan: Union["LogicalPlan", LogicalOperator],
) -> dict:
    """Anonymized plan + per-op config."""
    dag = _root(logical_plan)
    ops: List[dict] = []
    _walk_operators(dag, ops)
    return {
        "plan": "->".join(op["name"] for op in ops),
        "ops": ops,
        "input": _collect_input_layout(dag),
    }


def _root(
    logical_plan: Union["LogicalPlan", LogicalOperator],
) -> LogicalOperator:
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


def _iter_reads(op: LogicalOperator) -> Iterator[Read]:
    if isinstance(op, Read):
        yield op
    for child in op.input_dependencies:
        yield from _iter_reads(child)


def _collect_pipeline_perf(
    executor: "StreamingExecutor",
    spilled_at_start: Optional[int],
    spilled_now: Optional[int],
) -> dict:
    """Aggregate pipeline-level perf.

    ``spilled_now`` is passed in so the caller can read it outside the
    lock — it's a synchronous gRPC and we don't want it held under the
    collector lock.

    ``bytes_spilled`` is a cluster-wide delta sourced from Ray core's
    ``store_stats.spilled_bytes_total`` (the same path ``plan.py:259-265``
    uses for ``global_bytes_spilled``).
    """
    try:
        summary = executor.get_stats().to_summary()
    except Exception:
        summary = None

    try:
        wall = summary.get_total_wall_time() if summary is not None else None
    except Exception:
        wall = None

    if spilled_at_start is None or spilled_now is None:
        bytes_spilled = None
    else:
        bytes_spilled = max(0, spilled_now - spilled_at_start)

    return {
        "bytes_spilled": bytes_spilled,
        "total_wall_time_s": wall,
        "per_op": _collect_per_op_perf(summary),
        # TODO(ayushk7102): add OOM / worker / node death counts.
        "oom_kills": None,
        "unexpected_worker_kills": None,
        "node_deaths": None,
    }


def _collect_per_op_perf(summary: Optional["DatasetStatsSummary"]) -> List[dict]:
    """Per-operator stats from already-computed ``OperatorStatsSummary``.

    All values are aggregations over output blocks — already collected by
    Ray Data on the hot path regardless of whether we read them.

    Caveat: ``memory_mib`` is Linux-only (``max_uss_bytes`` is 0 on
    macOS/Windows).
    """
    if summary is None:
        return []

    per_op: List[dict] = []
    operators_stats = getattr(summary, "operators_stats", None) or []
    for idx, op_stat in enumerate(operators_stats):
        try:
            # TODO(ayushk7102): verify if this logic works for fused-ops
            name = _anonymize_op_name_str(op_stat.operator_name)
            entry: Dict[str, Any] = {
                "name": name,
                "op_id": f"{name}_{idx}",  # ensure unique op_id if names collide
            }
            if op_stat.wall_time is not None:
                entry["wall_time_s"] = _stats_summary_to_dict(op_stat.wall_time)
            if op_stat.udf_time is not None:
                entry["udf_time_s"] = _stats_summary_to_dict(op_stat.udf_time)
            if op_stat.memory is not None:
                entry["memory_mib"] = _stats_summary_to_dict(op_stat.memory)
            per_op.append(entry)
        except Exception:
            logger.debug("Failed per-op stats collection", exc_info=True)
            continue
    return per_op


def _stats_summary_to_dict(s: "StatsSummary") -> dict:
    """Convert a ``StatsSummary`` to a compact min/mean/max dict."""
    return {"min": s.min, "mean": s.mean, "max": s.max}


def _cluster_spilled_bytes() -> Optional[int]:
    """Cluster-wide cumulative spilled bytes from Ray core's store_stats.

    Returns None on any failure — usage collection must never break execution.
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
        _spillage_dict.clear()
        _env_cache = None
