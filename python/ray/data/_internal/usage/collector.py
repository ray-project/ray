"""Ray Data usage-stats collector.

Accumulates per-execution usage data (environment, workload description,
performance) and flushes it to GCS via ``record_extra_usage_tag``.
"""

import importlib.metadata
import json
import logging
import os
import threading
import time
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import ray
from ray._common.usage.usage_lib import (
    TagKey,
    record_extra_usage_tag,
    usage_stats_enabled,
)
from ray._private.internal_api import get_memory_info_reply, get_state_from_address
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators import AbstractUDFMap
from ray.data._internal.logical.util import anonymize_op_name
from ray.data.block import VALID_BATCH_FORMATS, _apply_batch_format

if TYPE_CHECKING:
    from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _OpConfig:
    """Configuration for an operator"""

    batch_format: Optional[str] = None


@dataclass(frozen=True)
class _Op:
    """An operator in the plan"""

    name: str
    config: Optional[_OpConfig] = None


@dataclass(frozen=True)
class _PlanNode:
    """A node in the anonymized plan tree (one logical operator)."""

    op: str
    inputs: List["_PlanNode"] = field(default_factory=list)


@dataclass(frozen=True)
class _Workload:
    """The anonymized plan tree, a human-readable rendering of it, and the
    per-op flat list (with config)."""

    plan: _PlanNode
    plan_str: str
    ops: List[_Op]


@dataclass(frozen=True)
class _Env:
    pyarrow: Optional[str]


@dataclass(frozen=True)
class _PipelinePerf:
    bytes_spilled: Optional[int]
    oom_kills: Optional[int] = None
    unexpected_worker_kills: Optional[int] = None
    node_deaths: Optional[int] = None


@dataclass
class _Entry:
    id: str
    started_at: float
    env: _Env
    workload: _Workload
    performance: Optional[_PipelinePerf] = None


# Bounded buffer of recent executions. OrderedDict so eviction picks the
# oldest-inserted entry
_MAX_EXECUTIONS_TO_TRACK = 100

# Module state. Mutations are serialized through ``_lock``.
_executions: "OrderedDict[str, _Entry]" = OrderedDict()
# Per-execution spillage recorded at start of execution to compute delta
_spillage_dict: Dict[str, Optional[int]] = {}
_lock = threading.Lock()


def record_workload(
    execution_id: str,
    logical_plan: "LogicalPlan",
) -> None:
    """Record the planning-time workload entry for an execution.
    This consists of the DAG, env, and configs for each operator.
    Flushes eagerly so that attempted executions are captured even if
    the execution fails.

    Short-circuits when the user has opted out of Ray usage stats (via
    ``RAY_USAGE_STATS_ENABLED=0``, ``ray disable-usage-stats``, or
    ``~/.ray/config.json``) or when ``RAY_DATA_USAGE_DISABLED=1`` is set.
    """
    if not usage_stats_enabled() or os.environ.get("RAY_DATA_USAGE_DISABLED") == "1":
        return
    try:
        entry = _Entry(
            id=execution_id,
            started_at=time.time(),
            env=_collect_env(),
            workload=_collect_workload(logical_plan),
        )
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
    execution_id: str,
) -> None:
    """Fill in performance for a previously recorded execution and flush.

    Short-circuits when the user has opted out of Ray usage stats (via
    ``RAY_USAGE_STATS_ENABLED=0``, ``ray disable-usage-stats``, or
    ``~/.ray/config.json``) or when ``RAY_DATA_USAGE_DISABLED=1`` is set.
    """
    if not usage_stats_enabled() or os.environ.get("RAY_DATA_USAGE_DISABLED") == "1":
        return
    try:
        spilled_now = _cluster_spilled_bytes()
        with _lock:
            entry = _executions.get(execution_id)
            if entry is None:  # if the execution was not found (could be evicted)
                _spillage_dict.pop(execution_id, None)
                return
            spilled_at_start = _spillage_dict.pop(execution_id, None)
            entry.performance = _collect_pipeline_perf(spilled_at_start, spilled_now)
            payload = _serialize_locked()
        record_extra_usage_tag(TagKey.DATA_USAGE, payload)
    except Exception:
        logger.debug("Failed to record execution result usage", exc_info=True)


def _serialize_locked() -> str:
    """Serialize current state to JSON. Caller must hold ``_lock``."""
    return json.dumps({"executions": [asdict(e) for e in _executions.values()]})


def _collect_env() -> _Env:
    """Process-wide environment info."""
    return _Env(pyarrow=_safe_version("pyarrow"))


def _safe_version(pkg: str) -> Optional[str]:
    try:
        return importlib.metadata.version(pkg)
    except importlib.metadata.PackageNotFoundError:
        return None


def _collect_workload(logical_plan: "LogicalPlan") -> _Workload:
    """Collect anonymized plan tree, indented text rendering, and per-op
    config list."""
    dag = logical_plan.dag
    plan, ops = _build_plan_and_ops(dag)
    return _Workload(
        plan=plan,
        plan_str=_format_plan_str(dag),
        ops=ops,
    )


def _build_plan_and_ops(op: LogicalOperator) -> Tuple[_PlanNode, List[_Op]]:
    """Build plan tree and flat op list in one post-order walk."""
    child_plans: List[_PlanNode] = []
    ops: List[_Op] = []
    for child in op.input_dependencies:
        child_plan, child_ops = _build_plan_and_ops(child)
        child_plans.append(child_plan)
        ops.extend(child_ops)

    name = anonymize_op_name(op)
    config: Optional[_OpConfig] = None
    if isinstance(op, AbstractUDFMap):
        batch_format = getattr(op, "batch_format", None)
        if batch_format == "default":
            batch_format = _apply_batch_format(batch_format)
        if batch_format in VALID_BATCH_FORMATS:
            config = _OpConfig(batch_format=batch_format)
        else:
            logger.debug(f"Unexpected batch format: {batch_format!r}")
            config = _OpConfig(batch_format="unknown")
    ops.append(_Op(name=name, config=config))
    return _PlanNode(op=name, inputs=child_plans), ops


def _format_plan_str(op: LogicalOperator, depth: int = 0) -> str:
    """Render the anonymized DAG as an indented tree, using ``anonymize_op_name`` to
    avoid leaking UDF / datasource details.
    """
    name = anonymize_op_name(op)
    if depth == 0:
        line = f"{name}\n"
    else:
        line = f"{' ' * ((depth - 1) * 3)}+- {name}\n"
    for child in op.input_dependencies:
        line += _format_plan_str(child, depth + 1)
    return line


def _collect_pipeline_perf(
    spilled_at_start: Optional[int],
    spilled_now: Optional[int],
) -> _PipelinePerf:
    """Pipeline-level perf metrics

    ``bytes_spilled`` is a cluster-wide delta sourced from Ray core's
    ``store_stats.spilled_bytes_total``, ``spilled_now`` is computed by the caller
    via a synchronous gRPC.
    """
    if spilled_at_start is None or spilled_now is None:
        bytes_spilled = None
    else:
        bytes_spilled = max(0, spilled_now - spilled_at_start)

    return _PipelinePerf(bytes_spilled=bytes_spilled)


def _cluster_spilled_bytes() -> Optional[int]:
    """Cluster-wide cumulative spilled bytes from Ray core's store_stats.

    Returns None on any failure — usage collection must never break execution.
    """
    try:
        reply = get_memory_info_reply(
            get_state_from_address(ray.get_runtime_context().gcs_address),
            timeout_seconds=10.0,
        )
        return int(reply.store_stats.spilled_bytes_total)
    except Exception:
        logger.debug("Failed to read cluster spilled bytes", exc_info=True)
        return None


def reset_for_testing() -> None:
    """Reset module state. Tests only."""
    with _lock:
        _executions.clear()
        _spillage_dict.clear()


def get_executions() -> "OrderedDict[str, _Entry]":
    """Get the current executions. Tests only."""
    with _lock:
        return _executions.copy()
