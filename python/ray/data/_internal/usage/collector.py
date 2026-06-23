"""Ray Data usage-stats collector.

Accumulates per-execution usage data (environment, workload description,
performance) and flushes it to GCS via ``record_extra_usage_tag``.
"""

import hashlib
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
from ray.data._internal.logical.operators import MapBatches
from ray.data._internal.logical.util import anonymize_op_name
from ray.data.block import VALID_BATCH_FORMATS, _apply_batch_format

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.issue_detection.issue_detector import IssueType
    from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _OpConfig:
    """Configuration for an operator"""

    batch_format: Optional[str] = None


@dataclass(frozen=True)
class _LogicalOp:
    """An operator in the plan"""

    usage_uuid: str
    name: str
    config: Optional[_OpConfig] = None


@dataclass(frozen=True)
class _PlanNode:
    """A node in the anonymized plan tree (one logical operator)."""

    usage_uuid: str
    op: str
    inputs: List["_PlanNode"] = field(default_factory=list)


@dataclass(frozen=True)
class _Workload:
    """The anonymized plan tree, a human-readable rendering of it, and the
    per-op flat list (with config)."""

    plan: _PlanNode
    plan_str: str
    ops: List[_LogicalOp]


@dataclass(frozen=True)
class _Env:
    pyarrow: Optional[str]


@dataclass(frozen=True)
class _PipelinePerf:
    bytes_spilled: Optional[int]
    oom_kills: Optional[int] = None
    unexpected_worker_kills: Optional[int] = None
    node_deaths: Optional[int] = None


@dataclass(frozen=True)
class _Issue:
    """An issue detected during execution, tied to an anonymized operator."""

    issue_type: str
    operator: str


@dataclass
class _Entry:
    id: str
    started_at: float
    env: _Env
    workload: _Workload
    performance: Optional[_PipelinePerf] = None
    detected_issues: List[_Issue] = field(default_factory=list)


# Bounded buffer of recent executions. OrderedDict so eviction picks the
# oldest-inserted entry
_MAX_EXECUTIONS_TO_TRACK = 100

# Module state. Mutations are serialized through ``_lock``.
_executions: "OrderedDict[str, _Entry]" = OrderedDict()
# Per-execution spillage recorded at start of execution to compute delta
_spillage_dict: Dict[str, Optional[int]] = {}
_lock = threading.Lock()


def _usage_collection_disabled() -> bool:
    """True when the user has opted out of usage stats (via
    ``RAY_USAGE_STATS_ENABLED=0``, ``ray disable-usage-stats``, or
    ``~/.ray/config.json``) or when ``RAY_DATA_USAGE_DISABLED=1`` is set.
    """
    return not usage_stats_enabled() or os.environ.get("RAY_DATA_USAGE_DISABLED") == "1"


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
    if _usage_collection_disabled():
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


def build_usage_uuid_map(logical_plan: "LogicalPlan") -> Dict[int, str]:
    """Build the ``id(logical_op) -> usage_uuid`` map for a plan.

    The issue detector uses this to label operators with the same usage UUIDs
    embedded in the recorded workload payload, so detected issues reference
    the operators in that payload. The UUIDs are computed based on the hash of the (post-order index, anonymized name) tuple.

    Short-circuits to an empty map when the user has opted out of usage stats:
    without a recorded payload there is nothing for the UUIDs to reference.
    """
    if _usage_collection_disabled():
        return {}
    try:
        ordered_logical_ops: List[Tuple[LogicalOperator, str]] = []
        _build_plan(logical_plan.dag, ordered_logical_ops)
        return {id(op): usage_uuid for op, usage_uuid in ordered_logical_ops}
    except Exception:
        logger.debug("Failed to build usage uuid map", exc_info=True)
        return {}


def physical_op_name_with_uuid(
    operator: "PhysicalOperator",
    usage_uuid_map: Optional[Dict[int, str]] = None,
) -> str:
    """Anonymized name for a physical op, used to label detected issues so they
    correlate with the recorded workload payload. Fused ops join their logical
    ops with "->" (matching operator fusion's naming); each logical op is
    formatted as ``<name>-<usage_uuid>`` when a UUID is available, else just
    ``<name>``. ``"Unknown"`` when the op has no logical operators."""
    logical_ops = operator._logical_operators
    if not logical_ops:
        return "Unknown"
    return "->".join(
        _logical_op_name_with_uuid(op, usage_uuid_map) for op in logical_ops
    )


def _logical_op_name_with_uuid(
    logical_op,
    usage_uuid_map: Optional[Dict[int, str]] = None,
) -> str:
    name = anonymize_op_name(logical_op)
    if usage_uuid_map:
        # Correlate with the UUIDs assigned to the logical ops in the workload plan.
        usage_uuid = usage_uuid_map.get(id(logical_op))
        if usage_uuid is not None:
            return f"{name}-{usage_uuid}"
    return name


def record_execution_result(
    execution_id: str,
    detected_issues: Optional[List[Tuple["IssueType", str]]] = None,
) -> None:
    """Fill in performance and detected issues for a previously recorded
    execution and flush.

    ``detected_issues`` is a list of ``(issue_type, anonymized_operator_name)``
    pairs surfaced by the issue detectors during execution.

    Short-circuits when the user has opted out of Ray usage stats (via
    ``RAY_USAGE_STATS_ENABLED=0``, ``ray disable-usage-stats``, or
    ``~/.ray/config.json``) or when ``RAY_DATA_USAGE_DISABLED=1`` is set.
    """
    if _usage_collection_disabled():
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
            entry.detected_issues = _collect_issues(detected_issues)
            payload = _serialize_locked()
        record_extra_usage_tag(TagKey.DATA_USAGE, payload)
    except Exception:
        logger.debug("Failed to record execution result usage", exc_info=True)


def _collect_issues(
    detected_issues: Optional[List[Tuple["IssueType", str]]],
) -> List[_Issue]:
    """Convert (issue_type, operator) pairs into ``_Issue`` records, mapping
    each ``IssueType`` enum to its string value."""
    if not detected_issues:
        return []
    return [
        _Issue(issue_type=issue_type.value, operator=operator)
        for issue_type, operator in detected_issues
    ]


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
    """Collect the anonymized plan tree, indented text rendering, and per-op
    config list in a single DAG walk."""
    dag = logical_plan.dag
    ordered_logical_ops: List[Tuple[LogicalOperator, str]] = []
    plan = _build_plan(dag, ordered_logical_ops)
    return _Workload(
        plan=plan,
        plan_str=_format_plan_str(dag),
        ops=_build_ops(ordered_logical_ops),
    )


def _build_plan(
    op: LogicalOperator,
    ordered_logical_ops: List[Tuple[LogicalOperator, str]],
) -> _PlanNode:
    """Build the plan tree and record logical ops in post-order."""
    child_plans: List[_PlanNode] = []
    for child in op.input_dependencies:
        child_plans.append(_build_plan(child, ordered_logical_ops))

    name = anonymize_op_name(op)
    usage_uuid = _make_usage_op_uuid(len(ordered_logical_ops), name)
    ordered_logical_ops.append((op, usage_uuid))
    return _PlanNode(usage_uuid=usage_uuid, op=name, inputs=child_plans)


def _build_ops(
    ordered_logical_ops: List[Tuple[LogicalOperator, str]],
) -> List[_LogicalOp]:
    """Build the flat logical-op list from the canonical post-order traversal."""
    ops: List[_LogicalOp] = []
    for op, usage_uuid in ordered_logical_ops:
        name = anonymize_op_name(op)
        ops.append(
            _LogicalOp(
                usage_uuid=usage_uuid,
                name=name,
                config=_get_op_config(op),
            )
        )
    return ops


def _get_op_config(op: LogicalOperator) -> Optional[_OpConfig]:
    # MapBatches is the only operator with a user-facing batch_format.
    if not isinstance(op, MapBatches):
        return None
    batch_format = op.batch_format
    if batch_format == "default":
        batch_format = _apply_batch_format(batch_format)
    if batch_format in VALID_BATCH_FORMATS:
        return _OpConfig(batch_format=batch_format)
    logger.debug(f"Unexpected batch format: {batch_format!r}")
    return _OpConfig(batch_format="unknown")


def _make_usage_op_uuid(index: int, name: str) -> str:
    return hashlib.sha256(f"{index}:{name}".encode()).hexdigest()[:4]


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
