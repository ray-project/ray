"""Ray Data usage-stats collector.

Accumulates per-execution usage data (environment, workload description,
performance) and flushes it to GCS via ``record_extra_usage_tag``.

The usage payload for each execution is assembled by :class:`UsageCallback`
this module owns the process-global buffer of recent executions and the builder functions
collecting usage data.
"""

import hashlib
import importlib.metadata
import json
import logging
import os
import threading
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from functools import cache
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple

import ray
from ray._common.usage.usage_lib import (
    TagKey,
    record_extra_usage_tag,
    usage_stats_enabled,
)
from ray._private.internal_api import get_memory_info_reply, get_state_from_address
from ray._private.worker import global_worker
from ray.core.generated.gcs_pb2 import GcsNodeInfo
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

# Bounded timeout for the GCS get_all_node_info query used to count dead nodes.
_NODE_INFO_RPC_TIMEOUT_S = 5.0


@dataclass(frozen=True)
class OpConfig:
    """Configuration for an operator"""

    batch_format: Optional[str] = None


@dataclass(frozen=True)
class LogicalOp:
    """An operator in the plan"""

    usage_id: str
    name: str
    config: Optional[OpConfig] = None


@dataclass(frozen=True)
class PlanNode:
    """A node in the anonymized plan tree (one logical operator)."""

    usage_id: str
    op: str
    inputs: List["PlanNode"] = field(default_factory=list)


@dataclass(frozen=True)
class WorkloadInfo:
    """The anonymized plan tree, a human-readable rendering of it, and the
    per-op flat list (with config)."""

    plan: PlanNode
    plan_str: str
    ops: List[LogicalOp]


@dataclass(frozen=True)
class EnvInfo:
    pyarrow: Optional[str]


@dataclass(frozen=True)
class PipelinePerf:
    bytes_spilled: Optional[int]
    oom_kills: Optional[int] = None
    unexpected_worker_kills: Optional[int] = None
    node_deaths: Optional[int] = None


@dataclass(frozen=True)
class Issue:
    """An issue detected during execution, tied to an anonymized operator."""

    issue_type: str
    operator: str


@dataclass
class UsageInfo:
    """Per-execution usage payload: the entry buffered and flushed to GCS."""

    id: str
    started_at: float
    env: EnvInfo
    workload: WorkloadInfo
    performance: Optional[PipelinePerf] = None
    detected_issues: List[Issue] = field(default_factory=list)


# A callable that records config information for a logical operator.
OpConfigFn = Callable[[LogicalOperator], Optional[OpConfig]]

# A callable that samples a cluster metric (spilled bytes, dead node
# count, ...)
MetricReader = Callable[[], Optional[int]]


# Bounded buffer of recent executions. OrderedDict so eviction picks the
# oldest-inserted entry
_MAX_EXECUTIONS_TO_TRACK = 100

# Module state. Mutations are serialized through ``_lock``.
_executions: "OrderedDict[str, UsageInfo]" = OrderedDict()
_lock = threading.Lock()


def usage_collection_disabled() -> bool:
    """True when the user has opted out of usage stats (via
    ``RAY_USAGE_STATS_ENABLED=0``, ``ray disable-usage-stats``, or
    ``~/.ray/config.json``) or when ``RAY_DATA_USAGE_DISABLED=1`` is set.
    """
    return not usage_stats_enabled() or os.environ.get("RAY_DATA_USAGE_DISABLED") == "1"


def cluster_spilled_bytes() -> Optional[int]:
    """Cluster-wide cumulative spilled bytes from Ray core's store_stats.

    Returns None on any failure — usage collection must never break execution.
    """
    if not ray.is_initialized():
        return None
    try:
        reply = get_memory_info_reply(
            get_state_from_address(ray.get_runtime_context().gcs_address),
            timeout_seconds=10.0,
        )
        return int(reply.store_stats.spilled_bytes_total)
    except Exception:
        logger.debug("Failed to read cluster spilled bytes", exc_info=True)
        return None


def cluster_dead_node_count() -> Optional[int]:
    """Number of dead nodes in the GCS node table.

    Queries GCS with a bounded timeout and a server-side DEAD state filter.
    Returns None on any failure.
    """
    if not ray.is_initialized():
        return None
    try:
        gcs_client = global_worker.gcs_client  # pyrefly: ignore[missing-attribute]
        dead_nodes = gcs_client.get_all_node_info(
            timeout=_NODE_INFO_RPC_TIMEOUT_S,
            state_filter=GcsNodeInfo.GcsNodeState.DEAD,
        )
        return len(dead_nodes)
    except Exception:
        logger.debug("Failed to read cluster dead node count", exc_info=True)
        return None


def compute_delta(start: Optional[int], end: Optional[int]) -> Optional[int]:
    """Non-negative delta between two cumulative samples. Returns None if
    either sample is missing"""
    if start is None or end is None:
        return None
    return max(0, end - start)


def record_usage_info(info: UsageInfo) -> None:
    """Buffer ``info`` (evicting the oldest entry when full) and flush the whole
    buffer to GCS via ``record_extra_usage_tag``.

    The callback calls this both before execution starts (so attempted
    executions are captured even if the execution fails) and after it finishes
    (to overwrite the same entry with performance and issue data).

    Short-circuits when the user has opted out of Ray usage stats (via
    ``RAY_USAGE_STATS_ENABLED=0``, ``ray disable-usage-stats``, or
    ``~/.ray/config.json``) or when ``RAY_DATA_USAGE_DISABLED=1`` is set.
    """
    if usage_collection_disabled():
        return
    try:
        with _lock:
            if (
                info.id not in _executions
                and len(_executions) >= _MAX_EXECUTIONS_TO_TRACK
            ):
                _executions.popitem(last=False)
            _executions[info.id] = info
            payload = _serialize_locked()
        record_extra_usage_tag(TagKey.DATA_USAGE, payload)
    except Exception:
        logger.debug("Failed to record usage info", exc_info=True)


def build_usage_id_map(logical_plan: "LogicalPlan") -> Dict[int, str]:
    """Build the ``id(logical_op) -> usage_id`` map for a plan.

    The IDs are computed based on the hash of the (post-order index, anonymized name) tuple. These are
    used to identify logical ops after anonymization (i.e. MapBatches-<id1>, MapBatches-<id2>, etc.).

    Short-circuits to an empty map when the user has opted out of usage stats:
    without a recorded payload there is nothing for the IDs to reference.
    """
    if usage_collection_disabled():
        return {}
    try:
        ordered_logical_ops: List[Tuple[LogicalOperator, str]] = []
        _build_plan(logical_plan.dag, ordered_logical_ops)
        return {id(op): usage_id for op, usage_id in ordered_logical_ops}
    except Exception:
        logger.debug("Failed to build usage id map", exc_info=True)
        return {}


def physical_op_name_with_id(
    operator: "PhysicalOperator",
    usage_id_map: Optional[Dict[int, str]] = None,
) -> str:
    """Anonymized name for a physical op. Fused ops join their constituent logical
    ops with '->' to signal operator fusion. We need physical op name as
    issues from the issue detector reference physical ops."""
    logical_ops = operator._logical_operators
    if not logical_ops:
        return "Unknown"
    return "->".join(_logical_op_name_with_id(op, usage_id_map) for op in logical_ops)


def _logical_op_name_with_id(
    logical_op: LogicalOperator,
    usage_id_map: Optional[Dict[int, str]] = None,
) -> str:
    """Logical op is formatted as ``<anonymized_name>-<usage_id>``. The usage ID map is populated before execution starts in the usage callback."""
    name = anonymize_op_name(logical_op)
    if usage_id_map:
        # Correlate with the IDs assigned to the logical ops in the workload plan.
        usage_id = usage_id_map.get(id(logical_op))
        if usage_id is not None:
            return f"{name}-{usage_id}"
    return name


def collect_issues(
    detected_issues: Optional[List[Tuple["IssueType", str]]],
) -> List[Issue]:
    """Convert (issue_type, operator) pairs into ``Issue`` records, mapping
    each ``IssueType`` enum to its string value."""
    if not detected_issues:
        return []
    return [
        Issue(issue_type=issue_type.value, operator=operator)
        for issue_type, operator in detected_issues
    ]


def _serialize_locked() -> str:
    """Serialize current state to JSON. Caller must hold ``_lock``."""
    return json.dumps({"executions": [asdict(e) for e in _executions.values()]})


def collect_env() -> EnvInfo:
    """Process-wide environment info."""
    return EnvInfo(pyarrow=_safe_version("pyarrow"))


def _safe_version(pkg: str) -> Optional[str]:
    try:
        return importlib.metadata.version(pkg)
    except importlib.metadata.PackageNotFoundError:
        return None


def collect_workload(
    logical_plan: "LogicalPlan",
    op_config_fn: OpConfigFn = None,
) -> WorkloadInfo:
    """Collect the anonymized plan tree, indented text rendering, and per-op
    config list in a single DAG walk.

    ``op_config_fn`` builds the per-op config; it defaults to ``collect_op_config``
    and is overridable if subclasses need to extract custom config info.
    """
    if op_config_fn is None:
        op_config_fn = collect_op_config
    dag = logical_plan.dag
    ordered_logical_ops: List[Tuple[LogicalOperator, str]] = []
    plan = _build_plan(dag, ordered_logical_ops)
    return WorkloadInfo(
        plan=plan,
        plan_str=_format_plan_str(dag),
        ops=_build_ops(ordered_logical_ops, op_config_fn),
    )


def _build_plan(
    op: LogicalOperator,
    ordered_logical_ops: List[Tuple[LogicalOperator, str]],
) -> PlanNode:
    """Build the plan tree and record logical ops in post-order.

    Deduplicates shared operator instances (e.g. ``ds.zip(ds)``), so each
    operator is assigned a single usage_id even when reachable via multiple
    plan branches.
    """

    @cache
    def _build_cached(op: LogicalOperator) -> PlanNode:
        child_plans = [_build_cached(child) for child in op.input_dependencies]
        name = anonymize_op_name(op)
        usage_id = make_usage_op_id(len(ordered_logical_ops), name)
        ordered_logical_ops.append((op, usage_id))
        return PlanNode(usage_id=usage_id, op=name, inputs=child_plans)

    return _build_cached(op)


def _build_ops(
    ordered_logical_ops: List[Tuple[LogicalOperator, str]],
    op_config_fn: OpConfigFn,
) -> List[LogicalOp]:
    """Build the flat logical-op list from the canonical post-order traversal."""
    ops: List[LogicalOp] = []
    for op, usage_id in ordered_logical_ops:
        name = anonymize_op_name(op)
        ops.append(
            LogicalOp(
                usage_id=usage_id,
                name=name,
                config=op_config_fn(op),
            )
        )
    return ops


def collect_op_config(op: LogicalOperator) -> Optional[OpConfig]:
    # MapBatches is the only operator with a user-facing batch_format.
    if not isinstance(op, MapBatches):
        return None
    batch_format = op.batch_format
    if batch_format == "default":
        batch_format = _apply_batch_format(batch_format)
    if batch_format in VALID_BATCH_FORMATS:
        return OpConfig(batch_format=batch_format)
    logger.debug(f"Unexpected batch format: {batch_format!r}")
    return OpConfig(batch_format="unknown")


def make_usage_op_id(index: int, name: str) -> str:
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


def reset_for_testing() -> None:
    """Reset module state. Tests only."""
    with _lock:
        _executions.clear()


def get_executions() -> "OrderedDict[str, UsageInfo]":
    """Get the current executions. Tests only."""
    with _lock:
        return _executions.copy()
