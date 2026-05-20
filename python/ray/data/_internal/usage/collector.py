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
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators import AbstractUDFMap, Read, Write
from ray.data._internal.logical.util import _op_name_white_list

if TYPE_CHECKING:
    from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan

logger = logging.getLogger(__name__)

# Bounded buffer of recent executions. OrderedDict so eviction picks the
# oldest-inserted entry
_MAX_EXECUTIONS_TO_TRACK = 100

# Module state. Mutations are serialized through ``_lock``.
_executions: "OrderedDict[str, dict]" = OrderedDict()
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
    Set ``RAY_DATA_USAGE_DISABLED=1`` to disable all collection.
    """
    if os.environ.get("RAY_DATA_USAGE_DISABLED") == "1":
        return
    try:
        entry = {
            "id": execution_id,
            "started_at": time.time(),
            "env": _collect_env(),
            "workload": _collect_workload(logical_plan),
            "performance": None,
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
    execution_id: str,
    error: Optional[BaseException],
) -> None:
    """Fill in performance, error for a previously recorded execution and flush.

    Set ``RAY_DATA_USAGE_DISABLED=1`` to short-circuit all collection.
    """
    if os.environ.get("RAY_DATA_USAGE_DISABLED") == "1":
        return
    try:
        spilled_now = _cluster_spilled_bytes()
        with _lock:
            entry = _executions.get(execution_id)
            if entry is None:  # if the execution was not found (could be evicted)
                _spillage_dict.pop(execution_id, None)
                return
            spilled_at_start = _spillage_dict.pop(execution_id, None)
            entry["performance"] = _collect_pipeline_perf(spilled_at_start, spilled_now)
            payload = _serialize_locked()
        record_extra_usage_tag(TagKey.DATA_USAGE, payload)
    except Exception:
        logger.debug("Failed to record execution result usage", exc_info=True)


def _serialize_locked() -> str:
    """Serialize current state to JSON. Caller must hold ``_lock``."""
    return json.dumps({"executions": list(_executions.values())})


def _collect_env() -> dict:
    """Process-wide environment info."""
    return {"pyarrow": _safe_version("pyarrow")}


def _safe_version(pkg: str) -> Optional[str]:
    try:
        return importlib.metadata.version(pkg)
    except importlib.metadata.PackageNotFoundError:
        return None


def _collect_workload(logical_plan: "LogicalPlan") -> dict:
    """Collect anonymized plan and per-op config"""
    ops = _walk_operators(logical_plan.dag)
    return {
        "plan": "->".join(op["name"] for op in ops),
        "ops": ops,
    }


def _walk_operators(op: LogicalOperator) -> List[dict]:
    """Post-order walk producing anonymized op names + per-op config."""
    ops: List[dict] = []
    for child in op.input_dependencies:
        ops.extend(_walk_operators(child))

    entry: Dict[str, Any] = {"name": _anonymize_op_name(op)}
    if isinstance(op, AbstractUDFMap):
        batch_format = getattr(op, "batch_format", None)
        if batch_format is not None:
            entry["config"] = {"batch_format": batch_format}
    ops.append(entry)
    return ops


def _anonymize_op_name(op: LogicalOperator) -> str:
    """Anonymize an operator name against the whitelist from ``logical/util.py``.
    """
    if isinstance(op, Read):
        name = f"Read{op.datasource.get_name()}"
        return name if name in _op_name_white_list else "ReadCustom"
    if isinstance(op, Write):
        name = f"Write{op.datasink_or_legacy_datasource.get_name()}"
        return name if name in _op_name_white_list else "WriteCustom"
    name = op.name
    if not name:
        return "Unknown"
    # Remove the function name from the map operator name
    bare = re.sub(r"\(.*\)$", "", name).strip()
    return bare if bare in _op_name_white_list else "Unknown"


def _collect_pipeline_perf(
    spilled_at_start: Optional[int],
    spilled_now: Optional[int],
) -> dict:
    """Pipeline-level perf metrics

    ``bytes_spilled`` is a cluster-wide delta sourced from Ray core's
    ``store_stats.spilled_bytes_total`` (same path ``plan.py:259-265`` uses
    for ``global_bytes_spilled``). ``spilled_now`` is computed by the caller
    outside the lock — it's a synchronous gRPC.

    OOM / worker / node death counts are emitted as ``None`` — the underlying
    counters (``WORKER_CRASH_OOM``, ``WORKER_CRASH_SYSTEM_ERROR``) are
    C++-only with no Python read API. Follow-up will wire them.
    """
    if spilled_at_start is None or spilled_now is None:
        bytes_spilled = None
    else:
        bytes_spilled = max(0, spilled_now - spilled_at_start)

    return {
        "bytes_spilled": bytes_spilled,
        "oom_kills": None,
        "unexpected_worker_kills": None,
        "node_deaths": None,
    }


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


def _reset_for_testing() -> None:
    """Reset module state. Tests only."""
    with _lock:
        _executions.clear()
        _spillage_dict.clear()
