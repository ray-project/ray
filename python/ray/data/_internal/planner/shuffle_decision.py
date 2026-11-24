"""Plan-time shuffle transport and partition count.

One shuffle uses one transport, chosen here. There is no runtime fallback:
a missing or untrusted byte estimate stays on the object store.
"""

import logging
import math
from typing import Optional

from ray.data._internal.logical.interfaces.logical_operator import LogicalOperator

logger = logging.getLogger(__name__)


def _shuffle_bytes_untrusted(op: LogicalOperator) -> bool:
    """UDFs and downloads can change byte size without updating ``size_bytes``."""
    from ray.data._internal.logical.operators.map_operator import (
        AbstractUDFMap,
        Project,
    )
    from ray.data._internal.logical.operators.one_to_one_operator import Download

    # Project copies size_bytes even when it adds columns, which underestimates.
    return isinstance(op, (AbstractUDFMap, Download, Project))


def _trusted_output_bytes(op: LogicalOperator) -> Optional[int]:
    """Return ``op``'s output bytes, or None if any ancestor cannot be trusted.

    ``size_bytes`` copied through a map is not a shuffle estimate: a map that
    adds columns keeps the old, smaller size. Unknown is safer than a confident
    under-estimate, which would pin both the transport and the partition count.
    """
    stack = [op]
    seen = set()
    output_bytes: Optional[int] = None
    while stack:
        current = stack.pop()
        ident = id(current)
        if ident in seen:
            continue
        seen.add(ident)
        if _shuffle_bytes_untrusted(current):
            return None
        meta = current.infer_metadata()
        if meta.size_bytes is None:
            return None
        if current is op:
            output_bytes = meta.size_bytes
        stack.extend(current.input_dependencies)
    return output_bytes


def estimate_shuffle_input_bytes(logical_op: LogicalOperator) -> Optional[int]:
    """Sum trusted input bytes. One copy: do not multiply by the partition count.

    Returns None unless every input's byte size is known and no ancestor is a
    UDF or download. Callers must not treat None as zero.
    """
    deps = list(logical_op.input_dependencies)
    if not deps:
        return _trusted_output_bytes(logical_op)
    total = 0
    for dep in deps:
        nbytes = _trusted_output_bytes(dep)
        if nbytes is None:
            return None
        total += nbytes
    return total


def cluster_object_store_bytes() -> Optional[int]:
    """Total object-store memory, or None when the cluster cannot be queried."""
    try:
        import ray

        if not ray.is_initialized():
            return None
        from ray.data._internal.execution.operators.hash_shuffle import (
            _get_total_cluster_resources,
        )

        total = _get_total_cluster_resources().object_store_memory
    except Exception:
        logger.debug("Shuffle object-store budget is unavailable", exc_info=True)
        return None
    if not total or total == float("inf"):
        return None
    return int(total)


def resolve_shuffle_transport(
    data_context,
    estimated_bytes: Optional[int],
    *,
    allow_auto: bool = True,
    object_store_bytes: Optional[int] = None,
) -> str:
    """Return ``object_store`` or ``disk``.

    ``allow_auto=False`` is for ``sort()``: ``auto`` stays on the object store
    so a large sort is not forced through ``SortSamplingOp``. An explicit
    ``disk`` or ``object_store`` is still honored. ``object_store_bytes`` lets
    tests supply a budget without a cluster.
    """
    transport = data_context.shuffle_transport
    if transport not in ("auto", "object_store", "disk"):
        raise ValueError(
            "DataContext.shuffle_transport must be 'auto', 'object_store', "
            f"or 'disk', got {transport!r}"
        )
    if transport != "auto" or not allow_auto:
        chosen = transport if transport != "auto" else "object_store"
        logger.info(
            "Resolved shuffle transport %s (explicit, estimated_bytes=%s)",
            chosen,
            estimated_bytes,
        )
        return chosen

    budget_bytes = (
        object_store_bytes
        if object_store_bytes is not None
        else cluster_object_store_bytes()
    )
    fraction = data_context.shuffle_object_store_budget_fraction
    budget = None
    if budget_bytes is not None and budget_bytes > 0 and fraction > 0:
        budget = int(budget_bytes * fraction)

    if estimated_bytes is None or budget is None or estimated_bytes < budget:
        chosen = "object_store"
    else:
        chosen = "disk"
    logger.info(
        "Resolved shuffle transport %s "
        "(estimated_bytes=%s, object_store_budget_bytes=%s)",
        chosen,
        estimated_bytes,
        budget,
    )
    return chosen


def resolve_num_partitions(
    data_context,
    estimated_bytes: Optional[int],
    *,
    requested: Optional[int],
    keyless: bool = False,
) -> int:
    """Partition count for one shuffle.

    A caller-supplied count wins. A keyless aggregation is one partition.
    Otherwise ``clamp(ceil(bytes / target), 1, max)``, or the context default
    when the byte estimate is missing. The result must not be written back onto
    the logical operator: shuffle elimination treats a stored count as a request
    to compare against the upstream partitioning.
    """
    if keyless:
        return 1
    if requested is not None:
        return requested
    fallback = data_context.default_hash_shuffle_parallelism
    target = data_context.shuffle_target_partition_bytes
    cap = data_context.shuffle_max_partitions
    if estimated_bytes is None or target <= 0:
        return fallback
    count = math.ceil(estimated_bytes / target)
    return max(1, min(int(cap), int(count)))
