import logging
from typing import Dict, List

from ray.data._internal.execution.interfaces import ExecutionResources

logger = logging.getLogger(__name__)


def cap_resource_request_to_limits(
    active_bundles: List[Dict],
    pending_bundles: List[Dict],
    resource_limits: ExecutionResources,
) -> List[Dict]:
    """Cap the resource request to not exceed user-configured resource limits.

    Active bundles (for running tasks or existing nodes) are always included first
    since they represent resources already in use. Pending bundles (for future work
    or scale-up requests) are then added best-effort, sorted smallest-first to
    maximize packing within limits.

    This ensures that resources for already-running tasks are never crowded out
    by pending work from smaller operators.

    Args:
        active_bundles: Bundles for already-running tasks or existing nodes
            (must include - these represent current resource usage).
        pending_bundles: Bundles for pending work or scale-up requests
            (best-effort - only added if within limits).
        resource_limits: The user-configured resource limits.

    Returns:
        A list of resource bundles that respects user limits, with active bundles
        always included first.
    """
    # If no explicit limits are set (all infinite), return everything
    if resource_limits == ExecutionResources.inf():
        return active_bundles + pending_bundles

    # Always include active bundles first - they're already running/allocated
    capped_request = list(active_bundles)
    total = ExecutionResources.zero()
    for bundle in active_bundles:
        total = total.add(ExecutionResources.from_resource_dict(bundle))

    # Sort pending bundles by size (smallest first) to maximize packing.
    # This ensures smaller bundles aren't excluded due to larger bundles
    # appearing earlier in arbitrary iteration order.
    def bundle_sort_key(bundle: Dict) -> tuple:
        return (
            bundle.get("CPU", 0),
            bundle.get("GPU", 0),
            bundle.get("memory", 0),
        )

    sorted_pending = sorted(pending_bundles, key=bundle_sort_key)

    for bundle in sorted_pending:
        new_total = total.add(ExecutionResources.from_resource_dict(bundle))

        # Skip bundles that don't fit, continue checking smaller ones
        if not new_total.satisfies_limit(resource_limits):
            continue

        capped_request.append(bundle)
        total = new_total

    total_input = len(active_bundles) + len(pending_bundles)
    if len(capped_request) < total_input:
        logger.debug(
            f"Capped autoscaling resource request from {total_input} "
            f"bundles to {len(capped_request)} bundles to respect "
            f"user-configured resource limits: {resource_limits}. "
            f"({len(active_bundles)} active bundles kept, "
            f"{len(capped_request) - len(active_bundles)}/{len(pending_bundles)} "
            f"pending bundles included)."
        )

    return capped_request
