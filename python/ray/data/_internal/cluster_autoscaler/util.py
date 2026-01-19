import logging
from typing import Dict, List

from ray.data._internal.execution.interfaces import ExecutionResources

logger = logging.getLogger(__name__)


def cap_resource_request_to_limits(
    resource_request: List[Dict],
    resource_limits: ExecutionResources,
) -> List[Dict]:
    """Cap the resource request to not exceed user-configured resource limits.

    If the user has set explicit (non-infinite) resource limits, this function
    filters the resource request to ensure the total requested resources do not
    exceed those limits.

    Bundles are sorted by size (smallest first) to maximize the number of bundles
    that can fit within the limits. This ensures that smaller bundles are not
    excluded just because a larger bundle appeared earlier in iteration order.

    Args:
        resource_request: List of resource bundles to request.
        resource_limits: The user-configured resource limits.

    Returns:
        A filtered list of resource bundles that respects user limits.
    """
    # If no explicit limits are set (all infinite), return the original request
    if resource_limits == ExecutionResources.inf():
        return resource_request

    # Sort bundles by size (smallest first) to maximize packing within limits.
    # This ensures smaller bundles aren't excluded due to larger bundles
    # appearing earlier in arbitrary iteration order.
    def bundle_sort_key(bundle: Dict) -> tuple:
        return (
            bundle.get("CPU", 0),
            bundle.get("GPU", 0),
            bundle.get("memory", 0),
        )

    sorted_bundles = sorted(resource_request, key=bundle_sort_key)

    capped_request = []
    total = ExecutionResources.zero()

    for bundle in sorted_bundles:
        new_total = total.add(ExecutionResources.from_resource_dict(bundle))

        # Skip bundles that don't fit, continue checking smaller ones
        if not new_total.satisfies_limit(resource_limits):
            continue

        capped_request.append(bundle)
        total = new_total

    if len(capped_request) < len(resource_request):
        logger.debug(
            f"Capped autoscaling resource request from {len(resource_request)} "
            f"bundles to {len(capped_request)} bundles to respect "
            f"user-configured resource limits: {resource_limits}."
        )

    return capped_request
