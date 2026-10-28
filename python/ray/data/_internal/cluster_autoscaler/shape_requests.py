"""Exact-shape resource requests for the cluster autoscaler.

A request is a list of Ray resource bundles, built from the exact requests the
operators report for their in-flight tasks and actors (``ExecutionResources``
cannot express custom resources) plus the throughput solver's delta, spread over
each operator's own shapes. ``select_over_utilized_shapes`` decides which shapes
need to grow, measuring each against the worker groups that can host it;
``build_request`` assembles a request and ``ScaleUpKeepAlive`` holds its scale-up
copies while utilization is low.
"""

import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple

from .base_autoscaling_coordinator import NodeResources, ResourceDict
from .default_autoscaling_coordinator import _bundle_can_fit_on_node
from .resource_utilization_gauge import ClusterUtil
from .supports_cluster_autoscaling import SupportsClusterAutoscaling
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources

# The hashable form of a resource request: its sorted items, custom resources
# included. Ray calls these "resource shapes"; a node's shape is a different thing.
ResourceShape = Tuple[Tuple[str, float], ...]


def to_resource_bundle(resources: ExecutionResources) -> ResourceDict:
    """Convert ExecutionResources to a bundle dict, dropping object_store_memory."""
    resource_dict = resources.copy(object_store_memory=0).to_resource_dict()
    return {k: v for k, v in resource_dict.items() if v > 0}


def collect_active_requests(
    ops: List[SupportsClusterAutoscaling],
) -> Dict[SupportsClusterAutoscaling, List[ResourceDict]]:
    """Map each operator to its non-empty in-flight requests, in submission order."""
    return {
        op: [request for request in op.get_resource_requests() if request] for op in ops
    }


def group_by_shape(
    requests: List[ResourceDict],
) -> Dict[ResourceShape, int]:
    """Group exact resource requests by their full resource shape."""
    return Counter(tuple(sorted(request.items())) for request in requests if request)


def distribute_bundles(
    shapes: Dict[ResourceShape, int], count: int
) -> List[ResourceShape]:
    """Spread ``count`` bundles over ``shapes`` in proportion to their demand.

    Scale-up copies follow the operator's own mix of in-flight requests, so a
    shortage of one shape never requests another. Shares use the largest
    remainder method and add up to exactly ``count``.
    """
    if count <= 0:
        return []
    if count < len(shapes):
        # Fewer bundles than shapes: scale the most in-demand ones.
        return [
            shape
            for shape, _ in sorted(
                shapes.items(), key=lambda item: item[1], reverse=True
            )[:count]
        ]

    # Seed every shape with one bundle, then spread what is left in proportion
    # to demand with the largest remainder method, so the shares add up to
    # exactly ``count``.
    allocation = {shape: 1 for shape in shapes}
    remaining = count - len(shapes)
    if remaining > 0:
        total_demand = sum(shapes.values())
        exact = {
            shape: remaining * demand / total_demand for shape, demand in shapes.items()
        }
        extra = {shape: math.floor(share) for shape, share in exact.items()}
        leftover = remaining - sum(extra.values())
        for shape, _ in sorted(
            exact.items(),
            key=lambda item: (item[1] - math.floor(item[1]), item[1], item[0]),
            reverse=True,
        )[:leftover]:
            extra[shape] += 1
        allocation = {shape: num + extra[shape] for shape, num in allocation.items()}

    return [shape for shape, num in allocation.items() for _ in range(num)]


def matching_worker_capacity(
    resources: ResourceDict, node_resources: NodeResources
) -> Dict[str, float]:
    """Return the capacity of the worker groups that can host a shape.

    Only nodes that can host the *whole* shape contribute, so no capacity is
    borrowed from a worker group the shape cannot run on.
    """
    capacity: Dict[str, float] = defaultdict(float)
    for node in node_resources.values():
        if not _bundle_can_fit_on_node(resources, node):
            continue
        for name in resources:
            capacity[name] += node.get(name, 0)
    return capacity


def utilization_threshold(thresholds: ClusterUtil, resource_name: str) -> float:
    """Return one resource's threshold; custom resources use the CPU threshold."""
    return {
        "CPU": thresholds.cpu,
        "GPU": thresholds.gpu,
        "memory": thresholds.memory,
        "object_store_memory": thresholds.object_store_memory,
    }.get(resource_name, thresholds.cpu)


def select_over_utilized_shapes(
    active_requests: List[ResourceDict],
    node_resources: NodeResources,
    thresholds: ClusterUtil,
) -> List[ResourceShape]:
    """Return the in-flight shapes whose utilization needs expansion.

    Utilization is measured per exact shape against the worker groups that can
    host it: the cluster-wide ratio cannot see a custom resource that only some
    worker groups provide, and a requester's own reservation reads as saturated
    for one however idle the cluster is. An empty ``node_resources`` means the
    view is unavailable, so no decision is made this tick; a shape no worker group
    can host is selected, since waiting cannot help.
    """
    if not node_resources:
        return []

    selected = []
    for shape, active_count in group_by_shape(active_requests).items():
        resources = dict(shape)
        demand = {name: amount * active_count for name, amount in resources.items()}
        capacity = matching_worker_capacity(resources, node_resources)
        # A shape no worker group can host can never be satisfied, so it is
        # selected: there is nothing to wait for.
        if not capacity:
            selected.append(shape)
            continue
        if any(
            resource_demand / capacity[name] >= utilization_threshold(thresholds, name)
            for name, resource_demand in demand.items()
            if resource_demand > 0 and capacity.get(name, 0) > 0
        ):
            selected.append(shape)
    return selected


def _scale_up_bundles(
    op: SupportsClusterAutoscaling,
    count: int,
    active_requests: List[ResourceDict],
) -> List[ResourceDict]:
    """Return the bundles to add on top of ``op``'s in-flight demand."""
    extra = max(0, count - len(active_requests))
    if extra == 0:
        return []

    shapes = group_by_shape(active_requests)
    if not shapes:
        # No exact demand reported (nothing in flight yet, or an operator
        # that only tracks logical bundles): fall back to the operator's
        # logical resource bundle.
        return [to_resource_bundle(op.min_scheduling_resources())] * extra
    return [dict(shape) for shape in distribute_bundles(shapes, extra)]


@dataclass
class RequestPlan:
    """A request, and the part of it added on top of the in-flight demand.

    ``scale_up`` is the solver delta plus one copy per over-utilized shape; it is
    what the keep-alive window holds while utilization is low.
    """

    request: List[ResourceDict]
    scale_up: List[ResourceDict]


def build_request(
    *,
    active_requests_by_op: Dict[SupportsClusterAutoscaling, List[ResourceDict]],
    bundle_counts: Dict[SupportsClusterAutoscaling, int],
    over_utilized_shapes: List[ResourceShape],
) -> RequestPlan:
    """Build the request, preserving exact resource shapes.

    In-flight demand goes out unchanged for every operator -- this autoscaler only
    scales up, so the request never falls below what is already running -- and the
    solver's delta is added on top, over each operator's own shapes. An
    over-utilized shape gets one copy unless the solver already asked for one.
    """
    request: List[ResourceDict] = []
    # In-flight demand goes out by reference: the bundles are owned by the
    # operators, and ``_send_resource_request`` copies everything it puts on
    # the wire, so nothing in between may mutate a bundle in place.
    for requests in active_requests_by_op.values():
        request.extend(requests)

    scale_up: List[ResourceDict] = []
    for op, count in bundle_counts.items():
        scale_up.extend(_scale_up_bundles(op, count, active_requests_by_op.get(op, [])))
    requested = Counter(tuple(sorted(bundle.items())) for bundle in scale_up)
    for shape in over_utilized_shapes:
        if requested[shape]:
            continue
        scale_up.append(dict(shape))
        requested[shape] += 1
    request.extend(scale_up)
    return RequestPlan(request=request, scale_up=scale_up)


class ScaleUpKeepAlive:
    """Holds a request's scale-up copies alive for a bounded window, per shape.

    In-flight demand needs no window: it is re-sent every tick while its tasks are
    alive. One instance belongs to one autoscaler (one execution).
    """

    def __init__(self, release_delay_s: float):
        #: How long a scale-up copy is held after it was last requested.
        self.release_delay_s = release_delay_s
        # shape -> (count, last requested), so each shape is released on its own
        # schedule instead of riding along on the most recently requested one.
        self._held: Dict[ResourceShape, Tuple[int, float]] = {}

    def record(self, scale_up: List[ResourceDict], now: float) -> None:
        """Record the scale-up copies to hold while utilization is low.

        Earlier copies are kept alongside the current ones (a 4-GPU scale-up must
        not cancel a 1-GPU one still inside its window), and each shape expires
        from when it was last requested, so a tick that adds nothing cannot extend
        another shape's window and churn cannot grow the set without bound.
        """
        held = {
            shape: entry
            for shape, entry in self._held.items()
            if now - entry[1] < self.release_delay_s
        }
        for shape, count in group_by_shape(scale_up).items():
            held[shape] = (count, now)
        self._held = held

    def bundles(self, now: float) -> List[ResourceDict]:
        """Return the scale-up copies still inside their own grace window."""
        return [
            dict(shape)
            for shape, (count, requested_at) in self._held.items()
            if now - requested_at < self.release_delay_s
            for _ in range(count)
        ]

    def low_utilization_request(
        self, active_requests: List[ResourceDict], now: float
    ) -> List[ResourceDict]:
        """Renew the request while utilization is below the scale-up threshold.

        Held copies are *added* to the current demand rather than replacing it, so
        a shape that first appears inside the window still reaches the coordinator.
        """
        return list(active_requests) + self.bundles(now)
