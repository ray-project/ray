import copy
import functools
import logging
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, FrozenSet, List, Optional, Set

import ray
import ray.exceptions
from .base_autoscaling_coordinator import (
    AutoscalingCoordinator,
    LabelKey,
    LabelSelector,
    LabelValue,
    NodeResources,
    RequesterId,
    ReservedResources,
    ResourceDict,
    ResourceRequestPriority,
    ResourceRequestStrategy,
    ResourceType,
)
from ray._common.utils import env_bool
from ray.data._internal.execution.interfaces.common import NodeIdStr
from ray.data._internal.execution.util import memory_string
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)

HEAD_NODE_RESOURCE_LABEL = "node:__internal_head__"
_RESOURCE_LOG_KEYS = ("CPU", "GPU", "memory", "object_store_memory")
_RESOURCE_LOG_MEMORY_KEYS = {"memory", "object_store_memory"}
# Label key the cluster autoscaler uses to bucket nodes by subcluster.
# Hardcoded so all components agree without per-Dataset configuration.
SUBCLUSTER_LABEL_KEY: LabelKey = "ray-subcluster"
# Sentinel for "no subcluster" — used as both a node-label fallback and
# the bucket key for unlabeled nodes in ``_cluster_node_resources``.
DEFAULT_SUBCLUSTER: Optional[LabelValue] = None


RAY_DATA_AUTOSCALING_COORDINATOR_LOG_TRACEBACK = env_bool(
    "RAY_DATA_AUTOSCALING_COORDINATOR_LOG_TRACEBACK", True
)


def _format_resource_value_for_log(resource_name: str, value: float) -> str:
    """Format a numerical resource value to a human-readable string.

    Args:
        resource_name: The resource name.
        value: The resource value.

    Returns:
        A human-readable string.
    """
    if resource_name in _RESOURCE_LOG_MEMORY_KEYS:
        return memory_string(value)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _format_resource_bundle_for_log(bundle: ResourceDict) -> str:
    """Format a resource bundle to a human-readable string.

    Drops custom resource keys (e.g. ``anyscale/...``, ``node:...``) and
    zero-valued resources, keeping only the standard keys in ``_RESOURCE_LOG_KEYS``.

    Args:
        bundle: The resource bundle to format.

    Returns:
        A human-readable string, e.g. ``"{CPU: 8, memory: 32.0GiB}"``.

    Example:
        >>> from ray.data._internal.util import GiB
        >>> _format_resource_bundle_for_log({"CPU": 8, "GPU": 0, "memory": 32 * GiB})
        '{CPU: 8, memory: 32.0GiB}'
    """
    resources = []
    for resource_name in _RESOURCE_LOG_KEYS:
        value = bundle.get(resource_name, 0)
        if value == 0:
            continue
        resources.append(
            f"{resource_name}: {_format_resource_value_for_log(resource_name, value)}"
        )
    return "{" + ", ".join(resources) + "}"


def _format_resource_bundles_for_log(resources: List[ResourceDict]) -> str:
    """Format and aggregate resource bundles for logging.

    Bundles that format to the same string (after dropping custom/zero-valued
    resources) are collapsed into a single ``N x {...}`` entry.

    Args:
        resources: The resource bundles to format.

    Returns:
        A human-readable string, e.g. ``"[2 x {CPU: 1}, 1 x {GPU: 1}]"``.

    Example:
        >>> _format_resource_bundles_for_log([{"CPU": 1}, {"CPU": 1}, {"GPU": 1}])
        '[2 x {CPU: 1}, 1 x {GPU: 1}]'
    """
    bundle_counts: Dict[str, int] = {}
    for resource in resources:
        bundle = _format_resource_bundle_for_log(resource)
        if bundle == "{}":
            continue
        bundle_counts[bundle] = bundle_counts.get(bundle, 0) + 1

    return (
        "["
        + ", ".join(f"{count} x {bundle}" for bundle, count in bundle_counts.items())
        + "]"
    )


def _format_node_resources_for_log(resources: ReservedResources) -> str:
    """Format reserved per-node resources for logging.

    Each node is emitted as ``node_id: {...}``. Custom/zero-valued resources
    are dropped via ``_format_resource_bundle_for_log``; nodes whose bundle
    is empty after that are omitted.

    Args:
        resources: Mapping from node id to the resource bundle reserved on
            that node.

    Returns:
        A human-readable string, e.g.
        ``"[n1: {CPU: 1}, n2: {CPU: 1}, n3: {GPU: 1}]"``.

    Example:
        >>> _format_node_resources_for_log(
        ...     {"n1": {"CPU": 1}, "n2": {"CPU": 1}, "n3": {"GPU": 1}}
        ... )
        '[n1: {CPU: 1}, n2: {CPU: 1}, n3: {GPU: 1}]'
    """
    entries = []
    for node_id, resource in resources.items():
        bundle = _format_resource_bundle_for_log(resource)
        if bundle == "{}":
            continue
        entries.append(f"{node_id}: {bundle}")

    return "[" + ", ".join(entries) + "]"


@dataclass
class OngoingRequest:
    """Represents an ongoing resource request from a requester."""

    # The time when the request was first received.
    first_request_time: float
    # Requested resources.
    requested_resources: List[ResourceDict]
    # The expiration time of the request.
    expiration_time: float
    # Resource types for which leftover cluster capacity should also be
    # reserved. Empty means do not reserve leftovers.
    request_remaining: FrozenSet[ResourceType]
    # The priority of the request, higher value means higher priority.
    priority: int
    # Resources that are reserved to the requester, keyed in the order bundles
    # were placed. Callers rely on that order to work out which node holds
    # which bundle (e.g. Ray Train subtracts its trainer bundle, always
    # submitted first, from the first node it fits on), so don't reorder it.
    reserved_resources: ReservedResources
    # Per-bundle label selectors, parallel to ``requested_resources``.
    # Empty dicts mean no label constraint on that bundle. Required to have
    # the same length as ``requested_resources``.
    requested_label_selectors: List[LabelSelector]
    # NOTE: So `requested_label_selectors` will request which types of nodes
    # will be requested. The strategy then places the bundles on that narrowed subset
    # So if the user specifies `instance_type` or maybe `ray.io/accelerator-type`
    # those will be the only type of nodes to be scheduled. Then a PACK will make sure
    # to use the fewest nodes possible of that subset, while SPREAD will try to place
    # them everywhere. This is supposed to mimic the ray core behavior we have now.
    # NOTE: Remember that this only dictacts how nodes are reserved. This is what I mean:
    # Suppose I request [{CPU: 8, GPU: 1}, {CPU: 16, GPU: 2}] and I want them to be
    # on [{ray.io/accelerator-type: L4}, {ray.io/accelerator-type: H100}]. Then for spread,
    # I will return both nodes. However, it is up to the caller for schedule the 1st bundle
    # on 1st label key and so forth. I don't think this is an issue because train has uniform
    # resource request, and data doesn't use label selectors.
    strategy: ResourceRequestStrategy

    def requested_resources_sum(self) -> ResourceDict:
        bundle_sum: ResourceDict = {}
        for bundle in self.requested_resources:
            for key, val in bundle.items():
                bundle_sum[key] = bundle_sum.get(key, 0) + val
        return bundle_sum

    def __lt__(self, other):
        """Used to sort requests when reserving resources.

        Higher priority first, then earlier first_request_time first.
        """
        if self.priority != other.priority:
            return self.priority > other.priority
        return self.first_request_time < other.first_request_time


class DefaultAutoscalingCoordinator(AutoscalingCoordinator):
    """Non-blocking client-side proxy for the _AutoscalingCoordinatorActor.

    Not thread-safe; all methods must be called from a single thread.

    Create one instance per requester. Multiple instances sharing the same
    ``requester_id`` will have diverging caches and break the FIFO ordering
    guarantee that ``request_resources`` and ``get_reserved_resources`` rely on.
    """

    def __init__(
        self,
        requester_id: RequesterId,
        autoscaling_coordinator_actor=None,  # For testing only: injects an actor instead of using the shared named singleton.
        subcluster_selector: Optional[LabelSelector] = None,
    ):
        self._requester_id = requester_id
        # Label selector keyed by ``SUBCLUSTER_LABEL_KEY`` pinning this
        # requester to a single subcluster.
        self._subcluster_selector = subcluster_selector
        self._cached_reserved_resources: ReservedResources = {}
        # In-flight get_reserved_resources ref, or None if no request is pending.
        self._pending_reserved_resources: Optional[ray.ObjectRef] = None
        if autoscaling_coordinator_actor is not None:
            # Bypass the cached_property by injecting the actor directly.
            # Used in tests to avoid the shared named actor.
            self.__dict__["_autoscaling_coordinator"] = autoscaling_coordinator_actor

    @functools.cached_property
    def _autoscaling_coordinator(self):
        # Lazy: avoids creating the actor in __init__.
        return get_or_create_autoscaling_coordinator()

    def request_resources(
        self,
        resources: List[ResourceDict],
        expire_after_s: float,
        request_remaining: Optional[List[ResourceType]] = None,
        priority: ResourceRequestPriority = ResourceRequestPriority.MEDIUM,
        label_selectors: Optional[List[LabelSelector]] = None,
        strategy: ResourceRequestStrategy = ResourceRequestStrategy.PACK,
    ) -> None:
        """Fire-and-forget: submit a resource request to the coordinator actor.

        Actor-side errors are not surfaced to the caller.
        """
        self._autoscaling_coordinator.request_resources.remote(
            requester_id=self._requester_id,
            resources=resources,
            expire_after_s=expire_after_s,
            request_remaining=request_remaining,
            priority=priority,
            label_selectors=label_selectors,
            subcluster_selector=self._subcluster_selector,
            strategy=strategy,
        )

    def cancel_request(self) -> None:
        """Fire-and-forget: cancel a resource request on the coordinator actor.

        Also clears client-side state (pending ref and cached reservation) so
        a subsequent ``get_reserved_resources`` call returns a fresh result
        rather than stale data from a prior pipeline run.
        """
        self._pending_reserved_resources = None
        self._cached_reserved_resources = {}
        self._autoscaling_coordinator.cancel_request.remote(self._requester_id)

    def get_reserved_resources(self) -> ReservedResources:
        """Return reserved resources without blocking.

        Submits an async RPC and immediately returns the last cached result.
        The cache is updated the next time the pending RPC completes.

        Because the actor processes calls in FIFO order, the result always
        reflects state after all previously submitted ``request_resources`` calls
        to the same actor.

        On actor errors, returns the cached value and logs a warning; never raises.
        """
        ref = self._pending_reserved_resources
        if ref is not None:
            ready, _ = ray.wait([ref], timeout=0)
            if ready:
                self._pending_reserved_resources = None
                try:
                    self._cached_reserved_resources = ray.get(ref, timeout=0)
                except ray.exceptions.RayError:
                    logger.warning(
                        f"Failed to get reserved resources for {self._requester_id};"
                        " falling back to the cached value."
                        " If this persists, file a GitHub issue.",
                        exc_info=RAY_DATA_AUTOSCALING_COORDINATOR_LOG_TRACEBACK,
                    )

        # Submit a new request if none is currently in-flight
        # (first call, or the previous request completed or errored).
        if self._pending_reserved_resources is None:
            self._pending_reserved_resources = (
                self._autoscaling_coordinator.get_reserved_resources.remote(
                    self._requester_id
                )
            )

        return self._cached_reserved_resources


def _default_send_resources_request(
    bundles: List[ResourceDict],
    label_selectors: Optional[List[LabelSelector]] = None,
) -> None:
    """Default ``send_resources_request`` implementation for the actor."""
    ray.autoscaler.sdk.request_resources(
        bundles=bundles, bundle_label_selectors=label_selectors
    )


class _AutoscalingCoordinatorActor:
    """An actor to coordinate autoscaling resource requests from different components.

    This actor is responsible for:
    * Merging received requests and dispatching them to Ray Autoscaler.
    * Reserving cluster resources to the requesters.
    """

    TICK_INTERVAL_S = 20

    def __init__(
        self,
        get_current_time: Callable[[], float] = time.time,
        send_resources_request: Callable[
            [List[ResourceDict], Optional[List[LabelSelector]]], None
        ] = _default_send_resources_request,
        get_cluster_nodes: Callable[[], List[Dict]] = ray.nodes,
    ):
        self._get_current_time = get_current_time
        self._send_resources_request = send_resources_request
        self._get_cluster_nodes = get_cluster_nodes

        self._ongoing_reqs: Dict[RequesterId, OngoingRequest] = {}
        # Map from requester id to its subcluster selector.
        self._subcluster_selectors: Dict[RequesterId, Optional[LabelSelector]] = {}
        # Node resources bucketed by their ``SUBCLUSTER_LABEL_KEY`` value.
        # Nodes without the key fall under ``DEFAULT_SUBCLUSTER``.
        self._cluster_node_resources: Dict[Optional[LabelValue], NodeResources] = {}
        # Lock for thread-safe access to shared state from the background
        self._lock = threading.RLock()
        self._update_cluster_node_resources()

        # This is an actor, so the following check should always be True.
        # It's only needed for unit tests.
        if ray.is_initialized():
            # Start a thread to perform periodical operations.
            def tick_thread_run():
                while True:
                    time.sleep(self.TICK_INTERVAL_S)
                    self._tick()

            self._tick_thread = threading.Thread(target=tick_thread_run, daemon=True)
            self._tick_thread.start()

    def _tick(self):
        """Used to perform periodical operations, e.g., purge expired requests,
        merge and send requests, check cluster resource updates, etc.

        Never raises. This runs on a bare background thread, where an escaping
        exception would kill the thread and silently stop autoscaling for every
        requester on the cluster, not just the one that misbehaved. Callers
        that tick on demand see the previous reservation instead of an error.
        """
        try:
            with self._lock:
                self._merge_and_send_requests()
                self._update_cluster_node_resources()
                self._refresh_resource_reservations()
        except Exception:
            logger.warning(
                "AutoscalingCoordinator tick failed; reservations may be stale"
                f" until the next tick in {self.TICK_INTERVAL_S} seconds."
                " If this error persists, file a GitHub issue.",
                exc_info=RAY_DATA_AUTOSCALING_COORDINATOR_LOG_TRACEBACK,
            )

    def request_resources(
        self,
        requester_id: RequesterId,
        resources: List[ResourceDict],
        expire_after_s: float,
        request_remaining: Optional[List[ResourceType]] = None,
        priority: ResourceRequestPriority = ResourceRequestPriority.MEDIUM,
        label_selectors: Optional[List[LabelSelector]] = None,
        subcluster_selector: Optional[LabelSelector] = None,
        strategy: ResourceRequestStrategy = ResourceRequestStrategy.PACK,
    ) -> None:
        share_leftover_resource_types = frozenset(request_remaining or ())
        logger.debug(
            "Received request from %s: %s "
            "(label_selectors=%s, subcluster_selector=%s, request_remaining=%s, "
            "strategy=%s).",
            requester_id,
            resources,
            label_selectors,
            subcluster_selector,
            share_leftover_resource_types,
            strategy,
        )
        if label_selectors is None:
            label_selectors = [{} for _ in resources]
        elif len(label_selectors) != len(resources):
            raise ValueError(
                f"label_selectors length ({len(label_selectors)}) must match "
                f"resources length ({len(resources)})."
            )
        strategy = ResourceRequestStrategy(strategy)
        if subcluster_selector and label_selectors:
            req_subcluster = subcluster_selector.get(SUBCLUSTER_LABEL_KEY)
            for i, sel in enumerate(label_selectors):
                bundle_subcluster = sel.get(SUBCLUSTER_LABEL_KEY)
                if (
                    bundle_subcluster is not None
                    and bundle_subcluster != req_subcluster
                ):
                    raise ValueError(
                        f"Bundle {i} label_selector targets subcluster "
                        f"{bundle_subcluster!r}, but requester is registered to "
                        f"{req_subcluster!r}. Per-bundle cross-subcluster "
                        f"reservation is not supported."
                    )
        with self._lock:
            now = self._get_current_time()
            request_updated = False
            old_req = self._ongoing_reqs.get(requester_id)
            if old_req is not None:
                if share_leftover_resource_types != old_req.request_remaining:
                    raise ValueError(
                        "Cannot change request_remaining of an ongoing request."
                        f"Old: {old_req.request_remaining} New: {share_leftover_resource_types}"
                    )
                if priority.value != old_req.priority:
                    raise ValueError("Cannot change priority of an ongoing request.")
                if strategy != old_req.strategy:
                    # Flipping the strategy would reshuffle every bundle onto
                    # different nodes, out from under whatever the requester
                    # already placed on the old reservation.
                    raise ValueError(
                        "Cannot change strategy of an ongoing request. "
                        f"Old: {old_req.strategy} New: {strategy}"
                    )
                if (
                    requester_id in self._subcluster_selectors
                    and self._subcluster_selectors[requester_id] != subcluster_selector
                ):
                    raise ValueError(
                        "Cannot change subcluster_selector of an ongoing request "
                        f"from {self._subcluster_selectors[requester_id]!r} to "
                        f"{subcluster_selector!r}."
                    )

                request_updated = (
                    resources != old_req.requested_resources
                    or label_selectors != old_req.requested_label_selectors
                )
                old_req.requested_resources = resources
                old_req.requested_label_selectors = label_selectors
                old_req.expiration_time = now + expire_after_s
            else:
                request_updated = True
                self._ongoing_reqs[requester_id] = OngoingRequest(
                    first_request_time=now,
                    requested_resources=resources,
                    requested_label_selectors=label_selectors,
                    request_remaining=share_leftover_resource_types,
                    priority=priority.value,
                    expiration_time=now + expire_after_s,
                    reserved_resources={},
                    strategy=strategy,
                )
            # Write subcluster after all validations so a rejected call
            # never leaves the registry on a new subcluster.
            self._subcluster_selectors[requester_id] = subcluster_selector
            if request_updated:
                # If the request has updated, immediately send
                # a new request and rereserve resources.
                self._merge_and_send_requests()
                self._refresh_resource_reservations()

    def cancel_request(
        self,
        requester_id: RequesterId,
    ):
        logger.debug("Canceling request for %s.", requester_id)
        with self._lock:
            if requester_id not in self._ongoing_reqs:
                return
            del self._ongoing_reqs[requester_id]
            self._subcluster_selectors.pop(requester_id, None)
            self._merge_and_send_requests()
            self._refresh_resource_reservations()

    def _purge_expired_requests(self):
        now = self._get_current_time()
        live = {
            requester_id: req
            for requester_id, req in self._ongoing_reqs.items()
            if req.expiration_time > now
        }
        for expired_id in self._ongoing_reqs.keys() - live.keys():
            self._subcluster_selectors.pop(expired_id, None)
        self._ongoing_reqs = live

    def _merge_and_send_requests(self):
        """Merge requests and send them to Ray Autoscaler.

        Each bundle's forwarded selector is the union of its per-bundle
        ``requested_label_selectors`` entry and the requester's
        ``subcluster_selector``. The subcluster pin wins on key conflict,
        so the autoscaler always sees the correct subcluster regardless
        of what the per-bundle selectors contain.

        ``STRICT_PACK`` bundles are collapsed into a single bundle of their
        sum, so the autoscaler is asked for one node big enough to hold all of
        them rather than several it is free to scatter. Bundles and selectors
        are parallel lists that the SDK requires to be the same length, so the
        selectors collapse with them.
        """
        self._purge_expired_requests()
        merged_req: List[ResourceDict] = []
        merged_selectors: List[LabelSelector] = []
        for requester_id, req in self._ongoing_reqs.items():
            requested_resources = req.requested_resources
            subcluster_selector = self._subcluster_selectors.get(requester_id) or {}
            requested_label_selectors = req.requested_label_selectors
            if req.strategy is ResourceRequestStrategy.STRICT_PACK and (
                requested_resources
            ):
                requested_resources = [req.requested_resources_sum()]
                # A single node has to satisfy every bundle's selector, so the
                # collapsed bundle carries all of their constraints.
                merged: LabelSelector = {}
                for label_selectors in req.requested_label_selectors:
                    merged.update(label_selectors)
                requested_label_selectors = [merged]
            merged_req.extend(requested_resources)
            for per_bundle in requested_label_selectors:
                merged_selectors.append({**per_bundle, **subcluster_selector})
        if any(merged_selectors):
            self._send_resources_request(merged_req, label_selectors=merged_selectors)
        else:
            self._send_resources_request(merged_req)

    def get_reserved_resources(
        self, requester_id: RequesterId, recompute: bool = False
    ) -> ReservedResources:
        """Get the reserved resources for the requester.

        Args:
            requester_id: The requester to answer for.
            recompute: Recompute against a fresh view of the cluster first. Callers
                that turn the answer into node pins want this, so they don't
                pin to a node that has died since the last tick. It costs a
                full tick, so leave it off for polling callers.

        Returns:
            The resources reserved to ``requester_id``, keyed by node id. Empty
            if the requester has no ongoing request.
        """
        with self._lock:
            if recompute:
                # `self._lock` is reentrant, so ticking from in here is safe.
                self._tick()
            if requester_id not in self._ongoing_reqs:
                return {}
            return self._ongoing_reqs[requester_id].reserved_resources

    def _update_cluster_node_resources(self) -> bool:
        """Update cluster resources bucketed by subcluster. Return True if changed."""

        def _is_node_eligible(node):
            # Exclude dead nodes.
            if not node["Alive"]:
                return False
            resources = node["Resources"]
            # Exclude the head node if it doesn't have CPUs and GPUs,
            # because the object store is not usable.
            if HEAD_NODE_RESOURCE_LABEL in resources and (
                resources.get("CPU", 0) == 0 and resources.get("GPU", 0) == 0
            ):
                return False
            return True

        nodes = list(filter(_is_node_eligible, self._get_cluster_nodes()))

        def _sort_key(node):
            # Mirrors Ray Core's bundle ordering in scarcest resource first, most of it first.
            # Core also ranks custom resources (between GPU and object store
            # memory); we skip them, since it's opaque to us. NodeID breaks ties so
            # the order is stable across ticks.
            resources = node.get("Resources", {})
            return (
                -resources.get("GPU", 0),
                -resources.get("object_store_memory", 0),
                -resources.get("memory", 0),
                -resources.get("CPU", 0),
                node.get("NodeID", ""),
            )

        nodes = sorted(nodes, key=_sort_key)
        cluster_node_resources: Dict[Optional[LabelValue], NodeResources] = {}
        for node in nodes:
            # Safeguard against case where the value of Labels is None.
            labels = node.get("Labels") or {}
            subcluster = labels.get(SUBCLUSTER_LABEL_KEY, DEFAULT_SUBCLUSTER)
            node_id = node.get("NodeID", "")
            per_node_resource = cluster_node_resources.setdefault(subcluster, {})
            per_node_resource[node_id] = node["Resources"]
        if cluster_node_resources == self._cluster_node_resources:
            return False
        logger.debug("Cluster resources updated: %s.", cluster_node_resources)
        self._cluster_node_resources = cluster_node_resources
        return True

    def _refresh_resource_reservations(self):
        """Rereserve cluster resources.

        Each requester's subcluster comes from its ``subcluster_selector``.
        A requester without one is eligible only for the ``None`` bucket.
        """
        now = self._get_current_time()
        cluster_node_resources = copy.deepcopy(self._cluster_node_resources)
        live_items = [
            (req_id, req)
            for req_id, req in self._ongoing_reqs.items()
            if req.expiration_time >= now
        ]
        live_items.sort(key=lambda item: item[1])

        def _subcluster_of(requester_id: RequesterId) -> Optional[LabelValue]:
            selector = self._subcluster_selectors.get(requester_id)
            return (selector or {}).get(SUBCLUSTER_LABEL_KEY, DEFAULT_SUBCLUSTER)

        # TODO(hchen): Optimize the following triple loop.
        for requester_id, ongoing_req in live_items:
            ongoing_req.reserved_resources = {}
            subcluster = _subcluster_of(requester_id)
            node_resources = cluster_node_resources.get(subcluster, {})
            reservations = _compute_reservations(
                ongoing_request=ongoing_req,
                node_resources=node_resources,
            )
            for node_id, bundle in reservations.items():
                _subtract_bundle_in_place(node_resources[node_id], bundle)
                _add_bundle_in_place(
                    ongoing_req.reserved_resources.setdefault(node_id, {}),
                    bundle,
                )

        # Reserve remaining resources. For each resource type, concurrent
        # requesters in the same subcluster that asked for that type, split
        # the leftover equally.
        remaining_items = [
            (req_id, req) for req_id, req in live_items if req.request_remaining
        ]
        for subcluster, node_resources_leftover in cluster_node_resources.items():
            eligible = [
                req
                for req_id, req in remaining_items
                if _subcluster_of(req_id) == subcluster
            ]
            if not eligible:
                continue
            for node_id, node_resource_leftover in node_resources_leftover.items():
                for res_type, amount in node_resource_leftover.items():
                    if amount <= 0:
                        continue
                    type_eligible = [
                        req for req in eligible if res_type in req.request_remaining
                    ]
                    if not type_eligible:
                        continue
                    # Integer division may leave some resources unreserved.
                    share = amount // len(type_eligible)
                    if share <= 0:
                        continue
                    for r in type_eligible:
                        _add_bundle_in_place(
                            r.reserved_resources.setdefault(node_id, {}),
                            {res_type: share},
                        )

        if logger.isEnabledFor(logging.DEBUG):
            msg = "Reserved resources:\n"
            for requester_id, ongoing_req in self._ongoing_reqs.items():
                requested_resources_log_str = _format_resource_bundles_for_log(
                    ongoing_req.requested_resources
                )
                reserved_resources_log_str = _format_node_resources_for_log(
                    ongoing_req.reserved_resources
                )
                msg += (
                    f"Requester {requester_id}: wants {requested_resources_log_str}, "
                    f"has {reserved_resources_log_str}\n"
                )
            logger.debug(msg)


def _compute_reservations(
    ongoing_request: OngoingRequest,
    node_resources: NodeResources,
) -> ReservedResources:
    """Compute per-node reservations for ``ongoing_request`` without mutating inputs.

    Reservation is best effort in all strategies: a bundle that fits nowhere is
    skipped and the remaining bundles are still attempted, so a partially
    satisfiable request still makes progress. Callers detect the shortfall by
    comparing what came back against what they asked for.

    Args:
        ongoing_request: The request to place.
        node_resources: Remaining per-node capacity. Not mutated; a working copy
            is used so later bundles in this request see prior placements.

    Returns:
        Resources to reserve, keyed by node id.
    """
    # Working copy so placement can account for earlier bundles in this request
    # without mutating the caller's remaining capacity.
    available = {
        node_id: dict(resources) for node_id, resources in node_resources.items()
    }
    node_items = list(available.items())
    if not node_items or not ongoing_request.requested_resources:
        return {}

    strategy = ongoing_request.strategy
    reservations: ReservedResources = {}

    if strategy is ResourceRequestStrategy.STRICT_PACK:
        for node_id, node_resource in node_items:
            bundle = ongoing_request.requested_resources_sum()
            if _bundle_can_fit_on_node(bundle=bundle, node=node_resource):
                _subtract_bundle_in_place(node_resource, bundle)
                _add_bundle_in_place(
                    reservations.setdefault(node_id, {}),
                    bundle,
                )
                return reservations
        # No node could hold the full request.
        return {}

    used_node_ids: Set[NodeIdStr] = set()
    scan_start: int = 0
    for bundle in ongoing_request.requested_resources:
        for offset in range(len(node_items)):
            idx = (scan_start + offset) % len(node_items)
            node_id, node_resource = node_items[idx]
            if (
                strategy is ResourceRequestStrategy.STRICT_SPREAD
                and node_id in used_node_ids
            ):
                # NOTE: since strict_spread, we cannot reuse this node_id
                continue
            if _bundle_can_fit_on_node(bundle=bundle, node=node_resource):
                _subtract_bundle_in_place(node_resource, bundle)
                _add_bundle_in_place(
                    reservations.setdefault(node_id, {}),
                    bundle,
                )
                used_node_ids.add(node_id)
                scan_start = idx
                if strategy.is_spread_like():
                    scan_start += 1
                break

    return reservations


def _bundle_can_fit_on_node(bundle: ResourceDict, node: ResourceDict) -> bool:
    return not any(node.get(key, 0) < bundle[key] for key in bundle)


def _subtract_bundle_in_place(target: ResourceDict, bundle: ResourceDict) -> None:
    """Subtract ``bundle`` from ``target`` in place."""
    for key, amount in bundle.items():
        if key in target:
            target[key] -= amount


def _add_bundle_in_place(target: ResourceDict, bundle: ResourceDict) -> None:
    """Add ``bundle`` into ``target`` in place."""
    for key, amount in bundle.items():
        if amount:
            target[key] = target.get(key, 0) + amount


_get_or_create_lock = threading.Lock()


def get_or_create_autoscaling_coordinator():
    """Get or create the AutoscalingCoordinator actor."""
    # Create the actor on the local node,
    # to reduce network overhead.
    scheduling_strategy = NodeAffinitySchedulingStrategy(
        ray.get_runtime_context().get_node_id(),
        soft=False,
    )
    actor_cls = ray.remote(num_cpus=0, max_restarts=-1, max_task_retries=-1)(
        _AutoscalingCoordinatorActor
    ).options(
        name="AutoscalingCoordinator",
        namespace="AutoscalingCoordinator",
        get_if_exists=True,
        lifetime="detached",
        scheduling_strategy=scheduling_strategy,
    )
    # NOTE: Need the following lock, because Ray Core doesn't allow creating the same
    # actor from multiple threads simultaneously.
    with _get_or_create_lock:
        return actor_cls.remote()
