import copy
import functools
import logging
import math
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

import ray
import ray.exceptions
from .base_autoscaling_coordinator import (
    AutoscalingCoordinator,
    ResourceDict,
    ResourceRequestPriority,
)
from ray._common.utils import env_bool
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)

HEAD_NODE_RESOURCE_LABEL = "node:__internal_head__"

RAY_DATA_AUTOSCALING_COORDINATOR_LOG_TRACEBACK = env_bool(
    "RAY_DATA_AUTOSCALING_COORDINATOR_LOG_TRACEBACK", True
)


@dataclass
class OngoingRequest:
    """Represents an ongoing resource request from a requester."""

    # The time when the request was first received.
    first_request_time: float
    # Requested resources.
    requested_resources: List[ResourceDict]
    # The expiration time of the request.
    expiration_time: float
    # If true, after allocating requested resources to each requester,
    # remaining resources will also be allocated to this requester.
    request_remaining: bool
    # The priority of the request, higher value means higher priority.
    priority: int
    # Resources that are already allocated to the requester.
    allocated_resources: List[ResourceDict]

    def __lt__(self, other):
        """Used to sort requests when allocating resources.

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
    guarantee that ``request_resources`` and ``get_allocated_resources`` rely on.
    """

    def __init__(
        self,
        requester_id: str,
        autoscaling_coordinator_actor=None,  # For testing only: injects an actor instead of using the shared named singleton.
    ):
        self._requester_id = requester_id
        self._cached_allocated_resources: List[ResourceDict] = []
        # In-flight get_allocated_resources ref, or None if no request is pending.
        self._pending_allocated_resources: Optional[ray.ObjectRef] = None
        if autoscaling_coordinator_actor is not None:
            # Bypass the cached_property by injecting the actor directly.
            # Used in tests to avoid the shared named actor.
            self.__dict__["_autoscaling_coordinator"] = autoscaling_coordinator_actor

    @functools.cached_property
    def _autoscaling_coordinator(self):
        # Create the coordinator actor lazily rather than eagerly in the constructor.
        return get_or_create_autoscaling_coordinator()

    def request_resources(
        self,
        resources: List[ResourceDict],
        expire_after_s: float,
        request_remaining: bool = False,
        priority: ResourceRequestPriority = ResourceRequestPriority.MEDIUM,
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
        )

    def cancel_request(self) -> None:
        """Fire-and-forget: cancel a resource request on the coordinator actor.

        Also clears client-side state (pending ref and cached allocation) so
        a subsequent ``get_allocated_resources`` call returns a fresh result
        rather than stale data from a prior pipeline run.
        """
        self._pending_allocated_resources = None
        self._cached_allocated_resources = []
        self._autoscaling_coordinator.cancel_request.remote(self._requester_id)

    def get_allocated_resources(self) -> List[ResourceDict]:
        """Return allocated resources without blocking.

        Submits an async RPC and immediately returns the last cached result.
        The cache is updated the next time the pending RPC completes.

        Because the actor processes calls in FIFO order, the result always
        reflects state after all previously submitted ``request_resources`` calls
        to the same actor.

        On actor errors, returns the cached value and logs a warning; never raises.
        """
        ref = self._pending_allocated_resources
        if ref is not None:
            ready, _ = ray.wait([ref], timeout=0)
            if ready:
                self._pending_allocated_resources = None
                try:
                    self._cached_allocated_resources = ray.get(ref, timeout=0)
                except ray.exceptions.RayError:
                    logger.warning(
                        f"Failed to get allocated resources for {self._requester_id};"
                        " falling back to the cached value."
                        " If this persists, file a GitHub issue.",
                        exc_info=RAY_DATA_AUTOSCALING_COORDINATOR_LOG_TRACEBACK,
                    )

        # Submit a new request if none is currently in-flight
        # (first call, or the previous request completed or errored).
        if self._pending_allocated_resources is None:
            self._pending_allocated_resources = (
                self._autoscaling_coordinator.get_allocated_resources.remote(
                    self._requester_id,
                )
            )

        return self._cached_allocated_resources


class _AutoscalingCoordinatorActor:
    """An actor to coordinate autoscaling resource requests from different components.

    This actor is responsible for:
    * Merging received requests and dispatching them to Ray Autoscaler.
    * Allocating cluster resources to the requesters.
    """

    TICK_INTERVAL_S = 20

    def __init__(
        self,
        get_current_time: Callable[[], float] = time.time,
        send_resources_request: Callable[[List[ResourceDict]], None] = lambda bundles: (
            ray.autoscaler.sdk.request_resources(bundles=bundles)
        ),
        get_cluster_nodes: Callable[[], List[Dict]] = ray.nodes,
    ):
        self._get_current_time = get_current_time
        self._send_resources_request = send_resources_request
        self._get_cluster_nodes = get_cluster_nodes

        self._ongoing_reqs: Dict[str, OngoingRequest] = {}
        self._cluster_node_resources: List[ResourceDict] = []
        # Lock for thread-safe access to shared state from the background
        self._lock = threading.Lock()
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
        merge and send requests, check cluster resource updates, etc."""
        with self._lock:
            self._merge_and_send_requests()
            self._update_cluster_node_resources()
            self._reallocate_resources()

    def request_resources(
        self,
        requester_id: str,
        resources: List[ResourceDict],
        expire_after_s: float,
        request_remaining: bool = False,
        priority: ResourceRequestPriority = ResourceRequestPriority.MEDIUM,
    ) -> None:
        logger.debug("Received request from %s: %s.", requester_id, resources)
        with self._lock:
            # Round up the resource values to integers,
            # because the Autoscaler SDK only accepts integer values.
            for r in resources:
                for k in r:
                    r[k] = math.ceil(r[k])
            now = self._get_current_time()
            request_updated = False
            old_req = self._ongoing_reqs.get(requester_id)
            if old_req is not None:
                if request_remaining != old_req.request_remaining:
                    raise ValueError(
                        "Cannot change request_remaining flag of an ongoing request."
                    )
                if priority.value != old_req.priority:
                    raise ValueError("Cannot change priority of an ongoing request.")

                request_updated = resources != old_req.requested_resources
                old_req.requested_resources = resources
                old_req.expiration_time = now + expire_after_s
            else:
                request_updated = True
                self._ongoing_reqs[requester_id] = OngoingRequest(
                    first_request_time=now,
                    requested_resources=resources,
                    request_remaining=request_remaining,
                    priority=priority.value,
                    expiration_time=now + expire_after_s,
                    allocated_resources=[],
                )
            if request_updated:
                # If the request has updated, immediately send
                # a new request and reallocate resources.
                self._merge_and_send_requests()
                self._reallocate_resources()

    def cancel_request(
        self,
        requester_id: str,
    ):
        logger.debug("Canceling request for %s.", requester_id)
        with self._lock:
            if requester_id not in self._ongoing_reqs:
                return
            del self._ongoing_reqs[requester_id]
            self._merge_and_send_requests()
            self._reallocate_resources()

    def _purge_expired_requests(self):
        now = self._get_current_time()
        self._ongoing_reqs = {
            requester_id: req
            for requester_id, req in self._ongoing_reqs.items()
            if req.expiration_time > now
        }

    def _merge_and_send_requests(self):
        """Merge requests and send them to Ray Autoscaler."""
        self._purge_expired_requests()
        merged_req = []
        for req in self._ongoing_reqs.values():
            merged_req.extend(req.requested_resources)
        self._send_resources_request(merged_req)

    def get_allocated_resources(self, requester_id: str) -> List[ResourceDict]:
        """Get the allocated resources for the requester."""
        with self._lock:
            if requester_id not in self._ongoing_reqs:
                return []
            return self._ongoing_reqs[requester_id].allocated_resources

    def _maybe_subtract_resources(self, res1: ResourceDict, res2: ResourceDict) -> bool:
        """If res2<=res1, subtract res2 from res1 in-place, and return True.
        Otherwise return False."""
        if any(res1.get(key, 0) < res2[key] for key in res2):
            return False
        for key in res2:
            if key in res1:
                res1[key] -= res2[key]
        return True

    def _update_cluster_node_resources(self) -> bool:
        """Update cluster's total resources. Return True if changed."""

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
        nodes = sorted(nodes, key=lambda node: node.get("NodeID", ""))
        cluster_node_resources = [node["Resources"] for node in nodes]
        if cluster_node_resources == self._cluster_node_resources:
            return False
        else:
            logger.debug("Cluster resources updated: %s.", cluster_node_resources)
            self._cluster_node_resources = cluster_node_resources
            return True

    def _reallocate_resources(self):
        """Reallocate cluster resources."""
        now = self._get_current_time()
        cluster_node_resources = copy.deepcopy(self._cluster_node_resources)
        ongoing_reqs = sorted(
            [req for req in self._ongoing_reqs.values() if req.expiration_time >= now]
        )
        # Allocate resources to ongoing requests.
        maybe_subtract_resources = self._maybe_subtract_resources
        num_nodes = len(cluster_node_resources)
        # Assumption for this optimization (within one _reallocate_resources pass):
        # node resources only decrease, never increase. Therefore for identical
        # bundles:
        # 1) the first feasible node index can only stay or move right;
        # 2) if one identical bundle cannot fit any node, later identical bundles
        #    also cannot fit in this pass.
        # This is most beneficial when requested_resources has many duplicate
        # bundles (e.g., homogeneous node shapes and/or multiple executors
        # requesting similar specs).
        first_fit_start_idx = {}
        impossible_bundles = set()
        for ongoing_req in ongoing_reqs:
            allocated_resources = []
            ongoing_req.allocated_resources = allocated_resources
            for req in ongoing_req.requested_resources:
                req_key = frozenset(req.items())
                if req_key in impossible_bundles:
                    continue

                start_idx = first_fit_start_idx.get(req_key, 0)
                allocated = False
                for node_idx in range(start_idx, num_nodes):
                    if maybe_subtract_resources(cluster_node_resources[node_idx], req):
                        allocated_resources.append(req)
                        first_fit_start_idx[req_key] = node_idx
                        allocated = True
                        break

                if not allocated:
                    impossible_bundles.add(req_key)
        # Allocate remaining resources.
        # NOTE: to handle the case where multiple datasets are running concurrently,
        # we divide remaining resources equally to all requesters with `request_remaining=True`.
        remaining_resource_requesters = [
            req for req in ongoing_reqs if req.request_remaining
        ]
        num_remaining_requesters = len(remaining_resource_requesters)
        if num_remaining_requesters > 0:
            for node_resource in cluster_node_resources:
                # Divide remaining resources equally among requesters.
                # NOTE: Integer division may leave some resources unallocated.
                divided_resource = {
                    k: v // num_remaining_requesters for k, v in node_resource.items()
                }
                for ongoing_req in remaining_resource_requesters:
                    if any(v > 0 for v in divided_resource.values()):
                        ongoing_req.allocated_resources.append(divided_resource)

        if logger.isEnabledFor(logging.DEBUG):
            msg = "Allocated resources:\n"
            for requester_id, ongoing_req in self._ongoing_reqs.items():
                msg += f"Requester {requester_id}: {ongoing_req.allocated_resources}\n"
            logger.debug(msg)


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
