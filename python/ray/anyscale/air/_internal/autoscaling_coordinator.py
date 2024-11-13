import copy
import logging
import math
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)

ResourceDict = Dict[str, float]


class ResourceRequestPriority(Enum):
    """Priority of a resource request."""

    LOW = -10
    MEDIUM = 0
    HIGH = 10


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


class AutoscalingCoordinator:
    """An actor to coordinate autoscaling resource requests
    from different components.

    This actor is responsible for:
    * Merging received requests and dispatching them to Ray Autoscaler.
    * Allocating cluster resources to the requesters.
    """

    TICK_INTERVAL_S = 20

    def __init__(self):
        self._ongoing_reqs: Dict[str, OngoingRequest] = {}
        self._cluster_node_resources: List[ResourceDict] = []
        self._update_cluster_node_resources()

        # This is an actor, so the following check should always be True.
        # It's only needed for unit tests.
        if ray.is_initialized():
            self._self_handle = ray.get_runtime_context().current_actor

            # Start a thread to perform periodical operations.
            def tick_thread_run():
                while True:
                    # Call tick() as an actor task,
                    # so we don't need to handle multi-threading.
                    time.sleep(self.TICK_INTERVAL_S)
                    ray.get(self._self_handle.tick.remote())

            self._tick_thread = threading.Thread(target=tick_thread_run, daemon=True)
            self._tick_thread.start()

    def tick(self):
        """Used to perform periodical operations, e.g., purge expired requests,
        merge and send requests, check cluster resource updates, etc."""
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
        """Request resources from the AutoscalingCoordinator. A request with
        the same requester_id will overwrite the previous one.

        Args:
            requester_id: The unique identifier of the requester.
            resources: The requested resources.
            expire_after_s: Time in seconds after which this request will expire.
                The requester is responsible for periodically sending new requests
                to avoid the request being purged.
            request_remaining: If true, after allocating requested resources to each
                requester, remaining resources will also be allocated to this requester.
        """
        logger.debug("Received request from %s: %s.", requester_id, resources)
        # Round up the resource values to integers,
        # because the Autoscaler SDK only accepts integer values.
        for r in resources:
            for k in r:
                r[k] = math.ceil(r[k])
        now = time.time()
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
        """Cancel the resource request from the given requester."""
        logger.debug("Canceling request for %s.", requester_id)
        if requester_id not in self._ongoing_reqs:
            return
        del self._ongoing_reqs[requester_id]
        self._merge_and_send_requests()
        self._reallocate_resources()

    def _purge_expired_requests(self):
        now = time.time()
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
        ray.autoscaler.sdk.request_resources(bundles=merged_req)

    def get_allocated_resources(self, requester_id: str) -> List[ResourceDict]:
        """Get the allocated resources for the requester."""
        if requester_id not in self._ongoing_reqs:
            return []
        return self._ongoing_reqs[requester_id].allocated_resources

    def _maybe_sutract_resources(self, res1: ResourceDict, res2: ResourceDict) -> bool:
        """If res2<=res1, subtract res2 from res1 in-place, and return True.
        Otherwise return False."""
        if any(res1.get(key, 0) < res2[key] for key in res2):
            return False
        for key in res2:
            res1[key] -= res2[key]
        return True

    def _update_cluster_node_resources(self) -> bool:
        """Update cluster's total resources. Return True if changed."""
        nodes = [node for node in ray.nodes() if node["Alive"]]
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
        now = time.time()
        cluster_node_resources = copy.deepcopy(self._cluster_node_resources)
        ongoing_reqs = sorted(
            [req for req in self._ongoing_reqs.values() if req.expiration_time >= now]
        )
        # Allocate resources to ongoing requests.
        # TODO(hchen): Optimize the following triple loop.
        for ongoing_req in ongoing_reqs:
            ongoing_req.allocated_resources = []
            for req in ongoing_req.requested_resources:
                for node_resource in cluster_node_resources:
                    if self._maybe_sutract_resources(node_resource, req):
                        ongoing_req.allocated_resources.append(req)
                        break
        # Allocate remaining resources.
        # NOTE, to handle the case where multiple datasets are running concurrently,
        # now we double-allocate remaining resources to all requesters with
        # `request_remaining=True`.
        # This achieves parity with the behavior before Ray Data was integrated with
        # AutoscalingCoordinator, where each dataset assumes it has the whole cluster.
        # TODO(hchen): handle multiple request_remaining requests better.
        for ongoing_req in ongoing_reqs:
            if ongoing_req.request_remaining:
                ongoing_req.allocated_resources.extend(cluster_node_resources)

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
        AutoscalingCoordinator
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
