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
from ray.autoscaler._private.constants import env_integer
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)

HEAD_NODE_RESOURCE_LABEL = "node:__internal_head__"


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


def handle_timeout_errors(
    failure_counter_attr: str,
    operation_name: str,
    requester_id_param: str = "requester_id",
    error_msg_suffix: Optional[str] = None,
    on_error_return: Optional[Callable] = None,
):
    """Decorator to handle GetTimeoutError with consecutive failure tracking.

    Args:
        failure_counter_attr: Name of the instance attribute that tracks
            consecutive failures.
        operation_name: Name of the operation for error messages (e.g.,
            "send resource request", "cancel resource request").
        requester_id_param: Name of the parameter that contains the
            requester_id.
        error_msg_suffix: Optional suffix to append to the error message.
            If None, uses a default message.
        on_error_return: Optional callable that takes (self, requester_id)
            and returns a value to return on error. If None, no value is
            returned (method should return None).

    Returns:
        A decorator that wraps methods to handle timeout errors.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # Extract requester_id from args or kwargs
            requester_id = kwargs.get(requester_id_param)
            if requester_id is None:
                # Try to get from args by checking function signature
                import inspect

                sig = inspect.signature(func)
                param_names = list(sig.parameters.keys())
                if requester_id_param in param_names:
                    param_index = param_names.index(requester_id_param) - 1
                    if param_index < len(args):
                        requester_id = args[param_index]

            failure_counter = getattr(self, failure_counter_attr)

            try:
                result = func(self, *args, **kwargs)
                # Reset counter on success
                setattr(self, failure_counter_attr, 0)
                return result
            except ray.exceptions.GetTimeoutError as exc:
                failure_counter += 1
                setattr(self, failure_counter_attr, failure_counter)

                consecutive_msg = (
                    f" (consecutive failures: {failure_counter})"
                    if failure_counter > 1
                    else ""
                )

                # Build error message
                base_msg = (
                    f"Failed to {operation_name} for {requester_id}."
                    f"{consecutive_msg}"
                )
                if error_msg_suffix is not None:
                    msg = f"{base_msg} {error_msg_suffix}"
                else:
                    msg = (
                        f"{base_msg}"
                        " If this only happens transiently during network"
                        " partition or CPU being overloaded, it's safe to"
                        " ignore this error."
                        " If this error persists, file a GitHub issue."
                    )

                # Check max failures and raise if exceeded
                if failure_counter >= self.MAX_CONSECUTIVE_FAILURES:
                    raise RuntimeError(
                        f"Failed to {operation_name} for {requester_id} "
                        f"after {failure_counter} consecutive failures."
                    ) from exc

                logger.warning(msg, exc_info=True)

                # Return value on error if callback provided
                if on_error_return is not None:
                    return on_error_return(self, requester_id)

        return wrapper

    return decorator


class DefaultAutoscalingCoordinator(AutoscalingCoordinator):
    AUTOSCALING_REQUEST_GET_TIMEOUT_S = env_integer(
        "RAY_DATA_AUTOSCALING_COORDINATOR_REQUEST_GET_TIMEOUT_S", 5
    )
    MAX_CONSECUTIVE_FAILURES = env_integer(
        "RAY_DATA_AUTOSCALING_COORDINATOR_MAX_CONSECUTIVE_FAILURES", 10
    )

    def __init__(self):
        self._cached_allocated_resources: Dict[str, List[ResourceDict]] = {}
        self._consecutive_failures_request_resources: int = 0
        self._consecutive_failures_cancel_request: int = 0
        self._consecutive_failures_get_allocated_resources: int = 0

    @functools.cached_property
    def _autoscaling_coordinator(self):
        # Create the coordinator actor lazily rather than eagerly in the constructor.
        return get_or_create_autoscaling_coordinator()

    @handle_timeout_errors(
        failure_counter_attr="_consecutive_failures_request_resources",
        operation_name="send resource request",
        error_msg_suffix=(
            "If this only happens transiently during network partition"
            " or CPU being overloaded, it's safe to ignore this error."
            " If this error persists, file a GitHub issue."
        ),
    )
    def request_resources(
        self,
        requester_id: str,
        resources: List[ResourceDict],
        expire_after_s: float,
        request_remaining: bool = False,
        priority: ResourceRequestPriority = ResourceRequestPriority.MEDIUM,
    ) -> None:
        ray.get(
            self._autoscaling_coordinator.request_resources.remote(
                requester_id=requester_id,
                resources=resources,
                expire_after_s=expire_after_s,
                request_remaining=request_remaining,
                priority=priority,
            ),
            timeout=self.AUTOSCALING_REQUEST_GET_TIMEOUT_S,
        )

    @handle_timeout_errors(
        failure_counter_attr="_consecutive_failures_cancel_request",
        operation_name="cancel resource request",
        error_msg_suffix=(
            "If this only happens transiently during network partition"
            " or CPU being overloaded, it's safe to ignore this error."
            " If this error persists, file a GitHub issue."
        ),
    )
    def cancel_request(self, requester_id: str):
        ray.get(
            self._autoscaling_coordinator.cancel_request.remote(
                requester_id,
            ),
            timeout=self.AUTOSCALING_REQUEST_GET_TIMEOUT_S,
        )

    @handle_timeout_errors(
        failure_counter_attr="_consecutive_failures_get_allocated_resources",
        operation_name="get allocated resources",
        error_msg_suffix=(
            "Returning cached value."
            " If this only happens transiently during network partition"
            " or CPU being overloaded, it's safe to ignore this error."
            " If this error persists, file a GitHub issue."
        ),
        on_error_return=lambda self, requester_id: (
            self._cached_allocated_resources.get(requester_id, [])
        ),
    )
    def get_allocated_resources(self, requester_id: str) -> List[ResourceDict]:
        result = ray.get(
            self._autoscaling_coordinator.get_allocated_resources.remote(
                requester_id,
            ),
            timeout=self.AUTOSCALING_REQUEST_GET_TIMEOUT_S,
        )
        self._cached_allocated_resources[requester_id] = result
        return result


class _AutoscalingCoordinatorActor:
    """An actor to coordinate autoscaling resource requests from different components.

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

        nodes = list(filter(_is_node_eligible, ray.nodes()))
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
                    if self._maybe_subtract_resources(node_resource, req):
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
