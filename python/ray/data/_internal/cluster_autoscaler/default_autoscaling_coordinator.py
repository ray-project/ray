import copy
import functools
import logging
import math
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

import ray
import ray.exceptions
from .base_autoscaling_coordinator import (
    AutoscalingCoordinator,
    ResourceDict,
    ResourceRequestPriority,
)
from ray._common.utils import env_bool
from ray.autoscaler._private.constants import env_integer
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
    """Client-side proxy for the _AutoscalingCoordinatorActor.

    All public methods (request_resources, cancel_request,
    get_allocated_resources) are non-blocking. Not thread-safe; assumed to be
    called from a single scheduling thread per instance.

    Single-tenant: every instance is owned by exactly one
    DefaultClusterAutoscalerV2 and must be called with a single, fixed
    requester_id for its entire lifetime. Passing different requester_ids to
    the same instance would corrupt in-flight state because the pending slots
    and failure counters are scalars, not keyed dicts. Violating this is
    undefined behavior.
    """

    AUTOSCALING_REQUEST_GET_TIMEOUT_S = env_integer(
        "RAY_DATA_AUTOSCALING_COORDINATOR_REQUEST_GET_TIMEOUT_S", 5
    )
    MAX_CONSECUTIVE_FAILURES = env_integer(
        "RAY_DATA_AUTOSCALING_COORDINATOR_MAX_CONSECUTIVE_FAILURES", 10
    )

    def __init__(self):
        self._cached_allocated_resources: List[ResourceDict] = []

        # In-flight async request per public method: (ObjectRef, submit_time) or None.
        self._pending_allocated_resources: Optional[Tuple[ray.ObjectRef, float]] = None
        self._pending_request_resources: Optional[Tuple[ray.ObjectRef, float]] = None
        self._pending_cancel_request: Optional[Tuple[ray.ObjectRef, float]] = None

        self._consecutive_failures_request_resources: int = 0
        self._consecutive_failures_cancel_request: int = 0
        self._consecutive_failures_get_allocated_resources: int = 0

    @functools.cached_property
    def _autoscaling_coordinator(self):
        # Create the coordinator actor lazily rather than eagerly in the constructor.
        return get_or_create_autoscaling_coordinator()

    def _record_failure(
        self,
        counter_attr: str,
        operation_name: str,
        requester_id: str,
        exc: Optional[Exception] = None,
    ) -> None:
        """Increment the failure counter; raise RuntimeError if the maximum is reached.

        The counter is reset to zero before raising so that the next call starts
        fresh after the exception has propagated and any higher-level recovery has
        taken place, rather than immediately re-raising.

        `exc` is the underlying exception when the actor task failed, or None
        when the request timed out without completing.
        """
        counter = getattr(self, counter_attr) + 1
        setattr(self, counter_attr, counter)
        consecutive_msg = f" (consecutive failures: {counter})" if counter > 1 else ""
        if counter >= self.MAX_CONSECUTIVE_FAILURES:
            setattr(self, counter_attr, 0)
            raise RuntimeError(
                f"Failed to {operation_name} for {requester_id} "
                f"after {counter} consecutive failures."
            ) from exc
        prefix = "Timed out on" if exc is None else "Failed to"
        logger.warning(
            f"{prefix} {operation_name} for {requester_id}{consecutive_msg}."
            " If this only happens transiently during network partition"
            " or CPU being overloaded, it's safe to ignore this error."
            " If this error persists, file a GitHub issue."
        )
        if exc is not None and RAY_DATA_AUTOSCALING_COORDINATOR_LOG_TRACEBACK:
            logger.debug(
                f"Traceback for {operation_name} failure for {requester_id}:",
                exc_info=True,
            )

    def _try_cancel(self, ref, operation_name: str, requester_id: str) -> None:
        """Best-effort soft cancel of an ObjectRef.

        Swallows all exceptions rather than propagating them, so a failing
        cancel cannot derail the caller. Logs the suppressed exception at DEBUG.
        """
        try:
            ray.cancel(ref, force=False)
        except Exception:
            logger.debug(
                f"Best-effort cancel failed for {operation_name} for {requester_id}.",
                exc_info=True,
            )

    def _resolve_pending(
        self,
        pending_attr: str,
        requester_id: str,
        counter_attr: str,
        operation_name: str,
        on_success: Optional[Callable] = None,
    ) -> None:
        """Check if a pending async request has completed and handle the result.

        If the request is ready, consume it: reset the failure counter and call
        on_success on success, or call _record_failure on actor error. If the
        request has timed out, issue a soft cancel (force=False) and discard the
        ref, then record a timeout failure. Does nothing if no request is pending.
        """
        pending = getattr(self, pending_attr)
        if pending is None:
            return
        ref, submit_time = pending
        ready, _ = ray.wait([ref], timeout=0)
        if ready:
            setattr(self, pending_attr, None)
            try:
                result = ray.get(ref)
                setattr(self, counter_attr, 0)
                if on_success is not None:
                    on_success(result)
            except ray.exceptions.RayError as exc:
                self._record_failure(counter_attr, operation_name, requester_id, exc)
        elif time.time() - submit_time > self.AUTOSCALING_REQUEST_GET_TIMEOUT_S:
            # Clear before cancelling for defense-in-depth: _try_cancel doesn't raise,
            # but cleanup should not depend on that guarantee.
            setattr(self, pending_attr, None)
            self._try_cancel(ref, operation_name, requester_id)
            self._record_failure(counter_attr, operation_name, requester_id)

    def request_resources(
        self,
        requester_id: str,
        resources: List[ResourceDict],
        expire_after_s: float,
        request_remaining: bool = False,
        priority: ResourceRequestPriority = ResourceRequestPriority.MEDIUM,
    ) -> None:
        """Fire-and-forget: submit a resource request to the coordinator actor.

        Returns immediately. If the previous request has completed, its result
        (including any actor error) is checked first and the failure counter is
        updated accordingly. If the previous request is still in-flight, this
        call is skipped — the next call after the slot frees will carry the
        latest state.
        """
        self._resolve_pending(
            "_pending_request_resources",
            requester_id,
            "_consecutive_failures_request_resources",
            "send resource request",
        )
        if self._pending_request_resources is None:
            self._pending_request_resources = (
                self._autoscaling_coordinator.request_resources.remote(
                    requester_id=requester_id,
                    resources=resources,
                    expire_after_s=expire_after_s,
                    request_remaining=request_remaining,
                    priority=priority,
                ),
                time.time(),
            )
        else:
            logger.debug(
                "Skipping request_resources for %s; previous request still in-flight.",
                requester_id,
            )

    def _clear_requester_state(self, requester_id: str) -> None:
        """Cancel in-flight requests and clear all client-side state for a requester.

        Called by cancel_request to prevent memory accumulation across many
        executions in a long-running process.
        """
        for pending_attr, operation_name in [
            ("_pending_request_resources", "send resource request"),
            ("_pending_allocated_resources", "get allocated resources"),
        ]:
            pending = getattr(self, pending_attr)
            if pending is not None:
                old_ref, _ = pending
                setattr(self, pending_attr, None)
                self._try_cancel(old_ref, operation_name, requester_id)
        self._cached_allocated_resources = []

    def cancel_request(self, requester_id: str) -> None:
        """Fire-and-forget: cancel a resource request on the coordinator actor.

        Returns immediately. If the previous cancel has completed, its result is
        checked first and the failure counter updated. If a cancel is already
        in-flight, the actor RPC is skipped — the in-flight cancel is idempotent
        provided no request_resources call arrives after this cancel_request.
        """
        self._resolve_pending(
            "_pending_cancel_request",
            requester_id,
            "_consecutive_failures_cancel_request",
            "cancel resource request",
        )
        # Always clear client-side state, regardless of whether an actor RPC
        # is in-flight. This ensures in-flight request_resources and
        # get_allocated_resources refs are cancelled and the cache is zeroed
        # even if the actor cancel RPC is skipped.
        self._clear_requester_state(requester_id)
        if self._pending_cancel_request is None:
            self._pending_cancel_request = (
                self._autoscaling_coordinator.cancel_request.remote(requester_id),
                time.time(),
            )
        else:
            logger.debug(
                "Skipping cancel_request for %s; previous cancel still in-flight.",
                requester_id,
            )

    def get_allocated_resources(self, requester_id: str) -> List[ResourceDict]:
        """Get the allocated resources for the requester without blocking.

        Submits an async request to the autoscaling coordinator actor and
        immediately returns the last cached value. The cache is updated whenever
        a pending request completes.

        Correctness relies on the actor processing requests in FIFO order: the
        response reflects state after all previously submitted request_resources
        calls from this driver.

        On repeated failures (actor errors or timeouts), falls back to the cached
        value and raises RuntimeError after MAX_CONSECUTIVE_FAILURES.
        """

        def _on_success(result):
            self._cached_allocated_resources = result

        self._resolve_pending(
            "_pending_allocated_resources",
            requester_id,
            "_consecutive_failures_get_allocated_resources",
            "get allocated resources",
            on_success=_on_success,
        )

        # Submit a new request if none is currently in-flight
        # (first call, or the previous request completed, errored, or timed out).
        if self._pending_allocated_resources is None:
            ref = self._autoscaling_coordinator.get_allocated_resources.remote(
                requester_id,
            )
            self._pending_allocated_resources = (ref, time.time())

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
        # TODO(hchen): Optimize the following triple loop.
        for ongoing_req in ongoing_reqs:
            ongoing_req.allocated_resources = []
            for req in ongoing_req.requested_resources:
                for node_resource in cluster_node_resources:
                    if self._maybe_subtract_resources(node_resource, req):
                        ongoing_req.allocated_resources.append(req)
                        break
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
