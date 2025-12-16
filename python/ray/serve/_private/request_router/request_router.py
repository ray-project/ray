import asyncio
import enum
import logging
import math
import random
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from typing import (
    AsyncGenerator,
    Callable,
    DefaultDict,
    Deque,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
)

from ray.actor import ActorHandle
from ray.exceptions import ActorDiedError, ActorUnavailableError
from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    ReplicaID,
    ReplicaQueueLengthInfo,
    RequestMetadata,
    RunningReplicaInfo,
)
from ray.serve._private.constants import (
    DEFAULT_LATENCY_BUCKET_MS,
    RAY_SERVE_MAX_QUEUE_LENGTH_RESPONSE_DEADLINE_S,
    RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S,
    RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S,
    RAY_SERVE_ROUTER_RETRY_BACKOFF_MULTIPLIER,
    RAY_SERVE_ROUTER_RETRY_INITIAL_BACKOFF_S,
    RAY_SERVE_ROUTER_RETRY_MAX_BACKOFF_S,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.request_router.common import (
    PendingRequest,
    ReplicaQueueLengthCache,
)
from ray.serve._private.request_router.replica_wrapper import RunningReplica
from ray.util import metrics
from ray.util.annotations import PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)


class LocalityScope(str, enum.Enum):
    NODE = "NODE"
    AVAILABILITY_ZONE = "AVAILABILITY_ZONE"


@PublicAPI(stability="alpha")
class LocalityMixin:
    """Mixin for locality routing.

    This mixin is used to route requests to replicas that are colocated
    with the handle. It adds necessary attributes and methods to keep track of
    locality scopes and offer the helpers to apply locality routing and
    rank replicas based on locality.
    """

    def __init__(
        self,
        self_node_id: Optional[str] = None,
        prefer_local_node_routing: bool = False,
        prefer_local_az_routing: bool = False,
        self_availability_zone: Optional[str] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._self_node_id = self_node_id
        self._prefer_local_node_routing = prefer_local_node_routing
        self._prefer_local_az_routing = prefer_local_az_routing
        self._self_availability_zone = self_availability_zone

        # Colocated replicas (e.g. wrt node, AZ)
        self._colocated_replica_ids: DefaultDict[
            LocalityScope, Set[ReplicaID]
        ] = defaultdict(set)
        self._replica_id_set: Set[ReplicaID] = set()

    def _discard_colocated_replica_ids_on_replica_actor_died(
        self, replica_id: ReplicaID
    ):
        """Remove the replica ID from the colocated replica IDs.
        This is called when a replica actor dies.
        """
        for id_set in self._colocated_replica_ids.values():
            id_set.discard(replica_id)

    def _update_colocated_replica_ids_with_replicas(
        self, replicas: List[RunningReplica]
    ):
        """Update the colocated replica IDs based on the replicas.
        This is called when the replicas are updated.
        """
        new_colocated_replica_ids = defaultdict(set)

        for r in replicas:
            if self._self_node_id is not None and r.node_id == self._self_node_id:
                new_colocated_replica_ids[LocalityScope.NODE].add(r.replica_id)
            if (
                self._self_availability_zone is not None
                and r.availability_zone == self._self_availability_zone
            ):
                new_colocated_replica_ids[LocalityScope.AVAILABILITY_ZONE].add(
                    r.replica_id
                )

        self._colocated_replica_ids = new_colocated_replica_ids

    def apply_locality_routing(
        self,
        pending_request: Optional[PendingRequest] = None,
    ) -> Set[ReplicaID]:
        """Apply locality routing to the pending request.

        When the reqeust is None, return all replicas. Each call will try to
        route the request to replicas in the priority of first on the
        same node, then in the same availability zone, and finally all
        replicas.

        Args:
            pending_request: The pending request to be routed.
        Returns:
            A set of replica IDs that are candidates based on
            the locality policy.
        """

        if not pending_request:
            return self._replica_id_set

        if (
            self._prefer_local_node_routing
            and not pending_request.routing_context.tried_same_node
            and len(self._colocated_replica_ids[LocalityScope.NODE]) > 0
        ):
            # Attempt to route requests to replicas on the
            # same node at most once
            candidate_replica_ids = self._colocated_replica_ids[LocalityScope.NODE]
            pending_request.routing_context.tried_same_node = True
            pending_request.routing_context.should_backoff = False
        elif (
            self._prefer_local_az_routing
            and not pending_request.routing_context.tried_same_az
            and len(self._colocated_replica_ids[LocalityScope.AVAILABILITY_ZONE]) > 0
        ):
            # Attempt to route requests to replicas in the same
            # AZ at most once
            candidate_replica_ids = self._colocated_replica_ids[
                LocalityScope.AVAILABILITY_ZONE
            ]
            pending_request.routing_context.tried_same_az = True
            pending_request.routing_context.should_backoff = False
        else:
            # On subsequent iterations or when there are no replicas on the same
            # node or AZ, consider all available replicas.
            candidate_replica_ids = self._replica_id_set
            pending_request.routing_context.should_backoff = True
        return candidate_replica_ids

    def rank_replicas_via_locality(
        self,
        replicas: List[RunningReplica],
    ) -> List[List[RunningReplica]]:
        """Rank the replicas based on the locality preference.
        Rank 0 is the list of replicas that are on the same node.
        Rank 1 is the list of replicas that are on the same availability zone.
        Rank 2 is the list of all other replicas.
        """
        ranked_replicas = [[] for _ in range(3)]
        for replica in replicas:
            if replica.replica_id in self._colocated_replica_ids[LocalityScope.NODE]:
                ranked_replicas[0].append(replica)
            elif (
                replica.replica_id
                in self._colocated_replica_ids[LocalityScope.AVAILABILITY_ZONE]
            ):
                ranked_replicas[1].append(replica)
            else:
                ranked_replicas[2].append(replica)
        return ranked_replicas


@PublicAPI(stability="alpha")
class MultiplexMixin:
    """Mixin for multiplex routing.

    This mixin is used to route requests to replicas that are multiplexed.
    It adds necessary attributes and methods to keep track of multiplexed
    model IDs and offer the helpers to apply multiplex routing and rank
    replicas based on multiplexed model IDs.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._multiplexed_model_id_to_replica_ids: DefaultDict[
            str, Set[ReplicaID]
        ] = defaultdict(set)

        # When there is no match for a multiplexed model id, we will try to fall back
        # to all replicas immediately. This set is used to make sure we only fall back
        # once for concurrent requests for the same model id.
        # Whenever there is a match, we will remove the model id from this set.
        self._multiplexed_model_id_fallback_match: Set[str] = set()
        self._replica_id_set: Set[ReplicaID] = set()
        self._replicas: Dict[ReplicaID, RunningReplica] = {}

    def _get_pending_request_matching_multiplexed_model_id(
        self,
        request_metadata: Optional[RequestMetadata] = None,
    ) -> Optional[PendingRequest]:
        """Matching pending request based on the request metadata."""
        if request_metadata is None or not request_metadata.multiplexed_model_id:
            return None

        for pr in self._pending_requests_to_fulfill:
            if (
                not pr.future.done()
                and pr.metadata.multiplexed_model_id
                == request_metadata.multiplexed_model_id
            ):
                return pr

    def _update_multiplexed_model_ids_with_replicas(
        self, replicas: List[RunningReplica]
    ):
        """Update the multiplexed model IDs based on the replicas.

        This should be called when the replicas are updated.
        """
        new_multiplexed_model_id_to_replica_ids = defaultdict(set)

        for r in replicas:
            for model_id in r.multiplexed_model_ids:
                new_multiplexed_model_id_to_replica_ids[model_id].add(r.replica_id)

        self._multiplexed_model_id_to_replica_ids = (
            new_multiplexed_model_id_to_replica_ids
        )

    def _get_replica_ids_with_fewest_multiplexed_models(self) -> Set[str]:
        """Get the set of replicas that have the fewest multiplexed models loaded."""
        candidates = set()
        sorted_replicas = sorted(
            self._replicas.values(), key=lambda x: len(x.multiplexed_model_ids)
        )
        least_num_multiplexed_model_ids = math.inf
        for replica in sorted_replicas:
            if len(replica.multiplexed_model_ids) <= least_num_multiplexed_model_ids:
                candidates.add(replica.replica_id)
                least_num_multiplexed_model_ids = len(replica.multiplexed_model_ids)
            else:
                break

        return candidates

    @property
    def _multiplexed_matching_timeout(self) -> float:
        return random.uniform(
            RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S,
            RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S * 2,
        )

    def apply_multiplex_routing(
        self,
        pending_request: Optional[PendingRequest] = None,
    ) -> Set[ReplicaID]:
        """Apply multiplex routing to the pending request.

        When the request is None, return all replicas. Each call will try to
        route the request to the replicas that have the multiplexed model ID
        to the hierarchy of first the replicas with the multiplexed model ID,
        then the replicas with the fewest multiplexed models, and finally all
        replicas.

        Args:
            pending_request: The pending request to be routed based on
                multiplexed model policy.

        Returns:
            A set of replica IDs that are candidates for the existing
            routing call.
        """
        if not pending_request:
            return self._replica_id_set

        if not pending_request.routing_context.multiplexed_start_matching_time:
            pending_request.routing_context.multiplexed_start_matching_time = (
                time.time()
            )

        multiplexed_start_matching_time = (
            pending_request.routing_context.multiplexed_start_matching_time
        )
        multiplexed_model_id = pending_request.metadata.multiplexed_model_id
        if (
            time.time() - multiplexed_start_matching_time
            < self._multiplexed_matching_timeout
        ):
            candidate_replica_ids = self._multiplexed_model_id_to_replica_ids.get(
                multiplexed_model_id, None
            )
            if (
                not candidate_replica_ids
                and multiplexed_model_id
                not in self._multiplexed_model_id_fallback_match
            ) or pending_request.routing_context.tried_first_multiplexed_models:
                # When there is no match for a multiplexed model id
                # or when the replica(s) with the matching model id is busy,
                # first try to fall back to replicas with the fewest models.
                candidate_replica_ids = (
                    self._get_replica_ids_with_fewest_multiplexed_models()
                )
                self._multiplexed_model_id_fallback_match.add(multiplexed_model_id)
            elif candidate_replica_ids:
                self._multiplexed_model_id_fallback_match.discard(multiplexed_model_id)
            pending_request.routing_context.tried_first_multiplexed_models = True
        elif not pending_request.routing_context.tried_fewest_multiplexed_models:
            # After the `_multiplexed_matching_timeout` is up, first try
            # routing to replicas that have the fewest models loaded.
            # We only try this once to avoid deterministically retrying on
            # the same replicas repeatedly.
            candidate_replica_ids = (
                self._get_replica_ids_with_fewest_multiplexed_models()
            )
            pending_request.routing_context.tried_fewest_multiplexed_models = True
        else:
            # If the timeout is up, and we've already tried the candidates
            # with the fewest models loaded, fall back to all replicas.
            candidate_replica_ids = self._replica_id_set

        pending_request.routing_context.should_backoff = True
        return candidate_replica_ids

    def rank_replicas_via_multiplex(
        self,
        replicas: List[RunningReplica],
        multiplexed_model_id: str,
    ) -> List[List[RunningReplica]]:
        """Rank the replicas based on the multiplexed model ID.
        Rank 0 is the list of replicas that have the multiplexed model ID.
        Rank 1 is the list of replicas that have the fewest multiplexed models.
        Rank 2 is the list of all other replicas.
        """
        replica_ids_with_multiplexed_model = (
            self._multiplexed_model_id_to_replica_ids.get(multiplexed_model_id, set())
        )
        replica_ids_with_fewest_multiplexed_models = (
            self._get_replica_ids_with_fewest_multiplexed_models()
        )

        ranked_replicas = [[] for _ in range(3)]
        for replica in replicas:
            if replica.replica_id in replica_ids_with_multiplexed_model:
                ranked_replicas[0].append(replica)
            elif replica.replica_id in replica_ids_with_fewest_multiplexed_models:
                ranked_replicas[1].append(replica)
            else:
                ranked_replicas[2].append(replica)
        return ranked_replicas


@PublicAPI(stability="alpha")
class FIFOMixin:
    """Mixin for FIFO routing.

    This mixin is used to route requests in FIFO order, optionally prioritizing
    requests with matching metadata. RequestRouter's default behavior is
    out-of-order routing and match exactly the internal request id of
    the request. This mixin doesn't provide any helper methods. By including it
    in your custom implementation of RequestRouter, it will override the
    reqeust matching algorithm to match based on the request metadata
    multiplexed model id, if available, and then fall back to the first pending
    request in the queue.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _get_pending_request_matching_metadata(
        self,
        request_metadata: Optional[RequestMetadata] = None,
    ) -> Optional[PendingRequest]:
        """Matching pending request based on the request metadata.

        If multiplex mixin is used, this will be using the multiplexed model
        id for the matching. Else, it will return none as no matching pending request.
        """
        if hasattr(self, "_get_pending_request_matching_multiplexed_model_id"):
            return self._get_pending_request_matching_multiplexed_model_id(
                request_metadata
            )

        return None

    def _fulfill_next_pending_request(
        self,
        replica: RunningReplica,
        request_metadata: Optional[RequestMetadata] = None,
    ):
        """Assign the replica to the next pending request in FIFO order.

        If a pending request has been cancelled, it will be popped from the queue
        and not assigned.
        """
        # First try to match a pending request based on the request metadata.
        matched_pending_request = self._get_pending_request_matching_metadata(
            request_metadata
        )
        if matched_pending_request is not None:
            self._record_queue_wait_time(matched_pending_request)
            matched_pending_request.future.set_result(replica)
            self._pending_requests_to_fulfill.remove(matched_pending_request)
            return

        # If no pending request matches the request metadata, fulfill the next in the
        # queue in FIFO order, passing over futures that have been cancelled.
        while len(self._pending_requests_to_fulfill) > 0:
            pr = self._pending_requests_to_fulfill.popleft()
            if not pr.future.done():
                self._record_queue_wait_time(pr)
                pr.future.set_result(replica)
                break


@PublicAPI(stability="alpha")
class RequestRouter(ABC):
    """Abstract interface for a request router (how the router calls it)."""

    """Backoff parameters for request router."""
    initial_backoff_s = RAY_SERVE_ROUTER_RETRY_INITIAL_BACKOFF_S
    backoff_multiplier = RAY_SERVE_ROUTER_RETRY_BACKOFF_MULTIPLIER
    max_backoff_s = RAY_SERVE_ROUTER_RETRY_MAX_BACKOFF_S

    # Deadline for replicas to respond with their queue length. If the response isn't
    # received within this deadline, the replica will not be considered.
    # If this deadline is repeatedly missed, it will be exponentially increased up to
    # the maximum configured here.
    queue_len_response_deadline_s = RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S
    """Deadline for receiving queue length info from replicas."""

    max_queue_len_response_deadline_s = RAY_SERVE_MAX_QUEUE_LENGTH_RESPONSE_DEADLINE_S
    """Maximum deadline for receiving queue length info from replicas."""

    max_num_routing_tasks_cap = 50
    """
    Hard limit on the maximum number of routing tasks to run. Having too many of
    these tasks can cause stability issue due to too much load on the local process
    and many too requests in flight to fetch replicas' queue lengths.
    """

    def __init__(
        self,
        deployment_id: DeploymentID,
        handle_source: DeploymentHandleSource,
        self_actor_id: Optional[str] = None,
        self_actor_handle: Optional[ActorHandle] = None,
        use_replica_queue_len_cache: bool = False,
        get_curr_time_s: Optional[Callable[[], float]] = None,
        create_replica_wrapper_func: Optional[
            Callable[[RunningReplicaInfo], RunningReplica]
        ] = None,
        *args,
        **kwargs,
    ):
        self._deployment_id = deployment_id
        self._handle_source = handle_source
        self._self_actor_handle = self_actor_handle
        self._use_replica_queue_len_cache = use_replica_queue_len_cache
        self._create_replica_wrapper_func = create_replica_wrapper_func

        # Current replicas available to be routed.
        # Updated via `update_replicas`.
        self._replica_id_set: Set[ReplicaID] = set()
        self._replicas: Dict[ReplicaID, RunningReplica] = {}
        self._replica_queue_len_cache = ReplicaQueueLengthCache(
            get_curr_time_s=get_curr_time_s,
        )

        # NOTE(edoakes): Python 3.10 removed the `loop` parameter to `asyncio.Event`.
        # Now, the `asyncio.Event` will call `get_running_loop` in its constructor to
        # determine the loop to attach to. This class can be constructed for the handle
        # from a different loop than it uses for routing, so we need to construct it
        # lazily to avoid an error due to the event being attached to the wrong loop.
        self._lazily_constructed_replicas_updated_event: Optional[asyncio.Event] = None
        self._lazily_fetched_loop: Optional[asyncio.AbstractEventLoop] = None

        # Tasks running the routing loop. The size of this set may vary over time
        # as new tasks will be routed when a request comes in or new replicas are
        # added, but it will not exceed self.max_num_routing_tasks.
        self._routing_tasks: Set[asyncio.Task] = set()

        # We keep two separate queues of pending requests:
        # - self._pending_requests_to_fulfill is a queue that will be used to fulfill
        # requests (potentially out of order) by routing tasks once they've acquired a replica.
        # - self.routing is a queue that is used for tasks to
        # best-effort grab the metadata of requests waiting to be fulfilled. This is
        # currently used for routing tasks to know which multiplexed model IDs they
        # should be trying to get replicas for.
        self._pending_requests_to_fulfill: Deque[PendingRequest] = deque()
        self._pending_requests_to_route: Deque[PendingRequest] = deque()

        # Prepare request router metrics.
        self.num_routing_tasks_gauge = metrics.Gauge(
            "serve_num_scheduling_tasks",
            description="The number of request routing tasks in the router.",
            tag_keys=("app", "deployment", "actor_id", "application", "handle_source"),
        ).set_default_tags(
            {
                # TODO(abrar): Remove "app" in future.
                "app": self._deployment_id.app_name,
                "application": self._deployment_id.app_name,
                "deployment": self._deployment_id.name,
                "actor_id": self_actor_id if self_actor_id else "",
                "handle_source": self._handle_source.value,
            }
        )
        self.num_routing_tasks_gauge.set(0)

        self.num_routing_tasks_in_backoff = 0
        self.num_routing_tasks_in_backoff_gauge = metrics.Gauge(
            "serve_num_scheduling_tasks_in_backoff",
            description=(
                "The number of request routing tasks in the router "
                "that are undergoing backoff."
            ),
            tag_keys=("app", "deployment", "actor_id", "application", "handle_source"),
        ).set_default_tags(
            {
                # TODO(abrar): Remove "app" in future.
                "app": self._deployment_id.app_name,
                "application": self._deployment_id.app_name,
                "deployment": self._deployment_id.name,
                "actor_id": self_actor_id if self_actor_id else "",
                "handle_source": self._handle_source.value,
            }
        )
        self.num_routing_tasks_in_backoff_gauge.set(self.num_routing_tasks_in_backoff)

        # Queue wait time histogram: time request spent waiting in queue
        # before being assigned to a replica.
        self.queue_wait_time_ms_histogram = metrics.Histogram(
            "serve_request_router_fulfillment_time_ms",
            description=(
                "Time in milliseconds that a request spent waiting in the "
                "queue before being assigned to a replica."
            ),
            boundaries=DEFAULT_LATENCY_BUCKET_MS,
            tag_keys=("deployment", "actor_id", "application", "handle_source"),
        ).set_default_tags(
            {
                "application": self._deployment_id.app_name,
                "deployment": self._deployment_id.name,
                "actor_id": self_actor_id if self_actor_id else "",
                "handle_source": self._handle_source.value,
            }
        )

        self.router_queue_len_gauge = metrics.Gauge(
            "serve_request_router_queue_len",
            description=(
                "The number of requests currently running on a replica "
                "as tracked by the router's queue length cache."
            ),
            tag_keys=(
                "deployment",
                "replica_id",
                "actor_id",
                "application",
                "handle_source",
            ),
        ).set_default_tags(
            {
                "application": self._deployment_id.app_name,
                "deployment": self._deployment_id.name,
                "actor_id": self_actor_id if self_actor_id else "",
                "handle_source": self._handle_source.value,
            }
        )

    def _update_router_queue_len_gauge(self, replica_id: ReplicaID, queue_len: int):
        """Update the router queue length gauge for a specific replica."""
        self.router_queue_len_gauge.set(
            queue_len,
            tags={"replica_id": replica_id.unique_id},
        )

    def initialize_state(self, **kwargs):
        """
        Initialize the state of the request router. Called by the Ray Serve framework with the
        contents of `RequestRouter.request_router_kwargs`.
        """
        pass

    @property
    def _event_loop(self) -> asyncio.AbstractEventLoop:
        if self._lazily_fetched_loop is None:
            self._lazily_fetched_loop = asyncio.get_running_loop()

        return self._lazily_fetched_loop

    @property
    def _replicas_updated_event(self) -> asyncio.Event:
        """Lazily construct `asyncio.Event`.

        See comment for self._lazily_constructed_replicas_updated_event.
        """
        if self._lazily_constructed_replicas_updated_event is None:
            self._lazily_constructed_replicas_updated_event = asyncio.Event()

        return self._lazily_constructed_replicas_updated_event

    @property
    def num_pending_requests(self) -> int:
        """Current number of requests pending assignment."""
        return len(self._pending_requests_to_fulfill)

    @property
    def curr_num_routing_tasks(self) -> int:
        """Current number of routing tasks running."""
        return len(self._routing_tasks)

    @property
    def max_num_routing_tasks(self) -> int:
        """Max number of routing tasks to run at any time."""
        return min(self.max_num_routing_tasks_cap, 2 * len(self._replicas))

    @property
    def target_num_routing_tasks(self) -> int:
        """Target number of routing tasks to be running based on pending requests.

        This will never exceed `self.max_num_routing_tasks`.
        """
        return min(self.num_pending_requests, self.max_num_routing_tasks)

    @property
    def curr_replicas(self) -> Dict[ReplicaID, RunningReplica]:
        """Current replicas available to be routed."""
        return self._replicas

    @property
    def app_name(self) -> str:
        """Name of the app this router is serving."""
        return self._deployment_id.app_name

    @property
    def replica_queue_len_cache(self) -> ReplicaQueueLengthCache:
        """Get the replica queue length cache."""
        return self._replica_queue_len_cache

    def create_replica_wrapper(
        self, replica_info: RunningReplicaInfo
    ) -> RunningReplica:
        return self._create_replica_wrapper_func(replica_info)

    def on_replica_actor_died(self, replica_id: ReplicaID):
        """Drop replica from replica set so it's not considered for future requests."""
        self._replicas.pop(replica_id, None)
        self._replica_id_set.discard(replica_id)
        if hasattr(self, "_discard_colocated_replica_ids_on_replica_actor_died"):
            self._discard_colocated_replica_ids_on_replica_actor_died(replica_id)

    def on_replica_actor_unavailable(self, replica_id: ReplicaID):
        """Invalidate cache entry so active probing is required for the next request."""
        self._replica_queue_len_cache.invalidate_key(replica_id)

    def on_new_queue_len_info(
        self, replica_id: ReplicaID, queue_len_info: ReplicaQueueLengthInfo
    ):
        """Update queue length cache with new info received from replica."""
        if self._use_replica_queue_len_cache:
            self._replica_queue_len_cache.update(
                replica_id, queue_len_info.num_ongoing_requests
            )
            self._update_router_queue_len_gauge(
                replica_id, queue_len_info.num_ongoing_requests
            )

    def on_send_request(self, replica_id: ReplicaID):
        """Increment queue length cache when a request is sent to a replica."""
        if self._use_replica_queue_len_cache:
            num_ongoing_requests = self._replica_queue_len_cache.get(replica_id) or 0
            new_queue_len = num_ongoing_requests + 1
            self._replica_queue_len_cache.update(replica_id, new_queue_len)
            self._update_router_queue_len_gauge(replica_id, new_queue_len)

    def update_replicas(self, replicas: List[RunningReplica]):
        """Update the set of available replicas to be considered for routing.

        When the set of replicas changes, we may spawn additional routing tasks
        if there are pending requests.
        """
        new_replicas = {}
        new_replica_id_set = set()
        if hasattr(self, "_update_colocated_replica_ids_with_replicas"):
            self._update_colocated_replica_ids_with_replicas(replicas)
        if hasattr(self, "_update_multiplexed_model_ids_with_replicas"):
            self._update_multiplexed_model_ids_with_replicas(replicas)

        for r in replicas:
            # If on the proxy, replica needs to call back into the proxy with
            # `receive_asgi_messages` which can be blocked when GCS is down.
            # To prevent that from happening, push proxy handle eagerly
            if (
                self._handle_source == DeploymentHandleSource.PROXY
                and r.replica_id not in self._replicas
            ):
                r.push_proxy_handle(self._self_actor_handle)

            new_replicas[r.replica_id] = r
            new_replica_id_set.add(r.replica_id)

        if self._replica_id_set != new_replica_id_set:
            replica_id_set_strs = {r.unique_id for r in new_replica_id_set}
            logger.info(
                f"Got updated replicas for {self._deployment_id}: "
                f"{replica_id_set_strs}.",
                extra={"log_to_stderr": False},
            )

        # Get list of new replicas
        new_ids = new_replica_id_set - self._replica_id_set
        replicas_to_ping = [new_replicas.get(id) for id in new_ids]

        self._replicas = new_replicas
        self._replica_id_set = new_replica_id_set
        self._replica_queue_len_cache.remove_inactive_replicas(
            active_replica_ids=new_replica_id_set
        )
        # Populate cache for new replicas
        self._event_loop.create_task(self._probe_queue_lens(replicas_to_ping, 0))
        self._replicas_updated_event.set()
        self._maybe_start_routing_tasks()

    async def _probe_queue_lens(
        self,
        replicas: List[RunningReplica],
        backoff_index: int,
    ) -> List[Tuple[RunningReplica, Optional[int]]]:
        """Actively probe the queue length from each of the replicas.

        Sends an RPC to each replica to fetch its queue length, with a response deadline
        that increases exponentially in backoff.

        Returns a list of queue lengths in the same order as the replicas passed in.
        Replicas whose RPCs fail or don't respond within the deadline will have a queue
        length of `None`. Replicas that return a `RayActorError` will be removed from
        future consideration for requests.

        This method also updates the local cache of replica queue lengths according to
        the responses.
        """
        result: List[Tuple[RunningReplica, Optional[int]]] = []
        if len(replicas) == 0:
            return result

        # Ensure the max deadline is always >= the initial deadline.
        max_queue_len_response_deadline_s = max(
            self.queue_len_response_deadline_s,
            self.max_queue_len_response_deadline_s,
        )

        try:
            queue_len_response_deadline_s = min(
                self.queue_len_response_deadline_s * (2**backoff_index),
                max_queue_len_response_deadline_s,
            )
        except OverflowError:
            # self.queue_len_response_deadline_s * (2**backoff_index)
            # can overflow if backoff_index gets sufficiently large (e.g.
            # 1024 when queue_len_response_deadline_s is 0.1).
            queue_len_response_deadline_s = max_queue_len_response_deadline_s

        get_queue_len_tasks = []
        for r in replicas:
            t = self._event_loop.create_task(
                r.get_queue_len(deadline_s=queue_len_response_deadline_s)
            )
            t.replica = r
            get_queue_len_tasks.append(t)

        done, pending = await asyncio.wait(
            get_queue_len_tasks,
            timeout=queue_len_response_deadline_s,
            return_when=asyncio.ALL_COMPLETED,
        )
        for t in pending:
            replica = t.replica
            result.append((replica, None))
            t.cancel()
            logger.warning(
                f"Failed to get queue length from {replica.replica_id} "
                f"within {queue_len_response_deadline_s}s. If this happens repeatedly "
                "it's likely caused by high network latency in the cluster. You can "
                "configure the deadline using the "
                "`RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S` environment variable."
            )

        for t in done:
            replica = t.replica
            if t.exception() is not None:
                result.append((replica, None))
                msg = (
                    "Failed to fetch queue length for "
                    f"{replica.replica_id}: '{t.exception()}'"
                )
                # If we get an ActorDiedError, the replica actor has died. This
                # is not recoverable (the controller will start a new replica in its
                # place), so we should no longer consider it for requests.
                # We do not catch RayActorError here because that error can be
                # raised even when a replica is temporarily unavailable.
                # See https://github.com/ray-project/ray/issues/44185 for details.
                if isinstance(t.exception(), ActorDiedError):
                    self.on_replica_actor_died(replica.replica_id)
                    msg += " This replica will no longer be considered for requests."
                # Replica is temporarily unavailable because of network issues, or
                # replica has died but GCS is down so ActorUnavailableError will
                # be raised until the GCS recovers. For the time being, invalidate
                # the cache entry so that we don't try to send requests to this
                # replica without actively probing.
                elif isinstance(t.exception(), ActorUnavailableError):
                    self.on_replica_actor_unavailable(replica.replica_id)
                    msg = (
                        "Failed to fetch queue length for "
                        f"{replica.replica_id}. Replica is temporarily "
                        "unavailable."
                    )

                logger.warning(msg)
            else:
                queue_len = t.result()
                result.append((replica, queue_len))
                self._replica_queue_len_cache.update(replica.replica_id, queue_len)
                self._update_router_queue_len_gauge(replica.replica_id, queue_len)

        assert len(result) == len(replicas)
        return result

    async def _select_from_candidate_replicas(
        self,
        candidates: List[RunningReplica],
        backoff_index: int,
    ) -> Optional[RunningReplica]:
        """Chooses the best replica from the list of candidates.

        If none of the replicas can be routed, returns `None`.

        The queue length for each replica is first looked up in the local cache. If not
        present in the cache, the replica will be actively probed and the cache updated.

        Among replicas that respond within the deadline and don't have full queues, the
        one with the lowest queue length is chosen.
        """
        lowest_queue_len = math.inf
        chosen_replica_id: Optional[str] = None
        not_in_cache: List[RunningReplica] = []
        if self._use_replica_queue_len_cache:
            # Populate available queue lens from the cache.
            for r in candidates:
                queue_len = self._replica_queue_len_cache.get(r.replica_id)
                # Include replicas whose queues are full as not in the cache so we will
                # actively probe them. Otherwise we may end up in "deadlock" until their
                # cache entries expire.
                if queue_len is None or queue_len >= r.max_ongoing_requests:
                    not_in_cache.append(r)
                elif queue_len < lowest_queue_len:
                    lowest_queue_len = queue_len
                    chosen_replica_id = r.replica_id
        else:
            not_in_cache = candidates

        # If there is a valid replica to route based on the information in the
        # cache, route it. Else fall back to actively probing.
        if chosen_replica_id is None:
            for r, queue_len in await self._probe_queue_lens(
                not_in_cache,
                backoff_index,
            ):
                if queue_len is None:
                    # None is returned if we failed to get the queue len.
                    continue

                if queue_len < r.max_ongoing_requests and queue_len < lowest_queue_len:
                    lowest_queue_len = queue_len
                    chosen_replica_id = r.replica_id
        elif len(not_in_cache) > 0:
            # If there are replicas without a valid cache entry, probe them in the
            # background to populate the cache.
            self._event_loop.create_task(
                self._probe_queue_lens(not_in_cache, backoff_index)
            )

        # `self._replicas` may have been updated since the candidates were chosen.
        # In that case, return `None` so a new one is selected.
        return self._replicas.get(chosen_replica_id, None)

    def _get_pending_request_matching_internal_request_id(
        self,
        request_metadata: Optional[RequestMetadata] = None,
    ) -> Optional[PendingRequest]:
        """Get the pending request that matches on the internal request id.

        If no request metadata is provided or no request is found that matches
        the internal request ID, return None.
        """
        if request_metadata is None:
            return None

        for pr in self._pending_requests_to_fulfill:
            if (
                not pr.future.done()
                and pr.metadata.internal_request_id
                == request_metadata.internal_request_id
            ):
                return pr

        return None

    def _record_queue_wait_time(self, pending_request: PendingRequest):
        """Records the time a request spent in the queue."""
        queue_wait_time_ms = (time.time() - pending_request.created_at) * 1000
        self.queue_wait_time_ms_histogram.observe(queue_wait_time_ms)

    def _fulfill_next_pending_request(
        self,
        replica: RunningReplica,
        request_metadata: Optional[RequestMetadata] = None,
    ):
        """Assign the replica to the next pending request, potentially not in
        order of when the request arrived.

        If a pending request has been cancelled, it will be popped from the queue
        and not assigned.
        """
        # Find the pending request that matches exactly.
        matched_pending_request = (
            self._get_pending_request_matching_internal_request_id(request_metadata)
        )
        if matched_pending_request is not None:
            self._record_queue_wait_time(matched_pending_request)
            matched_pending_request.future.set_result(replica)
            self._pending_requests_to_fulfill.remove(matched_pending_request)
            return

    def _get_next_pending_request_to_route(
        self,
    ) -> Optional[PendingRequest]:
        while len(self._pending_requests_to_route) > 0:
            pr = self._pending_requests_to_route.popleft()
            if not pr.future.done():
                return pr

        return None

    async def _choose_replicas_with_backoff(
        self,
        pending_request: Optional[PendingRequest] = None,
    ) -> AsyncGenerator[List[RunningReplica], None]:
        """Generator that repeatedly chooses available replicas.
        In the first iteration, only replicas colocated on the same node as this router
        will be considered. If those are occupied, the full set of replicas will be
        considered on subsequent iterations.
        After each iteration, there will be an increasing backoff sleep time (dictated
        by `initial_backoff_s` and `backoff_multiplier`). The caller should exit the
        generator to reset the backoff sleep time.
        """
        entered_backoff = False
        try:
            attempt = 0

            while True:
                # If no replicas are available, wait until `update_replicas` is called.
                while len(self._replicas) == 0:
                    logger.info(
                        "No replicas are currently available for "
                        f"{self._deployment_id}.",
                        extra={"log_to_stderr": False},
                    )
                    self._replicas_updated_event.clear()
                    await self._replicas_updated_event.wait()
                    logger.info(
                        f"New replicas are available for {self._deployment_id}, "
                        "attempting to route queued requests.",
                        extra={"log_to_stderr": False},
                    )

                replica_ranks = list(self._replicas.values())
                chosen_replicas: List[
                    List[RunningReplica]
                ] = await self.choose_replicas(
                    candidate_replicas=replica_ranks,
                    pending_request=pending_request,
                )
                for replicas in chosen_replicas:
                    if replicas:
                        yield replicas

                # We have a slight unintended behavior when enabled locality routing
                # for both node and AZ. The intention is to try same node first,
                # then try same AZ if node fails, then try everything else until a
                # replica is found. These sequence should only help to reduce the
                # latency of the request. No backoff and sleep should be applied, until
                # we have fall into the case trying on all available replicas.
                if (
                    pending_request
                    and not pending_request.routing_context.should_backoff
                ):
                    continue

                if not entered_backoff:
                    entered_backoff = True
                    self.num_routing_tasks_in_backoff += 1
                    self.num_routing_tasks_in_backoff_gauge.set(
                        self.num_routing_tasks_in_backoff
                    )
                else:
                    # Only backoff after the first retry.
                    backoff_s = min(
                        self.initial_backoff_s * self.backoff_multiplier**attempt,
                        self.max_backoff_s,
                    )
                    await asyncio.sleep(backoff_s)
                    attempt += 1
        finally:
            if entered_backoff:
                self.num_routing_tasks_in_backoff -= 1
                self.num_routing_tasks_in_backoff_gauge.set(
                    self.num_routing_tasks_in_backoff
                )

    async def _fulfill_pending_requests(self):
        """Repeatedly tries to fulfill a pending request with an available replica.

        This is expected to be run inside a task in self._routing_tasks.

        When a replica is found, this method will exit if the number of routing tasks
        has exceeded the target number. Else it will loop again to route another
        replica.
        """
        try:
            while len(self._routing_tasks) <= self.target_num_routing_tasks:
                start_time = time.time()
                backoff_index = 0
                pending_request = self._get_next_pending_request_to_route()
                request_metadata = pending_request.metadata if pending_request else None
                gen_choose_replicas_with_backoff = self._choose_replicas_with_backoff(
                    pending_request
                )
                try:
                    async for candidates in gen_choose_replicas_with_backoff:
                        # Clear out pending requests at the front of the
                        # queue that have been cancelled, then reevaluate
                        # if we need to continue this routing task.
                        while (
                            len(self._pending_requests_to_fulfill) > 0
                            and self._pending_requests_to_fulfill[0].future.done()
                        ):
                            self._pending_requests_to_fulfill.popleft()

                        if len(self._routing_tasks) > self.target_num_routing_tasks:
                            break

                        replica = await self._select_from_candidate_replicas(
                            candidates, backoff_index
                        )
                        if replica is not None:
                            self._fulfill_next_pending_request(
                                replica, request_metadata
                            )
                            break

                        backoff_index += 1
                        if backoff_index >= 50 and backoff_index % 50 == 0:
                            routing_time_elapsed = time.time() - start_time
                            warning_log = (
                                "Failed to route request after "
                                f"{backoff_index} attempts over "
                                f"{routing_time_elapsed:.2f}s. Retrying."
                            )
                            if request_metadata is not None:
                                warning_log += (
                                    f" Request ID: {request_metadata.request_id}."
                                )
                                if request_metadata.multiplexed_model_id:
                                    warning_log += (
                                        " Multiplexed model ID: "
                                        f"{request_metadata.multiplexed_model_id}."
                                    )
                            logger.warning(warning_log)
                finally:
                    await gen_choose_replicas_with_backoff.aclose()

        except Exception:
            logger.exception("Unexpected error in _fulfill_pending_requests.")
        finally:
            self._routing_tasks.remove(asyncio.current_task(loop=self._event_loop))
            self.num_routing_tasks_gauge.set(self.curr_num_routing_tasks)

    def _maybe_start_routing_tasks(self):
        """Start routing tasks to fulfill pending requests if necessary.

        Starts tasks so that there is at least one task per pending request
        (respecting the max number of routing tasks).

        In the common case, this will start a single task when a new request comes
        in for routing. However, in cases where the number of available replicas
        is updated or a task exits unexpectedly, we may need to start multiple.
        """
        tasks_to_start = self.target_num_routing_tasks - self.curr_num_routing_tasks
        for _ in range(tasks_to_start):
            self._routing_tasks.add(
                self._event_loop.create_task(self._fulfill_pending_requests())
            )
        if tasks_to_start > 0:
            self.num_routing_tasks_gauge.set(self.curr_num_routing_tasks)

    async def _choose_replica_for_request(
        self, pending_request: PendingRequest, *, is_retry: bool = False
    ) -> RunningReplica:
        """Chooses a replica to send the provided request to.

        Upon cancellation (by the caller), the future is cancelled and will be passed
        over when a replica becomes available.
        """
        try:
            if not is_retry:
                self._pending_requests_to_fulfill.append(pending_request)
                self._pending_requests_to_route.append(pending_request)
            else:
                pending_request.reset_future()
                index = 0
                for pr in self._pending_requests_to_fulfill:
                    if pending_request.created_at < pr.created_at:
                        break

                    index += 1

                self._pending_requests_to_fulfill.insert(index, pending_request)

                index = 0
                for pr in self._pending_requests_to_route:
                    if pending_request.created_at < pr.created_at:
                        break

                    index += 1

                self._pending_requests_to_route.insert(index, pending_request)

            self._maybe_start_routing_tasks()
            replica = await pending_request.future
        except asyncio.CancelledError as e:
            pending_request.future.cancel()

            raise e from None

        return replica

    def _update_running_replicas(self, running_replicas: List[RunningReplicaInfo]):
        """Compatibility shim for RunningReplicaInfo datatype."""
        replica_wrappers = []
        for r in running_replicas:
            try:
                replica_wrappers.append(self.create_replica_wrapper(r))
            except ValueError:
                # NOTE(abrar): ValueError is raised when the actor handle is not found
                # by ray.get_actor.

                # Actor has died (e.g., due to node failure) but controller hasn't
                # detected it yet. Skip this replica; controller will send an update
                # when it detects the failure.
                logger.warning(
                    f"Failed to get handle to replica {r.replica_id} during router "
                    "update. The replica actor may have died. Skipping this replica."
                )
        return self.update_replicas(replica_wrappers)

    def select_available_replicas(
        self, candidates: Optional[List[RunningReplica]] = None
    ) -> List[RunningReplica]:
        """Select available replicas from the list of candidates.

        This method is used to select replicas that are available to take more
        requests based on the queue length cache. If the queue length is not
        available in the cache, the replica is considered available. It does
        not actively probe the replicas for their queue length.

        If input candidates is `None`, all replicas are considered.
        """
        if candidates is None:
            candidates = list(self._replicas.values())

        available_replicas = []
        for r in candidates:
            queue_len = self._replica_queue_len_cache.get(r.replica_id)
            if queue_len is None or queue_len < r.max_ongoing_requests:
                available_replicas.append(r)

        return available_replicas

    @abstractmethod
    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        """Chooses a subset of candidate replicas from available replicas.

        This is the main function each request router should implement to
        decide which replica to send the request to. This is one iteration of
        replica selection.

        Args:
            candidate_replicas: A list of candidate replicas to be considered in the
                policy.
            pending_request: The request to be routed. This is used to
                determine which replicas are eligible for routing.

        Returns:
            A list of lists of replicas, where each inner list represents a
            rank of replicas. The first rank is the most preferred and the last
            rank is the least preferred.
        """
        pass

    def on_request_routed(
        self,
        pending_request: PendingRequest,
        replica_id: ReplicaID,
        result: ReplicaResult,
    ):
        """Called when a request is routed to a replica.

        This is used as a callback to update the state of the request router
        after a response is generated.
        """
        pass
