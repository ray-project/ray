from collections import deque
import concurrent.futures
import pickle
import itertools
import threading
import functools
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Iterable, List, Optional, Set
import random

from ray.actor import ActorHandle
from ray.serve.common import BackendTag, EndpointTag, TrafficPolicy
from ray.serve.config import BackendConfig
from ray.serve.endpoint_policy import EndpointPolicy, RandomEndpointPolicy
from ray.serve.long_poll import LongPollClient, LongPollNamespace
from ray.serve.utils import compute_iterable_delta, logger
from ray.serve.exceptions import RayServeException

import ray
from ray.util import metrics

REPORT_QUEUE_LENGTH_PERIOD_S = 1.0


@dataclass
class RequestMetadata:
    request_id: str
    endpoint: str

    call_method: str = "__call__"
    shard_key: Optional[str] = None

    http_method: str = "GET"
    http_headers: Dict[str, str] = field(default_factory=dict)

    is_shadow_query: bool = False

    # Determines whether or not the backend implementation will be presented
    # with a ServeRequest object or directly passed args and kwargs. This is
    # used to maintain backward compatibility and will be removed in the
    # future.
    use_serve_request: bool = True

    # This flag will be set to true if the input argument is manually pickled
    # and it needs to be deserialized by the backend worker.
    http_arg_is_pickled: bool = False

    def __post_init__(self):
        self.http_headers.setdefault("X-Serve-Call-Method", self.call_method)
        self.http_headers.setdefault("X-Serve-Shard-Key", self.shard_key)


@dataclass
class Query:
    args: List[Any]
    kwargs: Dict[Any, Any]
    metadata: RequestMetadata


@dataclass
class PendingQuery:
    query: Query
    ref_future: concurrent.futures.Future = field(
        default_factory=concurrent.futures.Future)


class ReplicaSet:
    """Data structure representing a set of replica actor handles"""

    def __init__(
            self,
            controller_handle,
            backend_tag,
    ):
        self.backend_tag = backend_tag
        # NOTE(simon): We have to do this because max_concurrent_queries
        # and the replica handles come from different long poll keys.
        self.max_concurrent_queries: int = 8
        # Track the in flight object refs assigned to replicas.
        self.in_flight_queries: Dict[ActorHandle, set] = dict()
        # The iterator used for load balancing among replicas. Using itertools
        # cycle, we implements a round-robin policy, skipping overloaded
        # replicas.
        # NOTE(simon): We can make this more pluggable and consider different
        # policies like: min load, pick min of two replicas, pick replicas on
        # the same node.
        self.replica_iterator = itertools.cycle(self.in_flight_queries.keys())

        self.pending_requests: Deque[PendingQuery] = deque()
        self.num_queued_queries = 0
        self.num_queued_queries_gauge = metrics.Gauge(
            "serve_backend_queued_queries",
            description=(
                "The current number of queries to this backend waiting"
                " to be assigned to a replica."),
            tag_keys=("backend", ))
        self.num_queued_queries_gauge.set_default_tags({
            "backend": self.backend_tag
        })

        self.long_poll_client = LongPollClient(
            controller_handle,
            {
                (LongPollNamespace.BACKEND_CONFIGS, backend_tag): self.
                set_max_concurrent_queries,
                (LongPollNamespace.REPLICA_HANDLES, backend_tag): self.
                update_worker_replicas,
            },
        )

    def set_max_concurrent_queries(self, backend_config: BackendConfig):
        new_value: int = backend_config.max_concurrent_queries
        if new_value != self.max_concurrent_queries:
            self.max_concurrent_queries = new_value
            logger.debug(
                f"ReplicaSet: changing max_concurrent_queries to {new_value}")
            self._try_send_pending_queries()

    def update_worker_replicas(self, worker_replicas: Iterable[ActorHandle]):
        added, removed, _ = compute_iterable_delta(
            self.in_flight_queries.keys(), worker_replicas)

        for new_replica_handle in added:
            self.in_flight_queries[new_replica_handle] = set()

        for removed_replica_handle in removed:
            # Delete it directly because shutdown is processed by controller.
            del self.in_flight_queries[removed_replica_handle]

        if len(added) > 0 or len(removed) > 0:
            # Shuffle the keys to avoid synchronization across clients.
            handles = list(self.in_flight_queries.keys())
            random.shuffle(handles)
            self.replica_iterator = itertools.cycle(handles)
            logger.debug(
                f"ReplicaSet: +{len(added)}, -{len(removed)} replicas.")
            self._try_send_pending_queries()

    def _try_send_pending_queries(self):
        while len(self.pending_requests) > 0:
            next_query = self.pending_requests.popleft()
            was_assigned = self._try_assign_replica(next_query)
            if not was_assigned:
                # All the replicas are busy. There is no need to send more
                # pending queries. We just put this query back and exit this
                # routine.
                self.pending_requests.appendleft(next_query)
                break
        self.num_queued_queries_gauge.set(len(self.pending_requests))

    def _try_assign_replica(self, pending_query: PendingQuery) -> bool:
        query = pending_query.query

        num_replicas = len(self.in_flight_queries)
        for _ in range(num_replicas):
            replica = next(self.replica_iterator)
            if len(self.in_flight_queries[replica]
                   ) >= self.max_concurrent_queries:
                # This replica is overloaded, try next one
                continue

            logger.debug(f"Assigned query {query.metadata.request_id} "
                         f"to replica {replica}.")
            # Directly passing args because it might contain an ObjectRef.
            tracker_ref, user_ref = replica.handle_request.remote(
                pickle.dumps(query.metadata), *query.args, **query.kwargs)
            # tracker_ref is used to implement back pressure so we don't send
            # more queries to overloaded replicas.
            self.in_flight_queries[replica].add(tracker_ref)
            tracker_ref.future().add_done_callback(
                functools.partial(
                    self._on_query_tracker_completed,
                    replica_handle=replica,
                    tracker_ref=tracker_ref))
            # user_ref is written to the concurrent future that the caller of
            # router gets immediately when enqueued a query.
            pending_query.ref_future.set_result(user_ref)

            return True
        return False

    def _on_query_tracker_completed(self, _tracker_result, *,
                                    replica_handle: ActorHandle,
                                    tracker_ref: ray.ObjectRef):
        tracker_refs: Set[ray.ObjectRef] = self.in_flight_queries.get(
            replica_handle, {})
        tracker_refs.discard(tracker_ref)
        if len(self.pending_requests):
            self._try_send_pending_queries()

    def assign_replica(self, query: Query) -> concurrent.futures.Future:
        pending_query = PendingQuery(query)
        self.pending_requests.append(pending_query)
        self._try_send_pending_queries()
        return pending_query.ref_future


class EndpointRouter:
    def __init__(
            self,
            controller_handle: ActorHandle,
            endpoint_tag: EndpointTag,
    ):
        """Router process incoming queries: choose backend, and assign replica.

        Args:
            controller_handle(ActorHandle): The controller handle.
        """
        self.controller = controller_handle
        self.endpoint_tag = endpoint_tag
        self.endpoint_policy: Optional[EndpointPolicy] = None
        self.backend_replicas: Dict[BackendTag, ReplicaSet] = dict()
        self._pending_endpoint_registered = threading.Event()

        # -- Metrics Registration -- #
        self.num_router_requests = metrics.Counter(
            "serve_num_router_requests",
            description="The number of requests processed by the router.",
            tag_keys=("endpoint", ))

        self.long_poll_client = LongPollClient(
            self.controller,
            {
                (LongPollNamespace.TRAFFIC_POLICIES, endpoint_tag): self.
                _update_traffic_policy,
            },
        )

    def __reduce__(self):
        # self._pending_endpoint_registered is not serializable
        # (threading.Event)
        return EndpointRouter, (self.controller, self.endpoint_tag)

    def _update_traffic_policy(self, traffic_policy: TrafficPolicy):
        self.endpoint_policy = RandomEndpointPolicy(traffic_policy)

        backend_tags = traffic_policy.backend_tags
        added, removed, _ = compute_iterable_delta(
            self.backend_replicas.keys(),
            backend_tags,
        )
        for tag in added:
            self.backend_replicas[tag] = ReplicaSet(self.controller, tag)
        for tag in removed:
            del self.backend_replicas[tag]

        if not self._pending_endpoint_registered.is_set():
            self._pending_endpoint_registered.set()

    def enqueue_request(
            self,
            request_meta: RequestMetadata,
            *request_args,
            **request_kwargs,
    ) -> concurrent.futures.Future:
        """Assign a query and returns an object ref represent the result"""
        endpoint = request_meta.endpoint
        query = Query(
            args=list(request_args),
            kwargs=request_kwargs,
            metadata=request_meta,
        )

        if not self._pending_endpoint_registered.is_set():
            # This can happen when the router is created but the endpoint
            # information hasn't been retrieved via long-poll yet.
            # Block the whole world wait, this should be very quick.
            is_set = self._pending_endpoint_registered.wait(timeout=5)
            if not is_set:
                raise RayServeException(
                    f"Endpoint {endpoint} doesn't exist after 5s timeout. "
                    "Marking the query failed.")

        chosen_backend, *shadow_backends = self.endpoint_policy.assign(query)

        ref_future = self.backend_replicas[chosen_backend].assign_replica(
            query)
        for backend in shadow_backends:
            self.backend_replicas[backend].assign_replica(query)

        self.num_router_requests.inc(tags={"endpoint": endpoint})

        return ref_future
