import concurrent.futures
import itertools
import functools
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple
import threading

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

    def __post_init__(self):
        self.http_headers.setdefault("X-Serve-Call-Method", self.call_method)
        self.http_headers.setdefault("X-Serve-Shard-Key", self.shard_key)


@dataclass
class Query:
    args: List[Any]
    kwargs: Dict[Any, Any]
    metadata: RequestMetadata


class ReplicaSet:
    """Data structure representing a set of replica actor handles"""

    def __init__(
            self,
            controller_handle,
            backend_tag,
    ):
        self.backend_tag = backend_tag

        # Lock used to serialize the callback updates and assign_replica.
        self.lock = threading.RLock()
        # NOTE(simon): We have to do this because max_concurrent_queries
        # and the replica handles come from different long poll keys.
        self.max_concurrent_queries: int = 8
        # Track the on-going object refs assigned to replicas.
        self.in_flight_queries: Dict[ActorHandle, set] = dict()
        # The iterator used for load balancing among replicas. Using itertools
        # cycle, we implements a round-robin policy, skipping overloaded
        # replicas. We can make this more pluggable and consider different
        # policies like: min load, pick min of two replicas, pick replicas on
        # the same node.
        self.replica_iterator = itertools.cycle(self.in_flight_queries.keys())

        self.pending_queue: List[Tuple[Query, concurrent.futures.Future]] = []

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
            controller_handle, {
                (LongPollNamespace.BACKEND_CONFIGS, backend_tag): self.
                set_max_concurrent_queries,
                (LongPollNamespace.REPLICA_HANDLES, backend_tag): self.
                update_worker_replicas,
            })

    def set_max_concurrent_queries(self, backend_config: BackendConfig):
        new_value: int = backend_config.max_concurrent_queries
        if new_value != self.max_concurrent_queries:
            self.max_concurrent_queries = new_value
            logger.debug(
                f"ReplicaSet: changing max_concurrent_queries to {new_value}")
        self._drain_pending_queue()

    def update_worker_replicas(self, worker_replicas: Iterable[ActorHandle]):
        with self.lock:
            added, removed, _ = compute_iterable_delta(
                self.in_flight_queries.keys(), worker_replicas)

            for new_replica_handle in added:
                self.in_flight_queries[new_replica_handle] = set()

            for removed_replica_handle in removed:
                # Delete it directly because shutdown is processed
                # by controller.
                del self.in_flight_queries[removed_replica_handle]

            if len(added) > 0 or len(removed) > 0:
                self.replica_iterator = itertools.cycle(
                    self.in_flight_queries.keys())
                logger.debug(
                    f"ReplicaSet: +{len(added)}, -{len(removed)} replicas.")
                self._drain_pending_queue()

    def _on_query_tracker_completed(
            self,
            _trakcer_result: Any,
            *,
            replica_handle: ActorHandle,
            tracker_ref: ray.ObjectRef,
    ):
        """Callback after each query is completed."""
        with self.lock:
            if replica_handle in self.in_flight_queries:
                self.in_flight_queries[replica_handle].remove(tracker_ref)
                self._drain_pending_queue()

    def _drain_pending_queue(self):
        with self.lock:
            num_pending = len(self.pending_queue)
            if num_pending == 0:
                # No pending queries.
                return

            num_possible = (
                self.max_concurrent_queries * len(self.in_flight_queries) -
                sum(map(len, self.in_flight_queries.values())))
            if num_possible <= 0:
                # All replicas busy.
                return

            if num_pending > num_possible:
                # More pending queries than possible, clip the value.
                num_to_send = num_pending - num_possible
            else:
                # We can send all the queries.
                num_to_send = num_pending

            logger.debug(f"Draining {num_to_send} queries")
            for _ in range(num_to_send):
                self._assign_replica(*self.pending_queue.pop(0))

            self.num_queued_queries_gauge.set(len(self.pending_queue))

    def _assign_replica(
            self,
            query: Query,
            future: concurrent.futures.Future,
    ):
        """Assign query to a replica, put the resulting ObjectRef into future.
        """
        with self.lock:
            num_replicas = len(self.in_flight_queries.keys())
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
                    query.metadata, *query.args, **query.kwargs)
                tracker_ref._on_completed(
                    functools.partial(
                        self._on_query_tracker_completed,
                        replica_handle=replica,
                        tracker_ref=tracker_ref,
                    ))
                self.in_flight_queries[replica].add(tracker_ref)
                future.set_result(user_ref)
                return
            raise RuntimeError(
                "Postcondition failed. The replica set failed to assign "
                f"query {query} to a replica.")

    def assign_replica(self, query: Query) -> concurrent.futures.Future:
        """Enqueue a query for submission. The result is a Future that will be
        fulfilled with the eventual ObjectRef that represents the result.
        """
        future = concurrent.futures.Future()
        self.pending_queue.append((query, future))
        self._drain_pending_queue()
        return future


class EndpointRouter:
    def __init__(self, controller_handle: ActorHandle,
                 endpoint_tag: EndpointTag):
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

    def assign_request(
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

        future_ref = self.backend_replicas[chosen_backend].assign_replica(
            query)
        for backend in shadow_backends:
            self.backend_replicas[backend].assign_replica(query)

        self.num_router_requests.inc(tags={"endpoint": endpoint})

        return future_ref
