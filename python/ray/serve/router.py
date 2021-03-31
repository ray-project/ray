import asyncio
import itertools
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

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

    def __post_init__(self):
        self.http_headers.setdefault("X-Serve-Call-Method", self.call_method)
        self.http_headers.setdefault("X-Serve-Shard-Key", self.shard_key)


@dataclass
class Query:
    args: List[Any]
    kwargs: Dict[Any, Any]
    metadata: RequestMetadata

    # Fields used by backend worker to perform timing measurement.
    tick_enter_replica: Optional[float] = None


class ReplicaSet:
    """Data structure representing a set of replica actor handles"""

    def __init__(
            self,
            controller_handle,
            backend_tag,
            event_loop: asyncio.AbstractEventLoop,
    ):
        self.backend_tag = backend_tag
        # NOTE(simon): We have to do this because max_concurrent_queries
        # and the replica handles come from different long poll keys.
        self.max_concurrent_queries: int = 8
        self.in_flight_queries: Dict[ActorHandle, set] = dict()
        # The iterator used for load balancing among replicas. Using itertools
        # cycle, we implements a round-robin policy, skipping overloaded
        # replicas.
        # NOTE(simon): We can make this more pluggable and consider different
        # policies like: min load, pick min of two replicas, pick replicas on
        # the same node.
        self.replica_iterator = itertools.cycle(self.in_flight_queries.keys())

        # Used to unblock this replica set waiting for free replicas. A newly
        # added replica or updated max_concurrent_queries value means the
        # query that waits on a free replica might be unblocked on.
        self.config_updated_event = asyncio.Event(loop=event_loop)
        self.num_queued_queries = 0
        self.num_queued_queries_gauge = metrics.Gauge(
            "serve_backend_queued_queries",
            description=(
                "The current number of queries to this backend waiting"
                " to be assigned to a replica."),
            tag_keys=("backend", "endpoint"))
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
            call_in_event_loop=event_loop,
        )

    def set_max_concurrent_queries(self, backend_config: BackendConfig):
        new_value: int = backend_config.max_concurrent_queries
        if new_value != self.max_concurrent_queries:
            self.max_concurrent_queries = new_value
            logger.debug(
                f"ReplicaSet: changing max_concurrent_queries to {new_value}")
            self.config_updated_event.set()

    def update_worker_replicas(self, worker_replicas: Iterable[ActorHandle]):
        added, removed, _ = compute_iterable_delta(
            self.in_flight_queries.keys(), worker_replicas)

        for new_replica_handle in added:
            self.in_flight_queries[new_replica_handle] = set()

        for removed_replica_handle in removed:
            # Delete it directly because shutdown is processed by controller.
            del self.in_flight_queries[removed_replica_handle]

        if len(added) > 0 or len(removed) > 0:
            self.replica_iterator = itertools.cycle(
                self.in_flight_queries.keys())
            self.config_updated_event.set()

    def _try_assign_replica(self, query: Query) -> Optional[ray.ObjectRef]:
        """Try to assign query to a replica, return the object ref if succeeded
        or return None if it can't assign this query to any replicas.
        """
        for _ in range(len(self.in_flight_queries.keys())):
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
            self.in_flight_queries[replica].add(tracker_ref)
            return user_ref
        return None

    @property
    def _all_query_refs(self):
        return list(
            itertools.chain.from_iterable(self.in_flight_queries.values()))

    def _drain_completed_object_refs(self) -> int:
        refs = self._all_query_refs
        done, _ = ray.wait(refs, num_returns=len(refs), timeout=0)
        for replica_in_flight_queries in self.in_flight_queries.values():
            replica_in_flight_queries.difference_update(done)
        return len(done)

    async def assign_replica(self, query: Query) -> ray.ObjectRef:
        """Given a query, submit it to a replica and return the object ref.
        This method will keep track of the in flight queries for each replicas
        and only send a query to available replicas (determined by the backend
        max_concurrent_quries value.)
        """
        endpoint = query.metadata.endpoint
        self.num_queued_queries += 1
        self.num_queued_queries_gauge.set(
            self.num_queued_queries, tags={"endpoint": endpoint})
        assigned_ref = self._try_assign_replica(query)
        while assigned_ref is None:  # Can't assign a replica right now.
            logger.debug("Failed to assign a replica for "
                         f"query {query.metadata.request_id}")
            # Maybe there exists a free replica, we just need to refresh our
            # query tracker.
            num_finished = self._drain_completed_object_refs()
            # All replicas are really busy, wait for a query to complete or the
            # config to be updated.
            if num_finished == 0:
                logger.debug(
                    "All replicas are busy, waiting for a free replica.")
                await asyncio.wait(
                    self._all_query_refs + [self.config_updated_event.wait()],
                    return_when=asyncio.FIRST_COMPLETED)
                if self.config_updated_event.is_set():
                    self.config_updated_event.clear()
            # We are pretty sure a free replica is ready now, let's recurse and
            # assign this query a replica.
            assigned_ref = self._try_assign_replica(query)
        self.num_queued_queries -= 1
        self.num_queued_queries_gauge.set(
            self.num_queued_queries, tags={"endpoint": endpoint})
        return assigned_ref


class Router:
    def __init__(
            self,
            controller_handle: ActorHandle,
            endpoint_tag: EndpointTag,
            loop: asyncio.BaseEventLoop = None,
    ):
        """Router process incoming queries: choose backend, and assign replica.

        Args:
            controller_handle(ActorHandle): The controller handle.
        """
        self.controller = controller_handle
        self.endpoint_tag = endpoint_tag
        self.endpoint_policy: Optional[EndpointPolicy] = None
        self.backend_replicas: Dict[BackendTag, ReplicaSet] = dict()
        self._pending_endpoint_registered = asyncio.Event(loop=loop)
        self._loop = loop or asyncio.get_event_loop()

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
            call_in_event_loop=self._loop,
        )

    def _update_traffic_policy(self, traffic_policy: TrafficPolicy):
        self.endpoint_policy = RandomEndpointPolicy(traffic_policy)

        backend_tags = traffic_policy.backend_tags
        added, removed, _ = compute_iterable_delta(
            self.backend_replicas.keys(),
            backend_tags,
        )
        for tag in added:
            self._get_or_create_replica_set(tag)
        for tag in removed:
            del self.backend_replicas[tag]

        if not self._pending_endpoint_registered.is_set():
            self._pending_endpoint_registered.set()

    def _get_or_create_replica_set(self, tag):
        if tag not in self.backend_replicas:
            self.backend_replicas[tag] = ReplicaSet(self.controller, tag,
                                                    self._loop)
        return self.backend_replicas[tag]

    async def assign_request(
            self,
            request_meta: RequestMetadata,
            *request_args,
            **request_kwargs,
    ):
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
            try:
                await asyncio.wait_for(
                    self._pending_endpoint_registered.wait(),
                    timeout=5,
                )
            except asyncio.TimeoutError:
                raise RayServeException(
                    f"Endpoint {endpoint} doesn't exist after 5s timeout. "
                    "Marking the query failed.")

        chosen_backend, *shadow_backends = self.endpoint_policy.assign(query)

        result_ref = await self._get_or_create_replica_set(
            chosen_backend).assign_replica(query)
        for backend in shadow_backends:
            (await self._get_or_create_replica_set(backend)
             .assign_replica(query))

        self.num_router_requests.inc(tags={"endpoint": endpoint})

        return result_ref
