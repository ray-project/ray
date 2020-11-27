import asyncio
import itertools
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, DefaultDict, Dict, Iterable, List, Optional

import ray
from ray.actor import ActorHandle
from ray.serve.context import TaskContext
from ray.serve.endpoint_policy import EndpointPolicy, RandomEndpointPolicy
from ray.serve.long_poll import LongPollerAsyncClient
from ray.serve.utils import logger
from ray.util import metrics

REPORT_QUEUE_LENGTH_PERIOD_S = 1.0


@dataclass
class RequestMetadata:
    request_id: str
    endpoint: str
    request_context: TaskContext

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
    context: TaskContext
    metadata: RequestMetadata

    # Fields used by backend worker to perform timing measurement.
    tick_enter_replica: Optional[float] = None


class ReplicaSet:
    """Data structure representing a set of replica actor handles"""

    def __init__(self):
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
        # added replica or updated max_concurrenty_queries value means the
        # query that waits on a free replica might be unblocked on.
        self.config_updated_event = asyncio.Event()

    def set_max_concurrent_queries(self, new_value):
        if new_value != self.max_concurrent_queries:
            self.max_concurrent_queries = new_value
            logger.debug(
                f"ReplicaSet: chaging max_concurrent_queries to {new_value}")
            self.config_updated_event.set()

    def update_worker_replicas(self, worker_replicas: Iterable[ActorHandle]):
        current_replica_set = set(self.in_flight_queries.keys())
        updated_replica_set = set(worker_replicas)

        added = updated_replica_set - current_replica_set
        for new_replica_handle in added:
            self.in_flight_queries[new_replica_handle] = set()

        removed = current_replica_set - updated_replica_set
        for removed_replica_handle in removed:
            # NOTE(simon): Do we warn if there are still inflight queries?
            # The current approach is no because the queries objectrefs are
            # just used to perform backpressure. Caller should decide what to
            # do with the object refs.
            del self.in_flight_queries[removed_replica_handle]

        # State changed, reset the round robin iterator
        if len(added) > 0 or len(removed) > 0:
            self.replica_iterator = itertools.cycle(
                self.in_flight_queries.keys())
            self.config_updated_event.set()

    def _try_assign_replica(self, query: Query) -> Optional[ray.ObjectRef]:
        """Try to assign query to a replica, return the object ref is succeeded
        or return None if it can't assign this query to any replicas.
        """
        for _ in range(len(self.in_flight_queries.keys())):
            replica = next(self.replica_iterator)
            if len(self.in_flight_queries[replica]
                   ) >= self.max_concurrent_queries:
                # This replica is overloaded, try next one
                continue
            logger.debug(f"Replica set assigned {query} to {replica}")
            ref = replica.handle_request.remote(query)
            self.in_flight_queries[replica].add(ref)
            return ref
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
        assigned_ref = self._try_assign_replica(query)
        while assigned_ref is None:  # Can't assign a replica right now.
            logger.debug(f"Failed to assign a replica for query {query}")
            # Maybe there exists a free replica, we just need to refresh our
            # query tracker.
            num_finished = self._drain_completed_object_refs()
            # All replicas are really busy, wait for a query to complete or the
            # config to be updated.
            if num_finished == 0:
                logger.debug(
                    f"All replicas are busy, waiting for a free replica.")
                await asyncio.wait(
                    self._all_query_refs + [self.config_updated_event.wait()],
                    return_when=asyncio.FIRST_COMPLETED)
                if self.config_updated_event.is_set():
                    self.config_updated_event.clear()
            # We are pretty sure a free replica is ready now, let's recurse and
            # assign this query a replica.
            assigned_ref = await self.assign_replica(query)
        return assigned_ref


class Router:
    def __init__(self, controller_handle: ActorHandle):
        """Router process incoming queries: choose backend, and assign replica.

        Args:
            controller_handle(ActorHandle): The controller handle.
        """
        self.controller = controller_handle

        self.endpoint_policies: Dict[str, EndpointPolicy] = dict()
        self.backend_replicas: Dict[str, ReplicaSet] = defaultdict(ReplicaSet)

        self._pending_endpoints: DefaultDict[str, asyncio.Event] = defaultdict(
            asyncio.Event)

        # -- Metrics Registration -- #
        self.num_router_requests = metrics.Count(
            "num_router_requests",
            description="Number of requests processed by the router.",
            tag_keys=("endpoint", ))

    async def setup_in_async_loop(self):
        # NOTE(simon): Instead of performing initialization in __init__,
        # We separated the init of LongPollerAsyncClient to this method because
        # __init__ might be called in sync context. LongPollerAsyncClient
        # requires async context.
        self.long_pull_client = LongPollerAsyncClient(
            self.controller, {
                "traffic_policies": self._update_traffic_policies,
                "worker_handles": self._update_worker_handles,
                "backend_configs": self._update_backend_configs,
            })

    async def _update_traffic_policies(self, traffic_policies):
        for endpoint, traffic_policy in traffic_policies.items():
            self.endpoint_policies[endpoint] = RandomEndpointPolicy(
                traffic_policy)
            if endpoint in self._pending_endpoints:
                event = self._pending_endpoints.pop(endpoint)
                event.set()

    async def _update_worker_handles(self, worker_handles):
        for backend_tag, replica_handles in worker_handles.items():
            self.backend_replicas[backend_tag].update_worker_replicas(
                replica_handles)

    async def _update_backend_configs(self, backend_configs):
        for backend_tag, config in backend_configs.items():
            self.backend_replicas[backend_tag].set_max_concurrent_queries(
                config.max_concurrent_queries)

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
            context=request_meta.request_context,
            metadata=request_meta,
        )

        if endpoint not in self.endpoint_policies:
            logger.info(
                f"Endpoint {endpoint} doesn't exist, waiting for registration."
            )
            await self._pending_endpoints[endpoint].wait()

        endpoint_policy = self.endpoint_policies[endpoint]
        chosen_backend, *shadow_backends = endpoint_policy.assign(query)

        result_ref = await self.backend_replicas[chosen_backend
                                                 ].assign_replica(query)
        for backend in shadow_backends:
            await self.backend_replicas[backend].assign_replica(query)

        self.num_router_requests.record(1, tags={"endpoint": endpoint})

        return result_ref
