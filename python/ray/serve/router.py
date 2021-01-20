import asyncio
from enum import Enum
import itertools
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, ChainMap, Dict, Iterable, List, Optional

from ray.serve.exceptions import RayServeException

import ray
from ray.actor import ActorHandle
from ray.serve.constants import LongPollKey
from ray.serve.endpoint_policy import EndpointPolicy, RandomEndpointPolicy
from ray.serve.long_poll import LongPollAsyncClient
from ray.serve.utils import logger, compute_dict_delta, compute_iterable_delta
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
        """Try to assign query to a replica, return the object ref is succeeded
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
            # We are pretty sure a free replica is ready now.
            assigned_ref = self._try_assign_replica(query)
        return assigned_ref


class _PendingEndpointFound(Enum):
    """Enum for the status of pending endpoint registration."""
    ADDED = 1
    REMOVED = 2


class Router:
    def __init__(self, controller_handle: ActorHandle):
        """Router process incoming queries: choose backend, and assign replica.

        Args:
            controller_handle(ActorHandle): The controller handle.
        """
        self.controller = controller_handle

        self.endpoint_policies: Dict[str, EndpointPolicy] = dict()
        self.backend_replicas: Dict[str, ReplicaSet] = defaultdict(ReplicaSet)

        self._pending_endpoints: Dict[str, asyncio.Future] = dict()

        # -- Metrics Registration -- #
        self.num_router_requests = metrics.Count(
            "serve_num_router_requests",
            description="The number of requests processed by the router.",
            tag_keys=("endpoint", ))

    async def setup_in_async_loop(self):
        # NOTE(simon): Instead of performing initialization in __init__,
        # We separated the init of LongPollAsyncClient to this method because
        # __init__ might be called in sync context. LongPollAsyncClient
        # requires async context.
        self.long_poll_client = LongPollAsyncClient(
            self.controller, {
                LongPollKey.TRAFFIC_POLICIES: self._update_traffic_policies,
                LongPollKey.REPLICA_HANDLES: self._update_replica_handles,
                LongPollKey.BACKEND_CONFIGS: self._update_backend_configs,
            })

    async def _update_traffic_policies(self, traffic_policies):
        added, removed, updated = compute_dict_delta(self.endpoint_policies,
                                                     traffic_policies)

        for endpoint, traffic_policy in ChainMap(added, updated).items():
            self.endpoint_policies[endpoint] = RandomEndpointPolicy(
                traffic_policy)
            if endpoint in self._pending_endpoints:
                future = self._pending_endpoints.pop(endpoint)
                future.set_result(_PendingEndpointFound.ADDED)

        for endpoint, traffic_policy in removed.items():
            del self.endpoint_policies[endpoint]
            if endpoint in self._pending_endpoints:
                future = self._pending_endpoints.pop(endpoint)
                future.set_result(_PendingEndpointFound.REMOVED)

    async def _update_replica_handles(self, replica_handles):
        added, removed, updated = compute_dict_delta(self.backend_replicas,
                                                     replica_handles)

        for backend_tag, replica_handles in ChainMap(added, updated).items():
            self.backend_replicas[backend_tag].update_worker_replicas(
                replica_handles)

        for backend_tag in removed.keys():
            if backend_tag in self.backend_replicas:
                del self.backend_replicas[backend_tag]

    async def _update_backend_configs(self, backend_configs):
        added, removed, updated = compute_dict_delta(self.backend_replicas,
                                                     backend_configs)
        for backend_tag, config in ChainMap(added, updated).items():
            self.backend_replicas[backend_tag].set_max_concurrent_queries(
                config.max_concurrent_queries)

        for backend_tag in removed.keys():
            if backend_tag in self.backend_replicas:
                del self.backend_replicas[backend_tag]

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

        if endpoint not in self.endpoint_policies:
            logger.info(
                f"Endpoint {endpoint} doesn't exist, waiting for registration."
            )
            future = asyncio.get_event_loop().create_future()
            if endpoint not in self._pending_endpoints:
                self._pending_endpoints[endpoint] = future
            endpoint_status = await self._pending_endpoints[endpoint]
            if endpoint_status == _PendingEndpointFound.REMOVED:
                raise RayServeException(
                    f"Endpoint {endpoint} was removed. This request "
                    "cannot be completed.")

        endpoint_policy = self.endpoint_policies[endpoint]
        chosen_backend, *shadow_backends = endpoint_policy.assign(query)

        result_ref = await self.backend_replicas[chosen_backend
                                                 ].assign_replica(query)
        for backend in shadow_backends:
            await self.backend_replicas[backend].assign_replica(query)

        self.num_router_requests.record(1, tags={"endpoint": endpoint})

        return result_ref
