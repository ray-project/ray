import asyncio
import copy
from collections import defaultdict, deque
import functools
import itertools
import time
from typing import DefaultDict, Iterable, List, Dict, Any, Optional, Union
import pickle
from dataclasses import dataclass, field

import ray
from ray.actor import ActorHandle
from ray.exceptions import RayTaskError
from ray.serve.long_pull import LongPullerAsyncClient, LongPullerSyncClient
from ray.util import metrics
from ray.serve.context import TaskContext
from ray.serve.endpoint_policy import EndpointPolicy, RandomEndpointPolicy
from ray.serve.utils import logger, chain_future

REPORT_QUEUE_LENGTH_PERIOD_S = 1.0


@dataclass
class RequestMetadata:
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
    async_future: Optional[asyncio.Future] = None

    tick_enter_router: Optional[float] = None
    tick_enter_replica: Optional[float] = None

    def __reduce__(self):
        return type(self).ray_deserialize, (self.ray_serialize(), )

    def ray_serialize(self):
        # NOTE: this method is needed because Query need to be serialized and
        # sent to the replica worker. However, after we send the query to
        # replica worker the async_future is still needed to retrieve the final
        # result. Therefore we need a way to pass the information to replica
        # worker without removing async_future.
        clone = copy.copy(self.__dict__)
        clone.pop("async_future")
        return pickle.dumps(clone)

    @staticmethod
    def ray_deserialize(value):
        kwargs = pickle.loads(value)
        return Query(**kwargs)


class ReplicaSet:
    """Data structure represents a set of replica actor handles"""

    def __init__(self):
        self.max_concurrent_queries = 8  # max_concurrent_queries default
        self.in_flight_queries: Dict[ActorHandle, set] = dict()
        self.replica_iterator = itertools.cycle(self.in_flight_queries.keys())

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

    def _try_assign_replica(self, query: Query) -> Optional[ray.ObjectRef]:
        for _ in range(len(self.in_flight_queries.keys())):
            replica = next(self.replica_iterator)
            if len(self.in_flight_queries[replica]
                   ) >= self.max_concurrent_queries:
                # This replica is overloaded, try next one
                continue
            ref = replica.handle_request.remote(query.ray_serialize())
            self.in_flight_queries[replica].add(ref)
            return ref
        return None

    @property
    def _all_queries_ref(self):
        return list(
            itertools.chain.from_iterable(self.in_flight_queries.values()))

    def _drain_completed_object_refs(self) -> int:
        refs = self._all_queries_ref
        done, _ = ray.wait(refs, num_returns=len(refs), timeout=0)
        for replica_in_flight_queries in self.in_flight_queries.values():
            replica_in_flight_queries.difference_update(done)
        return len(done)


class SyncReplicaSet(ReplicaSet):
    def __init__(self, long_puller_client):
        super().__init__()
        self.client: LongPullerSyncClient = long_puller_client

    def _wait_for_first_completed(self):
        on_state_changed = self.client.in_flight_request_ref
        ready, _ = ray.wait(
            self._all_queries_ref + [on_state_changed], num_returns=1)

        # If the configuration changed, trigger the callback to update
        # ReplicaSet internal state.
        if on_state_changed in ready:
            self.client.refresh()

    def assign_replica(self, query) -> ray.ObjectRef:
        assigned_ref = self._try_assign_replica(query)
        if assigned_ref is not None:
            return assigned_ref

        # Can't assign replica right now, try drain in-flight requests.
        num_finished = self._drain_completed_object_refs()
        if num_finished == 0:
            self._wait_for_first_completed()

        # Now we probably have a free replica, let's recurse and try again.
        return self.assign_replica(query)


class AsyncReplicaSet(ReplicaSet):
    async def assign_replica(self, query) -> ray.ObjectRef:
        assigned_ref = self._try_assign_replica(query)
        if assigned_ref is not None:
            return assigned_ref

        num_finished = self._drain_completed_object_refs()
        if num_finished == 0:
            await asyncio.wait(
                self._all_queries_ref, return_when=asyncio.FIRST_COMPLETED)

        return await self.assign_replica(query)


class LongPullRouter:
    def __init__(self, name, controller_name):
        self.name = name
        self.controller = ray.get_actor(controller_name)
        self.endpoint_policies: Dict[str, EndpointPolicy] = dict()
        self.in_async_ctx = asyncio.get_event_loop().is_running()

        # Forward declare here so SyncReplicaSet can get a pointer to to-be-created
        # long pull client.
        self.long_pull_client = None
        replica_set_class = AsyncReplicaSet if self.in_async_ctx else functools.partial(
            SyncReplicaSet, self.long_pull_client)
        self.backend_replicas: Dict[str, Union[
            SyncReplicaSet, AsyncReplicaSet]] = defaultdict(replica_set_class)

        # This client must be intialized last because it will call self.update_state
        client_class = LongPullerAsyncClient if self.in_async_ctx else LongPullerSyncClient
        self.long_pull_client = client_class(
            self.controller,
            ["traffic_policies", "worker_handles", "backend_configs"],
            self.update_state)

    def update_state(self, object_snapshots, keys_updated):
        if "traffic_policies" in keys_updated:
            traffic_policies = object_snapshots["traffic_policies"]
            for endpoint, traffic_policy in traffic_policies.items():
                self.endpoint_policies[endpoint] = RandomEndpointPolicy(
                    traffic_policy)

        if "worker_handles" in keys_updated:
            worker_handles = object_snapshots["worker_handles"]
            for backend_tag, replica_handles in worker_handles.items():
                self.backend_replicas[backend_tag].update_worker_replicas(
                    replica_handles)

        if "backend_configs" in keys_updated:
            backend_configs = object_snapshots["backend_configs"]
            for backend_tag, config in backend_configs.items():
                replica_set = self.backend_replicas[backend_tag]
                replica_set.max_concurrent_queries = config.max_concurrent_queries

    def enqueue_request_blocking(self, query: Query) -> ray.ObjectRef:
        assert not self.in_async_ctx
        self.long_pull_client.refresh()

        endpoint = query.metadata.endpoint
        if endpoint not in self.endpoint_policies:
            self.long_pull_client.refresh(block=True)
            assert endpoint in self.endpoint_policies, f"Can't find endpoint {endpoint}"

        endpoint_policy = self.endpoint_policies[endpoint]
        chosen_backend, *shadow_backends = endpoint_policy.flush(query)

        result_ref = self.backend_replicas[chosen_backend].assign_replica(
            query)
        for backend in shadow_backends:
            self.backend_replicas[backend].assign_replica(query)

        return result_ref

    async def enqueue_request_async(self, query):
        assert self.in_async_ctx

        endpoint = query.metadata.endpoint
        assert endpoint in self.endpoint_policies, f"Can't find endpoint {endpoint}"

        endpoint_policy = self.endpoint_policies[endpoint]
        chosen_backend, *shadow_backends = endpoint_policy.flush(query)

        result_ref = await self.backend_replicas[chosen_backend
                                                 ].assign_replica(query)
        for backend in shadow_backends:
            await self.backend_replicas[backend].assign_replica(query)

        return result_ref


class Router:
    """A router that routes request to available workers."""

    async def setup(self, name, controller_name):
        # Note: Several queues are used in the router
        # - When a request come in, it's placed inside its corresponding
        #   endpoint_queue.
        # - The endpoint_queue is dequeued during flush operation, which moves
        #   the queries to backend buffer_queue. Here we match a request
        #   for an endpoint to a backend given some policy.
        # - The worker_queue is used to collect idle actor handle. These
        #   handles are dequed during the second stage of flush operation,
        #   which assign queries in buffer_queue to actor handle.

        self.name = name

        # -- Queues -- #

        # endpoint_name -> request queue
        # We use FIFO (left to right) ordering. The new items should be added
        # using appendleft. Old items should be removed via pop().
        self.endpoint_queues: DefaultDict[deque[Query]] = defaultdict(deque)
        # backend_name -> worker replica tag queue
        self.worker_queues: DefaultDict[deque[str]] = defaultdict(deque)
        # backend_name -> worker payload queue
        self.backend_queues = defaultdict(deque)

        # -- Metadata -- #

        # endpoint_name -> traffic_policy
        self.traffic = dict()
        # backend_name -> backend_config
        self.backend_info = dict()
        # replica tag -> worker_handle
        self.replicas = dict()
        # backend_name -> replica_tag -> concurrent queries counter
        self.queries_counter = defaultdict(lambda: defaultdict(int))

        # -- Synchronization -- #

        # This lock guarantee that only one flush operation can happen at a
        # time. Without the lock, multiple flush operation can pop from the
        # same buffer_queue and worker_queue and create deadlock. For example,
        # an operation holding the only query and the other flush operation
        # holding the only idle replica. Additionally, allowing only one flush
        # operation at a time simplifies design overhead for custom queuing and
        # batching policies.
        self.flush_lock = asyncio.Lock()

        # -- State Restoration -- #
        # Fetch the worker handles, traffic policies, and backend configs from
        # the controller. We use a "pull-based" approach instead of pushing
        # them from the controller so that the router can transparently recover
        # from failure.
        self.controller = ray.get_actor(controller_name)

        # -- Metrics Registration -- #
        self.num_router_requests = metrics.Count(
            "num_router_requests",
            description="Number of requests processed by the router.",
            tag_keys=("endpoint", ))
        self.num_error_endpoint_requests = metrics.Count(
            "num_error_endpoint_requests",
            description=(
                "Number of requests that errored when getting results "
                "for the endpoint."),
            tag_keys=("endpoint", ))
        self.num_error_backend_requests = metrics.Count(
            "num_error_backend_requests",
            description=("Number of requests that errored when getting result "
                         "from the backend."),
            tag_keys=("backend", ))

        self.backend_queue_size = metrics.Gauge(
            "backend_queued_queries",
            description=("Current number of queries queued "
                         "in the router for a backend"),
            tag_keys=("backend", ))

        asyncio.get_event_loop().create_task(self.report_queue_lengths())

        traffic_policies = ray.get(
            self.controller.get_traffic_policies.remote())
        for endpoint, traffic_policy in traffic_policies.items():
            await self.set_traffic(endpoint, traffic_policy)

        backend_dict = ray.get(self.controller.get_all_worker_handles.remote())
        for backend_tag, replica_dict in backend_dict.items():
            for replica_tag, worker in replica_dict.items():
                await self.add_new_worker(backend_tag, replica_tag, worker)

        backend_configs = ray.get(self.controller.get_backend_configs.remote())
        for backend, backend_config in backend_configs.items():
            await self.set_backend_config(backend, backend_config)

    async def enqueue_request(self, request_meta, *request_args,
                              **request_kwargs):
        endpoint = request_meta.endpoint
        logger.debug("Received a request for endpoint {}".format(endpoint))
        self.num_router_requests.record(1, tags={"endpoint": endpoint})

        request_context = request_meta.request_context
        query = Query(
            request_args,
            request_kwargs,
            request_context,
            metadata=request_meta,
            async_future=asyncio.get_event_loop().create_future())
        async with self.flush_lock:
            self.endpoint_queues[endpoint].appendleft(query)
            self.flush_endpoint_queue(endpoint)

        try:
            result = await query.async_future
        except RayTaskError as e:
            self.num_error_endpoint_requests.record(
                1, tags={"endpoint": endpoint})
            result = e
        return result

    async def add_new_worker(self, backend_tag, replica_tag, worker_handle):
        backend_replica_tag = backend_tag + ":" + replica_tag
        if backend_replica_tag in self.replicas:
            return
        self.replicas[backend_replica_tag] = worker_handle

        logger.debug("New worker added for backend '{}'".format(backend_tag))
        await self.mark_worker_idle(backend_tag, backend_replica_tag)

    async def mark_worker_idle(self, backend_tag, backend_replica_tag):
        logger.debug(
            "Marking backend with tag {} as idle.".format(backend_replica_tag))
        if backend_replica_tag not in self.replicas:
            return

        async with self.flush_lock:
            # NOTE(simon): This is a O(n) operation where n=len(worker_queue)
            if backend_replica_tag not in self.worker_queues[backend_tag]:
                self.worker_queues[backend_tag].appendleft(backend_replica_tag)
            self.flush_backend_queues([backend_tag])

    async def remove_worker(self, backend_tag, replica_tag):
        backend_replica_tag = backend_tag + ":" + replica_tag
        if backend_replica_tag not in self.replicas:
            return

        # We need this lock because we modify worker_queue here.
        async with self.flush_lock:
            del self.replicas[backend_replica_tag]

            try:
                self.worker_queues[backend_tag].remove(backend_replica_tag)
            except ValueError:
                # Replica doesn't exist in the idle worker queues.
                # It's ok because the worker might not have returned the
                # result.
                pass

    async def set_traffic(self, endpoint, traffic_policy):
        logger.debug("Setting traffic for endpoint %s to %s", endpoint,
                     traffic_policy)
        async with self.flush_lock:
            self.traffic[endpoint] = RandomEndpointPolicy(traffic_policy)
            self.flush_endpoint_queue(endpoint)

    async def remove_endpoint(self, endpoint):
        logger.debug("Removing endpoint {}".format(endpoint))
        async with self.flush_lock:
            self.flush_endpoint_queue(endpoint)
            if endpoint in self.endpoint_queues:
                del self.endpoint_queues[endpoint]
            if endpoint in self.traffic:
                del self.traffic[endpoint]

    async def set_backend_config(self, backend, config):
        logger.debug("Setting backend config for "
                     "backend {} to {}.".format(backend, config))
        async with self.flush_lock:
            self.backend_info[backend] = config

    async def remove_backend(self, backend):
        logger.debug("Removing backend {}".format(backend))
        async with self.flush_lock:
            self.flush_backend_queues([backend])
            if backend in self.backend_info:
                del self.backend_info[backend]
            if backend in self.worker_queues:
                del self.worker_queues[backend]
            if backend in self.backend_queues:
                del self.backend_queues[backend]

    def flush_endpoint_queue(self, endpoint):
        """Attempt to schedule any pending requests to available backends."""
        assert self.flush_lock.locked()
        if endpoint not in self.traffic:
            return
        if len(self.endpoint_queues[endpoint]):
            query = self.endpoint_queues[endpoint].pop()
            backends_to_flush = self.traffic[endpoint].flush(query)
            for b in backends_to_flush:
                self.backend_queues[b].appendleft(query)
            self.flush_backend_queues(backends_to_flush)

    # Flushes the specified backend queues and assigns work to workers.
    def flush_backend_queues(self, backends_to_flush):
        assert self.flush_lock.locked()
        for backend in backends_to_flush:
            # No workers available.
            if len(self.worker_queues[backend]) == 0:
                continue
            # No work to do.
            if len(self.backend_queues[backend]) == 0:
                continue

            buffer_queue = self.backend_queues[backend]
            worker_queue = self.worker_queues[backend]

            logger.debug("Assigning queries for backend {} with buffer "
                         "queue size {} and worker queue size {}".format(
                             backend, len(buffer_queue), len(worker_queue)))

            self._assign_query_to_worker(
                backend,
                buffer_queue,
                worker_queue,
            )

    async def _do_query(self, backend, backend_replica_tag, req):
        # If the worker died, this will be a RayActorError. Just return it and
        # let the HTTP proxy handle the retry logic.
        logger.debug("Sending query to replica:" + backend_replica_tag)
        start = time.time()
        worker = self.replicas[backend_replica_tag]
        try:
            object_ref = worker.handle_request.remote(req.ray_serialize())
            if req.metadata.is_shadow_query:
                # No need to actually get the result, but we do need to wait
                # until the call completes to mark the worker idle.
                await asyncio.wait([object_ref])
                result = ""
            else:
                result = await object_ref
        except RayTaskError as error:
            self.num_error_backend_requests.record(
                1, tags={"backend": backend})
            result = error
        self.queries_counter[backend][backend_replica_tag] -= 1
        await self.mark_worker_idle(backend, backend_replica_tag)
        logger.debug("Got result in {:.2f}s".format(time.time() - start))
        return result

    def _assign_query_to_worker(self, backend, buffer_queue, worker_queue):
        overloaded_replicas = set()
        while len(buffer_queue) and len(worker_queue):
            backend_replica_tag = worker_queue.pop()

            # The replica might have been deleted already.
            if backend_replica_tag not in self.replicas:
                continue

            # We have reached the end of the worker queue where all replicas
            # are overloaded.
            if backend_replica_tag in overloaded_replicas:
                break

            # This replica has too many in flight and processing queries.
            max_queries = 1
            if backend in self.backend_info:
                max_queries = self.backend_info[backend].max_concurrent_queries
            curr_queries = self.queries_counter[backend][backend_replica_tag]
            if curr_queries >= max_queries:
                # Put the worker back to the queue.
                worker_queue.appendleft(backend_replica_tag)
                overloaded_replicas.add(backend_replica_tag)
                logger.debug(
                    "Skipping backend {} because it has {} in flight "
                    "requests which exceeded the concurrency limit.".format(
                        backend, curr_queries))
                continue

            request = buffer_queue.pop()
            self.queries_counter[backend][backend_replica_tag] += 1
            future = asyncio.get_event_loop().create_task(
                self._do_query(backend, backend_replica_tag, request))

            # For shadow queries, just ignore the result.
            if not request.metadata.is_shadow_query:
                chain_future(future, request.async_future)

            worker_queue.appendleft(backend_replica_tag)

    async def report_queue_lengths(self):
        while True:
            queue_lengths = {
                backend: len(q)
                for backend, q in self.backend_queues.items()
            }
            self.controller.report_queue_lengths.remote(
                self.name, queue_lengths)

            for backend, length in queue_lengths.items():
                self.backend_queue_size.record(
                    length, tags={"backend": backend})

            await asyncio.sleep(REPORT_QUEUE_LENGTH_PERIOD_S)
