import asyncio
import copy
from collections import defaultdict, deque
import time
from typing import DefaultDict, List
import pickle

import blist

from ray.exceptions import RayTaskError

import ray
from ray import serve
from ray.serve.metric import MetricClient
from ray.serve.policy import RandomEndpointPolicy
from ray.serve.utils import logger, chain_future


class Query:
    def __init__(
            self,
            request_args,
            request_kwargs,
            request_context,
            request_slo_ms,
            call_method="__call__",
            shard_key=None,
            async_future=None,
            is_shadow_query=False,
    ):
        self.request_args = request_args
        self.request_kwargs = request_kwargs
        self.request_context = request_context

        self.async_future = async_future

        # Service level objective in milliseconds. This is expected to be the
        # absolute time since unix epoch.
        self.request_slo_ms = request_slo_ms

        self.call_method = call_method
        self.shard_key = shard_key
        self.is_shadow_query = is_shadow_query

    def ray_serialize(self):
        # NOTE: this method is needed because Query need to be serialized and
        # sent to the replica worker. However, after we send the query to
        # replica worker the async_future is still needed to retrieve the final
        # result. Therefore we need a way to pass the information to replica
        # worker without removing async_future.
        clone = copy.copy(self).__dict__
        clone.pop("async_future")
        return pickle.dumps(clone)

    @staticmethod
    def ray_deserialize(value):
        kwargs = pickle.loads(value)
        return Query(**kwargs)

    # adding comparator fn for maintaining an
    # ascending order sorted list w.r.t request_slo_ms
    def __lt__(self, other):
        return self.request_slo_ms < other.request_slo_ms


def _make_future_unwrapper(client_futures: List[asyncio.Future],
                           host_future: asyncio.Future):
    """Distribute the result of host_future to each of client_future"""
    for client_future in client_futures:
        # Keep a reference to host future so the host future won't get
        # garbage collected.
        client_future.host_ref = host_future

    def unwrap_future(_):
        result = host_future.result()

        if isinstance(result, list):
            for client_future, result_item in zip(client_futures, result):
                client_future.set_result(result_item)
        else:  # Result is an exception.
            for client_future in client_futures:
                client_future.set_result(result)

    return unwrap_future


class Router:
    """A router that routes request to available workers."""

    async def setup(self, instance_name=None):
        # Note: Several queues are used in the router
        # - When a request come in, it's placed inside its corresponding
        #   endpoint_queue.
        # - The endpoint_queue is dequeued during flush operation, which moves
        #   the queries to backend buffer_queue. Here we match a request
        #   for an endpoint to a backend given some policy.
        # - The worker_queue is used to collect idle actor handle. These
        #   handles are dequed during the second stage of flush operation,
        #   which assign queries in buffer_queue to actor handle.

        # -- Queues -- #

        # endpoint_name -> request queue
        # We use FIFO (left to right) ordering. The new items should be added
        # using appendleft. Old items should be removed via pop().
        self.endpoint_queues: DefaultDict[deque[Query]] = defaultdict(deque)
        # backend_name -> worker replica tag queue
        self.worker_queues: DefaultDict[deque[str]] = defaultdict(deque)
        # backend_name -> worker payload queue
        self.backend_queues = defaultdict(blist.sortedlist)

        # -- Metadata -- #

        # endpoint_name -> traffic_policy
        self.traffic = dict()
        # backend_name -> backend_config
        self.backend_info = dict()
        # replica tag -> worker_handle
        self.replicas = dict()
        # replica_tag -> concurrent queries counter
        self.queries_counter = defaultdict(lambda: 0)

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
        serve.init(name=instance_name)
        controller = serve.api._get_controller()

        traffic_policies = ray.get(controller.get_traffic_policies.remote())
        for endpoint, traffic_policy in traffic_policies.items():
            await self.set_traffic(endpoint, traffic_policy)

        backend_dict = ray.get(controller.get_all_worker_handles.remote())
        for backend_tag, replica_dict in backend_dict.items():
            for replica_tag, worker in replica_dict.items():
                await self.add_new_worker(backend_tag, replica_tag, worker)

        backend_configs = ray.get(controller.get_backend_configs.remote())
        for backend, backend_config in backend_configs.items():
            await self.set_backend_config(backend, backend_config)

        # -- Metric Registration -- #
        [metric_exporter] = ray.get(controller.get_metric_exporter.remote())
        self.metric_client = MetricClient(metric_exporter)
        self.num_router_requests = self.metric_client.new_counter(
            "num_router_requests",
            description="Number of requests processed by the router.",
            label_names=("endpoint", ))
        self.num_error_endpoint_request = self.metric_client.new_counter(
            "num_error_endpoint_requests",
            description=("Number of requests errored when getting result "
                         "for endpoint."),
            label_names=("endpoint", ))
        self.num_error_backend_request = self.metric_client.new_counter(
            "num_error_backend_requests",
            description=("Number of requests errored when getting result "
                         "from backend."),
            label_names=("backend", ))

    async def enqueue_request(self, request_meta, *request_args,
                              **request_kwargs):
        endpoint = request_meta.endpoint
        logger.debug("Received a request for endpoint {}".format(endpoint))
        self.num_router_requests.labels(endpoint=endpoint).add()

        # check if the slo specified is directly the
        # wall clock time
        if request_meta.absolute_slo_ms is not None:
            request_slo_ms = request_meta.absolute_slo_ms
        else:
            request_slo_ms = request_meta.adjust_relative_slo_ms()
        request_context = request_meta.request_context
        query = Query(
            request_args,
            request_kwargs,
            request_context,
            request_slo_ms,
            call_method=request_meta.call_method,
            shard_key=request_meta.shard_key,
            async_future=asyncio.get_event_loop().create_future())
        async with self.flush_lock:
            self.endpoint_queues[endpoint].appendleft(query)
            self.flush_endpoint_queue(endpoint)

        try:
            result = await query.async_future
        except RayTaskError as e:
            self.num_error_endpoint_request.labels(endpoint=endpoint).add()
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
        backends_to_flush = self.traffic[endpoint].flush(
            self.endpoint_queues[endpoint], self.backend_queues)
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
            if req.is_shadow_query:
                # No need to actually get the result, but we do need to wait
                # until the call completes to mark the worker idle.
                await asyncio.wait([object_ref])
                result = ""
            else:
                result = await object_ref
        except RayTaskError as error:
            self.num_error_backend_request.labels(backend=backend).add()
            result = error
        self.queries_counter[backend_replica_tag] -= 1
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
            curr_queries = self.queries_counter[backend_replica_tag]
            if curr_queries >= max_queries:
                # Put the worker back to the queue.
                worker_queue.appendleft(backend_replica_tag)
                overloaded_replicas.add(backend_replica_tag)
                logger.debug(
                    "Skipping backend {} because it has {} in flight "
                    "requests which exceeded the concurrency limit.".format(
                        backend, curr_queries))
                continue

            request = buffer_queue.pop(0)
            self.queries_counter[backend_replica_tag] += 1
            future = asyncio.get_event_loop().create_task(
                self._do_query(backend, backend_replica_tag, request))

            # For shadow queries, just ignore the result.
            if not request.is_shadow_query:
                chain_future(future, request.async_future)

            worker_queue.appendleft(backend_replica_tag)
