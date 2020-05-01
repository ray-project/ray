import asyncio
import copy
from collections import defaultdict
import time
from typing import DefaultDict, List

# Note on choosing blist instead of stdlib heapq
# 1. pop operation should be O(1) (amortized)
#    (helpful even for batched pop)
# 2. There should not be significant overhead in
#    maintaining the sorted list.
# 3. The blist implementation is fast and uses C extensions.
import blist

import ray
import ray.cloudpickle as pickle
from ray.serve.utils import logger, retry_actor_failures


class Query:
    def __init__(self,
                 request_args,
                 request_kwargs,
                 request_context,
                 request_slo_ms,
                 call_method="__call__",
                 async_future=None):
        self.request_args = request_args
        self.request_kwargs = request_kwargs
        self.request_context = request_context

        self.async_future = async_future

        # Service level objective in milliseconds. This is expected to be the
        # absolute time since unix epoch.
        self.request_slo_ms = request_slo_ms

        self.call_method = call_method

    def ray_serialize(self):
        # NOTE: this method is needed because Query need to be serialized and
        # sent to the replica worker. However, after we send the query to
        # replica worker the async_future is still needed to retrieve the final
        # result. Therefore we need a way to pass the information to replica
        # worker without removing async_future.
        clone = copy.copy(self).__dict__
        clone.pop("async_future")
        return pickle.dumps(clone, protocol=5)

    @staticmethod
    def ray_deserialize(value):
        kwargs = pickle.loads(value)
        return Query(**kwargs)

    # adding comparator fn for maintaining an
    # ascending order sorted list w.r.t request_slo_ms
    def __lt__(self, other):
        return self.request_slo_ms < other.request_slo_ms

    def __repr__(self):
        return "<Query args={} kwargs={}>".format(self.request_args,
                                                  self.request_kwargs)


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
    """A router that routes request to available workers.

    The traffic policy is used to assign requests to workers.

    Traffic policy splits the traffic among different replicas
    probabilistically:

    1. When all backends are ready to receive traffic, we will randomly
       choose a backend based on the weights assigned by the traffic policy
       dictionary.

    2. When more than 1 but not all backends are ready, we will normalize the
       weights of the ready backends to 1 and choose a backend via sampling.

    3. When there is only 1 backend ready, we will only use that backend.
    """

    async def __init__(self):
        # Note: Several queues are used in the router
        # - When a request come in, it's placed inside its corresponding
        #   service_queue.
        # - The service_queue is dequed during flush operation, which moves
        #   the queries to backend buffer_queue. Here we match a request
        #   for a service to a backend given some policy.
        # - The worker_queue is used to collect idle actor handle. These
        #   handles are dequed during the second stage of flush operation,
        #   which assign queries in buffer_queue to actor handle.

        # -- Queues -- #

        # service_name -> request queue
        self.service_queues: DefaultDict[asyncio.Queue[Query]] = defaultdict(
            asyncio.Queue)
        # backend_name -> worker request queue
        self.worker_queues: DefaultDict[asyncio.Queue[
            ray.actor.ActorHandle]] = defaultdict(asyncio.Queue)
        # backend_name -> worker payload queue
        self.buffer_queues = defaultdict(blist.sortedlist)

        # -- Metadata -- #

        # service_name -> traffic_policy
        self.traffic = defaultdict(dict)
        # backend_name -> backend_config
        self.backend_info = dict()
        # replica tag -> worker_handle
        self.replicas = dict()

        # -- Synchronization -- #

        # This lock guarantee that only one flush operation can happen at a
        # time. Without the lock, multiple flush operation can pop from the
        # same buffer_queue and worker_queue and create deadlock. For example,
        # an operation holding the only query and the other flush operation
        # holding the only idle replica. Additionally, allowing only one flush
        # operation at a time simplifies design overhead for custom queuing and
        # batching polcies.
        self.flush_lock = asyncio.Lock()

        # Fetch the worker handles, traffic policies, and backend configs from
        # the master actor. We use a "pull-based" approach instead of pushing
        # them from the master so that the router can transparently recover
        # from failure.
        ray.serve.init()
        master_actor = ray.serve.api._get_master_actor()

        traffic_policies = retry_actor_failures(
            master_actor.get_traffic_policies)
        for endpoint, traffic_policy in traffic_policies.items():
            await self.set_traffic(endpoint, traffic_policy)

        backend_dict = retry_actor_failures(
            master_actor.get_all_worker_handles)
        for backend_tag, replica_dict in backend_dict.items():
            for replica_tag, worker in replica_dict.items():
                await self.add_new_worker(backend_tag, replica_tag, worker)

        backend_configs = retry_actor_failures(
            master_actor.get_backend_configs)
        for backend, backend_config in backend_configs.items():
            await self.set_backend_config(backend, backend_config)

    def is_ready(self):
        return True

    def get_metrics(self):
        return {
            "backend_{}_queue_size".format(backend_name): {
                "value": len(queue),
                "type": "counter",
            }
            for backend_name, queue in self.buffer_queues.items()
        }

    async def enqueue_request(self, request_meta, *request_args,
                              **request_kwargs):
        service = request_meta.service
        logger.debug("Received a request for service {}".format(service))

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
            async_future=asyncio.get_event_loop().create_future())
        await self.service_queues[service].put(query)
        await self.flush()

        # Note: a future change can be to directly return the ObjectID from
        # replica task submission
        result = await query.async_future
        return result

    async def add_new_worker(self, backend_tag, replica_tag, worker_handle):
        backend_replica_tag = backend_tag + ":" + replica_tag
        if backend_replica_tag in self.replicas:
            return
        self.replicas[backend_replica_tag] = worker_handle

        logger.debug("New worker added for backend '{}'".format(backend_tag))
        # await worker_handle.ready.remote()
        await self.mark_worker_idle(backend_tag, backend_replica_tag)

    async def mark_worker_idle(self, backend_tag, backend_replica_tag):
        if backend_replica_tag not in self.replicas:
            return

        await self.worker_queues[backend_tag].put(backend_replica_tag)
        await self.flush()

    async def remove_worker(self, backend_tag, replica_tag):
        backend_replica_tag = backend_tag + ":" + replica_tag
        if backend_replica_tag not in self.replicas:
            return
        worker_handle = self.replicas.pop(backend_replica_tag)

        # We need this lock because we modify worker_queue here.
        async with self.flush_lock:
            old_queue = self.worker_queues[backend_tag]
            new_queue = asyncio.Queue()

            while not old_queue.empty():
                curr_tag = await old_queue.get()
                if curr_tag != backend_replica_tag:
                    await new_queue.put(curr_tag)

            self.worker_queues[backend_tag] = new_queue
            # We need to terminate the worker here instead of from the master
            # so we can guarantee that the router won't submit any more tasks
            # on it.
            worker_handle.__ray_terminate__.remote()

    async def set_traffic(self, service, traffic_dict):
        logger.debug("Setting traffic for service %s to %s", service,
                     traffic_dict)
        self.traffic[service] = traffic_dict
        await self.flush()

    async def remove_service(self, service):
        logger.debug("Removing service {}".format(service))
        async with self.flush_lock:
            await self._flush_service_queues()
            await self._flush_buffer_queues()
            if service in self.service_queues:
                del self.service_queues[service]
            if service in self.traffic:
                del self.traffic[service]

    async def set_backend_config(self, backend, config):
        logger.debug("Setting backend config for "
                     "backend {} to {}.".format(backend, config))
        self.backend_info[backend] = config

    async def remove_backend(self, backend):
        logger.debug("Removing backend {}".format(backend))
        async with self.flush_lock:
            await self._flush_service_queues()
            await self._flush_buffer_queues()
            if backend in self.backend_info:
                del self.backend_info[backend]
            if backend in self.worker_queues:
                del self.worker_queues[backend]
            if backend in self.buffer_queues:
                del self.buffer_queues[backend]

    async def flush(self):
        """In the default case, flush calls ._flush.

        When this class is a Ray actor, .flush can be scheduled as a remote
        method invocation.
        """
        async with self.flush_lock:
            await self._flush_service_queues()
            await self._flush_buffer_queues()

    def _get_available_backends(self, service):
        backends_in_policy = set(self.traffic[service].keys())
        available_workers = {
            backend
            for backend, queues in self.worker_queues.items()
            if queues.qsize() > 0
        }
        return list(backends_in_policy.intersection(available_workers))

    async def _flush_service_queues(self):
        """Selects the backend and puts the service queue query to the buffer
        Expected Implementation:
            The implementer is expected to access and manipulate
            self.service_queues        : dict[str,Deque]
            self.buffer_queues : dict[str,sortedlist]
        For registering the implemented policies register at policy.py
        Expected Behavior:
            the Deque of all services in self.service_queues linked with
            atleast one backend must be empty irrespective of whatever
            backend policy is implemented.
        """
        raise NotImplementedError(
            "This method should be implemented by child class.")

    # flushes the buffer queue and assigns work to workers
    async def _flush_buffer_queues(self):
        for service in self.traffic.keys():
            ready_backends = self._get_available_backends(service)
            for backend in ready_backends:
                # no work available
                if len(self.buffer_queues[backend]) == 0:
                    continue

                buffer_queue = self.buffer_queues[backend]
                worker_queue = self.worker_queues[backend]

                logger.debug("Assigning queries for backend {} with buffer "
                             "queue size {} and worker queue size {}".format(
                                 backend, len(buffer_queue),
                                 worker_queue.qsize()))

                max_batch_size = None
                if backend in self.backend_info:
                    max_batch_size = self.backend_info[backend].max_batch_size

                await self._assign_query_to_worker(
                    backend, buffer_queue, worker_queue, max_batch_size)

    async def _do_query(self, backend, backend_replica_tag, req):
        # If the worker died, this will be a RayActorError. Just return it and
        # let the HTTP proxy handle the retry logic.
        logger.debug("Sending query to replica:" + backend_replica_tag)
        start = time.time()
        worker = self.replicas[backend_replica_tag]
        result = await worker.handle_request.remote(req)
        await self.mark_worker_idle(backend, backend_replica_tag)
        logger.debug("Got result in {:.2f}s", time.time() - start)
        return result

    async def _assign_query_to_worker(self,
                                      backend,
                                      buffer_queue,
                                      worker_queue,
                                      max_batch_size=None):

        while len(buffer_queue) and worker_queue.qsize():
            backend_replica_tag = await worker_queue.get()
            if max_batch_size is None:  # No batching
                request = buffer_queue.pop(0)
                future = asyncio.get_event_loop().create_task(
                    self._do_query(backend, backend_replica_tag, request))
                # chaining satisfies request.async_future with future result.
                asyncio.futures._chain_future(future, request.async_future)
            else:
                real_batch_size = min(len(buffer_queue), max_batch_size)
                requests = [
                    buffer_queue.pop(0) for _ in range(real_batch_size)
                ]

                # split requests by method type
                requests_group = defaultdict(list)
                for request in requests:
                    requests_group[request.call_method].append(request)

                for group in requests_group.values():
                    future = asyncio.get_event_loop().create_task(
                        self._do_query(backend, backend_replica_tag, group))
                    future.add_done_callback(
                        _make_future_unwrapper(
                            client_futures=[req.async_future for req in group],
                            host_future=future))
