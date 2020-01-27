import asyncio
from collections import defaultdict
import itertools
import time
from typing import DefaultDict

from blist import sortedlist
import numpy as np

import ray
from ray.experimental.serve.utils import logger


class Query:
    def __init__(self,
                 request_args,
                 request_kwargs,
                 request_context,
                 request_slo_ms,
                 result_object_id=None):
        self.request_args = request_args
        self.request_kwargs = request_kwargs
        self.request_context = request_context

        self.async_future = asyncio.get_event_loop().create_future()

        # Service level objective in milliseconds. This is expected to be the
        # absolute time since unix epoch.
        self.request_slo_ms = request_slo_ms
    
    def ray_serialize(self):
        copy = self.copy()
        copy.async_future = None
        return ray.cloudpickle.dumps(copy)
    
    @staticmethod
    def ray_deserialize(value):
        return ray.cloudpickle.loads(value)

    # adding comparator fn for maintaining an
    # ascending order sorted list w.r.t request_slo_ms
    def __lt__(self, other):
        return self.request_slo_ms < other.request_slo_ms

ray.register_custom_serializer(Query, Query.ray_serialize, Query.ray_deserialize)


class CentralizedQueues:
    """A router that routes request to available workers.

    Router aceepts each request from the `enqueue_request` method and enqueues
    it. It also accepts worker request to work (called work_intention in code)
    from workers via the `dequeue_request` method. The traffic policy is used
    to match requests with their corresponding workers.

    Behavior:
        >>> # psuedo-code
        >>> queue = CentralizedQueues()
        >>> queue.enqueue_request(
            "service-name", request_args, request_kwargs, request_context)
        # nothing happens, request is queued.
        # returns result ObjectID, which will contains the final result
        >>> queue.dequeue_request('backend-1', replica_handle)
        # nothing happens, work intention is queued.
        # return work ObjectID, which will contains the future request payload
        >>> queue.link('service-name', 'backend-1')
        # here the enqueue_requester is matched with replica, request
        # data is put into work ObjectID, and the replica processes the request
        # and store the result into result ObjectID

    Traffic policy splits the traffic among different replicas
    probabilistically:

    1. When all backends are ready to receive traffic, we will randomly
       choose a backend based on the weights assigned by the traffic policy
       dictionary.

    2. When more than 1 but not all backends are ready, we will normalize the
       weights of the ready backends to 1 and choose a backend via sampling.

    3. When there is only 1 backend ready, we will only use that backend.
    """

    def __init__(self):
        # Several queues are used in the router:
        # - When a request come in, it's placed inside its corresponding
        #   service_queue. 
        # - The service_queue is dequed during flush operation, which moves
        #   the queries to backend buffer_queue. Here we match a request
        #   for a service to a backend given some policy.

        # service_name -> request queue
        self.service_queues: DefaultDict[asyncio.Queue[Query]] = defaultdict(asyncio.Queue)
        # backend_name -> worker request queue
        self.workers: DefaultDict[asyncio.Queue[ray.actor.ActorHandle]] = defaultdict(asyncio.Queue)
        # backend_name -> worker payload queue
        # using blist sortedlist for deadline awareness
        # blist is chosen because:
        # 1. pop operation should be O(1) (amortized)
        #    (helpful even for batched pop)
        # 2. There should not be significant overhead in
        #    maintaining the sorted list.
        # 3. The blist implementation is fast and uses C extensions.
        self.buffer_queues = defaultdict(sortedlist)

        # service_name -> traffic_policy
        self.traffic = defaultdict(dict)
        # backend_name -> backend_config
        self.backend_info = dict()


    def is_ready(self):
        return True

    def _serve_metric(self):
        return {
            "backend_{}_queue_size".format(backend_name): {
                "value": len(queue),
                "type": "counter",
            }
            for backend_name, queue in self.buffer_queues.items()
        }

    # request_slo_ms is time specified in milliseconds till which the
    # answer of the query should be calculated
    async def enqueue_request(self,
                              service,
                              request_args,
                              request_kwargs,
                              request_context,
                              request_slo_ms=None):
        # if request_slo_ms is not specified then set it to a high value
        if request_slo_ms is None:
            request_slo_ms = 1e9

        # add wall clock time to specify the deadline for completion of query
        # this also assures FIFO behaviour if request_slo_ms is not specified
        request_slo_ms += (time.time() * 1000)

        query = Query(request_args, request_kwargs, request_context,
                      request_slo_ms)

        await self.service_queues[service].put(query)
        await self.flush()
        
        # Note: a future change can be to directly return the ObjectID from
        # replica task submission
        result = await query.async_future

        return result

    async def dequeue_request(self, backend, replica_handle):
        await self.workers[backend].put(replica_handle)
        await self.flush()

    def remove_and_destory_replica(self, backend, replica_handle):
        # NOTE: this function scale by O(#replicas for the backend)
        new_queue = asyncio.Queue()
        target_id = replica_handle._actor_id

        for work_intent in self.workers[backend]:
            if work_intent.replica_handle._actor_id != target_id:
                new_queue.append(work_intent)

        self.workers[backend] = new_queue
        replica_handle.__ray_terminate__.remote()

    def link(self, service, backend):
        logger.debug("Link %s with %s", service, backend)
        self.set_traffic(service, {backend: 1.0})

    async def set_traffic(self, service, traffic_dict):
        logger.debug("Setting traffic for service %s to %s", service,
                     traffic_dict)
        self.traffic[service] = traffic_dict
        # await self.flush()

    async def set_backend_config(self, backend, config_dict):
        logger.debug("Setting backend config for "
                     "backend {} to {}".format(backend, config_dict))
        self.backend_info[backend] = config_dict
        # await self.flush()

    async def flush(self):
        """In the default case, flush calls ._flush.

        When this class is a Ray actor, .flush can be scheduled as a remote
        method invocation.
        """
        self._flush_service_queue()
        await self._flush_buffer()

    def _get_available_backends(self, service):
        backends_in_policy = set(self.traffic[service].keys())
        available_workers = {
            backend
            for backend, queues in self.workers.items() if queues.qsize() > 0
        }
        return list(backends_in_policy.intersection(available_workers))

    # flushes the buffer queue and assigns work to workers
    async def _flush_buffer(self):
        for service in self.traffic.keys():
            ready_backends = self._get_available_backends(service)
            for backend in ready_backends:
                # no work available
                if len(self.buffer_queues[backend]) == 0:
                    continue

                buffer_queue = self.buffer_queues[backend]
                work_queue = self.workers[backend]
                max_batch_size = None
                if backend in self.backend_info:
                    max_batch_size = self.backend_info[backend][
                        "max_batch_size"]

                while len(buffer_queue) and work_queue.qsize():
                    # get the work from work intent queue
                    work = await work_queue.get()
                    # see if backend accepts batched queries
                    if max_batch_size is not None:
                        pop_size = min(len(buffer_queue), max_batch_size)
                        request = [
                            buffer_queue.pop(0) for _ in range(pop_size)
                        ]
                        future_caches = [r.async_future for r in request]
                        for r in request:
                            r.async_future = None
                        fut = work.replica_handle._ray_serve_call.remote(
                            request)
                        for r, f in zip(request, future_caches):
                            r.async_future = f
                    else:
                        request = buffer_queue.pop(0)

                        old_future = request.async_future
                        request.async_future = None
                        fut = work.replica_handle._ray_serve_call.remote(
                            request)
                        request.async_future = old_future

                    result = await fut
                    print("result", result)
                    if isinstance(request, list):
                        for res, req in zip(result, request):
                            req.async_future.set_result(res)
                    else:
                        request.async_future.set_result(result)
                        # future_cache.pop(fut)

    # selects the backend and puts the service queue query to the buffer
    # different policies will implement different backend selection policies
    def _flush_service_queue(self):
        """
        Expected Implementation:
            The implementer is expected to access and manipulate
            self.service_queues        : dict[str,Deque]
            self.buffer_queues : dict[str,sortedlist]
        For registering the implemented policies register at policy.py!
        Expected Behavior:
            the Deque of all services in self.service_queues linked with
            atleast one backend must be empty irrespective of whatever
            backend policy is implemented.
        """
        pass

    # _flush function has to flush the service and buffer queues.
    # async def _flush(self):
    #     # self._flush_service_queue()
    #     await self._flush_buffer()


class CentralizedQueuesActor(CentralizedQueues):
    """
    A wrapper class for converting wrapper policy classes to ray
    actors. This is needed to make `flush` call asynchronous.
    """
    pass
