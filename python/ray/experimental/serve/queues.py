from collections import defaultdict, deque

import numpy as np

import ray
from ray.experimental.serve.utils import logger
import itertools
from blist import sortedlist
import time


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

        if result_object_id is None:
            self.result_object_id = ray.ObjectID.from_random()
        else:
            self.result_object_id = result_object_id

        # Service level objective in milliseconds. This is expected to be the
        # absolute time since unix epoch.
        self.request_slo_ms = request_slo_ms

    # adding comparator fn for maintaining an
    # ascending order sorted list w.r.t request_slo_ms
    def __lt__(self, other):
        return self.request_slo_ms < other.request_slo_ms


class WorkIntent:
    def __init__(self, replica_handle):
        self.replica_handle = replica_handle


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
        # service_name -> request queue
        self.queues = defaultdict(deque)

        # service_name -> traffic_policy
        self.traffic = defaultdict(dict)

        # backend_name -> backend_config
        self.backend_info = dict()

        # backend_name -> worker request queue
        self.workers = defaultdict(deque)

        # backend_name -> worker payload queue
        # using blist sortedlist for deadline awareness
        # blist is chosen because:
        # 1. pop operation should be O(1) (amortized)
        #    (helpful even for batched pop)
        # 2. There should not be significant overhead in
        #    maintaining the sorted list.
        # 3. The blist implementation is fast and uses C extensions.
        self.buffer_queues = defaultdict(sortedlist)

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
    def enqueue_request(self,
                        service,
                        request_args,
                        request_kwargs,
                        request_context,
                        request_slo_ms=None):
        if request_slo_ms is None:
            # if request_slo_ms is not specified then set it to a high level
            request_slo_ms = 1e9

        # add wall clock time to specify the deadline for completion of query
        # this also assures FIFO behaviour if request_slo_ms is not specified
        request_slo_ms += (time.time() * 1000)
        query = Query(request_args, request_kwargs, request_context,
                      request_slo_ms)
        self.queues[service].append(query)
        self.flush()
        return query.result_object_id.binary()

    def dequeue_request(self, backend, replica_handle):
        intention = WorkIntent(replica_handle)
        self.workers[backend].append(intention)
        self.flush()

    def remove_and_destory_replica(self, backend, replica_handle):
        # NOTE: this function scale by O(#replicas for the backend)
        new_queue = deque()
        target_id = replica_handle._actor_id

        for work_intent in self.workers[backend]:
            if work_intent.replica_handle._actor_id != target_id:
                new_queue.append(work_intent)

        self.workers[backend] = new_queue

        replica_handle.__ray_terminate__.remote()

    def link(self, service, backend):
        logger.debug("Link %s with %s", service, backend)
        self.set_traffic(service, {backend: 1.0})

    def set_traffic(self, service, traffic_dict):
        logger.debug("Setting traffic for service %s to %s", service,
                     traffic_dict)
        self.traffic[service] = traffic_dict
        self.flush()

    def set_backend_config(self, backend, config_dict):
        logger.debug("Setting backend config for "
                     "backend {} to {}".format(backend, config_dict))
        self.backend_info[backend] = config_dict

    def flush(self):
        """In the default case, flush calls ._flush.

        When this class is a Ray actor, .flush can be scheduled as a remote
        method invocation.
        """
        self._flush()

    def _get_available_backends(self, service):
        backends_in_policy = set(self.traffic[service].keys())
        available_workers = {
            backend
            for backend, queues in self.workers.items() if len(queues) > 0
        }
        return list(backends_in_policy.intersection(available_workers))

    # flushes the buffer queue and assigns work to workers
    def _flush_buffer(self):
        for service in self.queues.keys():
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

                while len(buffer_queue) and len(work_queue):
                    # get the work from work intent queue
                    work = work_queue.popleft()
                    # see if backend accepts batched queries
                    if max_batch_size is not None:
                        pop_size = min(len(buffer_queue), max_batch_size)
                        request = [
                            buffer_queue.pop(0) for _ in range(pop_size)
                        ]
                    else:
                        request = buffer_queue.pop(0)

                    work.replica_handle._ray_serve_call.remote(request)

    # selects the backend and puts the service queue query to the buffer
    # different policies will implement different backend selection policies
    def _flush_service_queue(self):
        """
        Expected Implementation:
            The implementer is expected to access and manipulate
            self.queues        : dict[str,Deque]
            self.buffer_queues : dict[str,sortedlist]
        For registering the implemented policies register at policy.py!
        Expected Behavior:
            the Deque of all services in self.queues linked with
            atleast one backend must be empty irrespective of whatever
            backend policy is implemented.
        """
        pass

    # _flush function has to flush the service and buffer queues.
    def _flush(self):
        self._flush_service_queue()
        self._flush_buffer()


class CentralizedQueuesActor(CentralizedQueues):
    """
    A wrapper class for converting wrapper policy classes to ray
    actors. This is needed to make `flush` call asynchronous.
    """
    self_handle = None

    def register_self_handle(self, handle_to_this_actor):
        self.self_handle = handle_to_this_actor

    def flush(self):
        if self.self_handle:
            self.self_handle._flush.remote()
        else:
            self._flush()


class RandomPolicyQueue(CentralizedQueues):
    """
    A wrapper class for Random policy.This backend selection policy is
    `Stateless` meaning the current decisions of selecting backend are
    not dependent on previous decisions. Random policy (randomly) samples
    backends based on backend weights for every query. This policy uses the
    weights assigned to backends.
    """

    def _flush_service_queue(self):
        # perform traffic splitting for requests
        for service, queue in self.queues.items():
            # while there are incoming requests and there are backends
            while len(queue) and len(self.traffic[service]):
                backend_names = list(self.traffic[service].keys())
                backend_weights = list(self.traffic[service].values())
                # randomly choose a backend for every query
                chosen_backend = np.random.choice(
                    backend_names, p=backend_weights).squeeze()

                request = queue.popleft()
                self.buffer_queues[chosen_backend].add(request)


@ray.remote
class RandomPolicyQueueActor(RandomPolicyQueue, CentralizedQueuesActor):
    pass


class RoundRobinPolicyQueue(CentralizedQueues):
    """
    A wrapper class for RoundRobin policy. This backend selection policy
    is `Stateful` meaning the current decisions of selecting backend are
    dependent on previous decisions. RoundRobinPolicy assigns queries in
    an interleaved manner to every backend serving for a service. Consider
    backend A,B linked to a service. Now queries will be assigned to backends
    in the following order - [ A, B, A, B ... ] . This policy doesn't use the
    weights assigned to backends.
    """

    # Saves the information about last assigned
    # backend for every service
    round_robin_iterator_map = {}

    def set_traffic(self, service, traffic_dict):
        logger.debug("Setting traffic for service %s to %s", service,
                     traffic_dict)
        self.traffic[service] = traffic_dict
        backend_names = list(self.traffic[service].keys())
        self.round_robin_iterator_map[service] = itertools.cycle(backend_names)
        self.flush()

    def _flush_service_queue(self):
        # perform traffic splitting for requests
        for service, queue in self.queues.items():
            # if there are incoming requests and there are backends
            if len(queue) and len(self.traffic[service]):
                while len(queue):
                    # choose the next backend available from persistent
                    # information
                    chosen_backend = next(
                        self.round_robin_iterator_map[service])
                    request = queue.popleft()
                    self.buffer_queues[chosen_backend].add(request)


@ray.remote
class RoundRobinPolicyQueueActor(RoundRobinPolicyQueue,
                                 CentralizedQueuesActor):
    pass


class PowerOfTwoPolicyQueue(CentralizedQueues):
    """
    A wrapper class for powerOfTwo policy. This backend selection policy is
    `Stateless` meaning the current decisions of selecting backend are
    dependent on previous decisions. PowerOfTwo policy (randomly) samples two
    backends (say Backend A,B among A,B,C) based on the backend weights
    specified and chooses the backend which is less loaded. This policy uses
    the weights assigned to backends.
    """

    def _flush_service_queue(self):
        # perform traffic splitting for requests
        for service, queue in self.queues.items():
            # while there are incoming requests and there are backends
            while len(queue) and len(self.traffic[service]):
                backend_names = list(self.traffic[service].keys())
                backend_weights = list(self.traffic[service].values())
                if len(self.traffic[service]) >= 2:
                    # randomly pick 2 backends
                    backend1, backend2 = np.random.choice(
                        backend_names, 2, p=backend_weights)

                    # see the length of buffer queues of the two backends
                    # and pick the one which has less no. of queries
                    # in the buffer
                    if (len(self.buffer_queues[backend1]) <= len(
                            self.buffer_queues[backend2])):
                        chosen_backend = backend1
                    else:
                        chosen_backend = backend2
                else:
                    chosen_backend = np.random.choice(
                        backend_names, p=backend_weights).squeeze()
                request = queue.popleft()
                self.buffer_queues[chosen_backend].add(request)


@ray.remote
class PowerOfTwoPolicyQueueActor(PowerOfTwoPolicyQueue,
                                 CentralizedQueuesActor):
    pass


class FixedPackingPolicyQueue(CentralizedQueues):
    """
    A wrapper class for FixedPacking policy. This backend selection policy is
    `Stateful` meaning the current decisions of selecting backend are dependent
    on previous decisions. FixedPackingPolicy is k RoundRobin policy where
    first packing_num queries are handled by 'backend-1' and next k queries are
    handled by 'backend-2' and so on ... where 'backend-1' and 'backend-2' are
    served by the same service. This policy doesn't use the weights assigned to
    backends.

    """

    def __init__(self, packing_num=3):
        # Saves the information about last assigned
        # backend for every service
        self.fixed_packing_iterator_map = {}
        self.packing_num = packing_num
        super().__init__()

    def set_traffic(self, service, traffic_dict):
        logger.debug("Setting traffic for service %s to %s", service,
                     traffic_dict)
        self.traffic[service] = traffic_dict
        backend_names = list(self.traffic[service].keys())
        self.fixed_packing_iterator_map[service] = itertools.cycle(
            itertools.chain.from_iterable(
                itertools.repeat(x, self.packing_num) for x in backend_names))
        self.flush()

    def _flush_service_queue(self):
        # perform traffic splitting for requests
        for service, queue in self.queues.items():
            # if there are incoming requests and there are backends
            if len(queue) and len(self.traffic[service]):
                while len(queue):
                    # choose the next backend available from persistent
                    # information
                    chosen_backend = next(
                        self.fixed_packing_iterator_map[service])
                    request = queue.popleft()
                    self.buffer_queues[chosen_backend].add(request)


@ray.remote
class FixedPackingPolicyQueueActor(FixedPackingPolicyQueue,
                                   CentralizedQueuesActor):
    pass
