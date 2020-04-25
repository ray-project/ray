from enum import Enum
import itertools

import numpy as np

import ray
from ray.serve.router import Router
from ray.serve.utils import logger


class RandomPolicyQueue(Router):
    """
    A wrapper class for Random policy.This backend selection policy is
    `Stateless` meaning the current decisions of selecting backend are
    not dependent on previous decisions. Random policy (randomly) samples
    backends based on backend weights for every query. This policy uses the
    weights assigned to backends.
    """

    async def _flush_service_queues(self):
        # perform traffic splitting for requests
        for service, queue in self.service_queues.items():
            # while there are incoming requests and there are backends
            while queue.qsize() and len(self.traffic[service]):
                backend_names = list(self.traffic[service].keys())
                backend_weights = list(self.traffic[service].values())
                # randomly choose a backend for every query
                chosen_backend = np.random.choice(
                    backend_names, replace=False, p=backend_weights).squeeze()
                logger.debug("Matching service {} to backend {}".format(
                    service, chosen_backend))

                request = await queue.get()
                self.buffer_queues[chosen_backend].add(request)


@ray.remote
class RandomPolicyQueueActor(RandomPolicyQueue):
    pass


class RoundRobinPolicyQueue(Router):
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

    async def set_traffic(self, service, traffic_dict):
        logger.debug("Setting traffic for service %s to %s", service,
                     traffic_dict)
        self.traffic[service] = traffic_dict
        backend_names = list(self.traffic[service].keys())
        self.round_robin_iterator_map[service] = itertools.cycle(backend_names)
        await self.flush()

    async def _flush_service_queues(self):
        # perform traffic splitting for requests
        for service, queue in self.service_queues.items():
            # if there are incoming requests and there are backends
            if queue.qsize() and len(self.traffic[service]):
                while queue.qsize():
                    # choose the next backend available from persistent
                    # information
                    chosen_backend = next(
                        self.round_robin_iterator_map[service])
                    request = await queue.get()
                    self.buffer_queues[chosen_backend].add(request)


@ray.remote
class RoundRobinPolicyQueueActor(RoundRobinPolicyQueue):
    pass


class PowerOfTwoPolicyQueue(Router):
    """
    A wrapper class for powerOfTwo policy. This backend selection policy is
    `Stateless` meaning the current decisions of selecting backend are
    dependent on previous decisions. PowerOfTwo policy (randomly) samples two
    backends (say Backend A,B among A,B,C) based on the backend weights
    specified and chooses the backend which is less loaded. This policy uses
    the weights assigned to backends.
    """

    async def _flush_service_queues(self):
        # perform traffic splitting for requests
        for service, queue in self.service_queues.items():
            # while there are incoming requests and there are backends
            while queue.qsize() and len(self.traffic[service]):
                backend_names = list(self.traffic[service].keys())
                backend_weights = list(self.traffic[service].values())
                if len(self.traffic[service]) >= 2:
                    # randomly pick 2 backends
                    backend1, backend2 = np.random.choice(
                        backend_names, 2, replace=False, p=backend_weights)

                    # see the length of buffer queues of the two backends
                    # and pick the one which has less no. of queries
                    # in the buffer
                    if (len(self.buffer_queues[backend1]) <= len(
                            self.buffer_queues[backend2])):
                        chosen_backend = backend1
                    else:
                        chosen_backend = backend2
                    logger.debug("[Power of two chocies] found two backends "
                                 "{} and {}: choosing {}.".format(
                                     backend1, backend2, chosen_backend))
                else:
                    chosen_backend = np.random.choice(
                        backend_names, replace=False,
                        p=backend_weights).squeeze()
                request = await queue.get()
                self.buffer_queues[chosen_backend].add(request)


@ray.remote
class PowerOfTwoPolicyQueueActor(PowerOfTwoPolicyQueue):
    pass


class FixedPackingPolicyQueue(Router):
    """
    A wrapper class for FixedPacking policy. This backend selection policy is
    `Stateful` meaning the current decisions of selecting backend are dependent
    on previous decisions. FixedPackingPolicy is k RoundRobin policy where
    first packing_num queries are handled by 'backend-1' and next k queries are
    handled by 'backend-2' and so on ... where 'backend-1' and 'backend-2' are
    served by the same service. This policy doesn't use the weights assigned to
    backends.

    """

    async def __init__(self, packing_num=3):
        # Saves the information about last assigned
        # backend for every service
        self.fixed_packing_iterator_map = {}
        self.packing_num = packing_num
        await super().__init__()

    async def set_traffic(self, service, traffic_dict):
        logger.debug("Setting traffic for service %s to %s", service,
                     traffic_dict)
        self.traffic[service] = traffic_dict
        backend_names = list(self.traffic[service].keys())
        self.fixed_packing_iterator_map[service] = itertools.cycle(
            itertools.chain.from_iterable(
                itertools.repeat(x, self.packing_num) for x in backend_names))
        await self.flush()

    async def _flush_service_queues(self):
        # perform traffic splitting for requests
        for service, queue in self.service_queues.items():
            # if there are incoming requests and there are backends
            if queue.qsize() and len(self.traffic[service]):
                while queue.qsize():
                    # choose the next backend available from persistent
                    # information
                    chosen_backend = next(
                        self.fixed_packing_iterator_map[service])
                    request = await queue.get()
                    self.buffer_queues[chosen_backend].add(request)


@ray.remote
class FixedPackingPolicyQueueActor(FixedPackingPolicyQueue):
    pass


class RoutePolicy(Enum):
    """
    A class for registering the backend selection policy.
    Add a name and the corresponding class.
    Serve will support the added policy and policy can be accessed
    in `serve.init` method through name provided here.
    """
    Random = RandomPolicyQueueActor
    RoundRobin = RoundRobinPolicyQueueActor
    PowerOfTwo = PowerOfTwoPolicyQueueActor
    FixedPacking = FixedPackingPolicyQueueActor
