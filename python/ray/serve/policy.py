from abc import ABCMeta, abstractmethod
from enum import Enum
import itertools

import numpy as np

from ray.serve.utils import logger


class RoutingPolicy:
    """Defines the interface for a routing policy for a single endpoint.

    To add a new routing policy, a class should be defined that provides this
    interface. The class may be stateful, in which case it may also want to
    provide a non-default constructor. However, this state will be lost when
    the policy is updated (e.g., a new backend is added).
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    async def flush(self, endpoint_queue, backend_queues):
        """Flush the endpoint queue into the given backend queues.

        This method should assign each query in the endpoint_queue to a
        backend in the backend_queues. Queries are assigned by popping them
        from the endpoint queue and pushing them onto a backend queue. The
        method must also return a set of all backend tags so that the caller
        knows which backend_queues to flush.

        Arguments:
            endpoint_queue: asyncio.Queue containing queries to assign.
            backend_queues: Dict(str, asyncio.Queue) mapping backend tags to
            their corresponding query queues.

        Returns:
            Set of backend tags that had queries added to their queues.
        """
        assigned_backends = set()
        return assigned_backends


class RandomPolicy(RoutingPolicy):
    """
    A stateless policy that makes a weighted random decision to map each query
    to a backend using the specified weights.
    """

    def __init__(self, traffic_dict):
        self.backend_names = list(traffic_dict.keys())
        self.backend_weights = list(traffic_dict.values())

    async def flush(self, endpoint_queue, backend_queues):
        if len(self.backend_names) == 0:
            logger.info("No backends to assign traffic to.")
            return set()

        assigned_backends = set()
        while endpoint_queue.qsize():
            chosen_backend = np.random.choice(
                self.backend_names, replace=False,
                p=self.backend_weights).squeeze()
            assigned_backends.add(chosen_backend)
            backend_queues[chosen_backend].add(await endpoint_queue.get())

        return assigned_backends


class RoundRobinPolicy(RoutingPolicy):
    """A stateful policy that assigns queries in round-robin order."""

    def __init__(self, traffic_dict):
        # NOTE(edoakes): the backend weights are not used.
        self.backend_names = list(traffic_dict.keys())
        # Saves the information about last assigned backend for every endpoint.
        self.round_robin_iterator = itertools.cycle(self.backend_names)

    async def flush(self, endpoint_queue, backend_queues):
        if len(self.backend_names) == 0:
            logger.info("No backends to assign traffic to.")
            return set()

        assigned_backends = set()
        while endpoint_queue.qsize():
            chosen_backend = next(self.round_robin_iterator)
            assigned_backends.add(chosen_backend)
            backend_queues[chosen_backend].add(await endpoint_queue.get())

        return assigned_backends


class PowerOfTwoPolicy(RoutingPolicy):
    """A stateless policy that uses the "power of two" policy.

    For each query, two random backends are chosen. Of those two, the query is
    assigned to the backend whose queue length is shorter.
    """

    def __init__(self, traffic_dict):
        self.backend_names = list(traffic_dict.keys())
        self.backend_weights = list(traffic_dict.values())

    async def flush(self, endpoint_queue, backend_queues):
        if len(self.backend_names) == 0:
            logger.info("No backends to assign traffic to.")
            return set()

        assigned_backends = set()
        while endpoint_queue.qsize():
            if len(self.backend_names) >= 2:
                backend1, backend2 = np.random.choice(
                    self.backend_names,
                    2,
                    replace=False,
                    p=self.backend_weights)

                # Choose the backend that has a shorter queue.
                if (len(backend_queues[backend1]) <= len(
                        backend_queues[backend2])):
                    chosen_backend = backend1
                else:
                    chosen_backend = backend2
            else:
                chosen_backend = np.random.choice(
                    self.backend_names, replace=False,
                    p=self.backend_weights).squeeze()
            backend_queues[chosen_backend].add(await endpoint_queue.get())
            assigned_backends.add(chosen_backend)

        return assigned_backends


class FixedPackingPolicy(RoutingPolicy):
    """A stateful policy that uses a "fixed packing" policy.

    The policy round-robins groups of packing_num queries across backends. For
    example, the first packing_num queries are handled by backend-1, then the
    next packing_num queries are handled by backend-2, etc.
    """

    def __init__(self, traffic_dict, packing_num=3):
        # NOTE(edoakes): the backend weights are not used.
        self.backend_names = list(traffic_dict.keys())
        self.fixed_packing_iterator = itertools.cycle(
            itertools.chain.from_iterable(
                itertools.repeat(x, self.packing_num)
                for x in self.backend_names))
        self.packing_num = packing_num

    async def flush(self, endpoint_queue, backend_queues):
        if len(self.backend_names) == 0:
            logger.info("No backends to assign traffic to.")
            return set()

        assigned_backends = set()
        while endpoint_queue.qsize():
            chosen_backend = next(self.fixed_packing_iterator)
            backend_queues[chosen_backend].add(await endpoint_queue.get())
            assigned_backends.add(chosen_backend)

        return assigned_backends


class RoutePolicy(Enum):
    """All builtin routing policies."""
    Random = RandomPolicy
    RoundRobin = RoundRobinPolicy
    PowerOfTwo = PowerOfTwoPolicy
    FixedPacking = FixedPackingPolicy
