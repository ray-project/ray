from enum import Enum
import itertools

import numpy as np

from ray.serve.utils import logger


class RandomPolicy:
    """
    A stateless policy that makes a weighted random decision to map each query
    to a backend using the specified weights.
    """

    def __init__(self, endpoint, traffic_dict):
        self.endpoint = endpoint
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


class RoundRobinPolicy:
    """
    A wrapper class for RoundRobin policy. This backend selection policy
    is `Stateful` meaning the current decisions of selecting backend are
    dependent on previous decisions. RoundRobinPolicy assigns queries in
    an interleaved manner to every backend serving for an endpoint. Consider
    backend A,B linked to a endpoint. Now queries will be assigned to backends
    in the following order - [ A, B, A, B ... ] . This policy doesn't use the
    weights assigned to backends.
    """

    def __init__(self, endpoint, traffic_dict):
        self.endpoint = endpoint
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


class PowerOfTwoPolicy:
    """
    A wrapper class for powerOfTwo policy. This backend selection policy is
    `Stateless` meaning the current decisions of selecting backend are
    dependent on previous decisions. PowerOfTwo policy (randomly) samples two
    backends (say Backend A,B among A,B,C) based on the backend weights
    specified and chooses the backend which is less loaded. This policy uses
    the weights assigned to backends.
    """

    def __init__(self, endpoint, traffic_dict):
        self.endpoint = endpoint
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


class FixedPackingPolicy:
    """
    A wrapper class for FixedPacking policy. This backend selection policy is
    `Stateful` meaning the current decisions of selecting backend are dependent
    on previous decisions. FixedPackingPolicy is k RoundRobin policy where
    first packing_num queries are handled by 'backend-1' and next k queries are
    handled by 'backend-2' and so on ... where 'backend-1' and 'backend-2' are
    served by the same endpoint. This policy doesn't use the weights assigned
    to backends.

    """

    def __init__(self, endpoint, traffic_dict, packing_num=3):
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
    """
    A class for registering the backend selection policy.
    Add a name and the corresponding class.
    Serve will support the added policy and policy can be accessed
    in `serve.init` method through name provided here.
    """
    Random = RandomPolicy
    RoundRobin = RoundRobinPolicy
    PowerOfTwo = PowerOfTwoPolicy
    FixedPacking = FixedPackingPolicy
