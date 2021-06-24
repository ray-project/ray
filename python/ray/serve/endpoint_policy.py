from abc import ABCMeta, abstractmethod
import random
from hashlib import sha256
from functools import lru_cache
from typing import List

import numpy as np

import ray
from ray.serve.utils import logger


@lru_cache(maxsize=128)
def deterministic_hash(key: bytes) -> float:
    """Given an arbitrary bytes, return a deterministic value between 0 and 1.

    Note:
        This function uses stdlib random number generator because it's faster
        than numpy's. On a cache miss, the runtime of this function is about
        ~10us.
    """
    bytes_hash = sha256(key).digest()  # should return 32 bytes value
    int_seed = int.from_bytes(bytes_hash, "little", signed=False)
    random_state = random.Random(int_seed)
    return random_state.random()


class EndpointPolicy:
    """Defines the interface for a routing policy for a single endpoint.
    To add a new routing policy, a class should be defined that provides this
    interface. The class may be stateful, in which case it may also want to
    provide a non-default constructor. However, this state will be lost when
    the policy is updated (e.g., a new backend is added).
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def assign(self, query) -> List[str]:
        """Assign a query to a list of backends.

        Arguments:
            query (ray.serve.router.Query): the incoming query object.
        Returns:
            A list with length >= 1. It should contains the list of backend
            tags that had queries added to their queues. Ordered by importance.
            The first value should be the backend to assign and rest values
            correspond to shadow backends.
        """
        raise NotImplementedError()


class RandomEndpointPolicy(EndpointPolicy):
    """
    A stateless policy that makes a weighted random decision to map each query
    to a backend using the specified weights. If a shard key is provided in a
    query, the weighted random selection will be made deterministically based
    on the hash of the shard key.
    """

    def __init__(self, traffic_policy: "ray.serve.controller.TrafficPolicy"):
        self.backends = sorted(traffic_policy.traffic_dict.items())
        self.shadow_backends = list(traffic_policy.shadow_dict.items())

    def _select_backends(self, val):
        curr_sum = 0
        for name, weight in self.backends:
            curr_sum += weight
            if curr_sum > val:
                chosen_backend = name
                break
        else:
            assert False, "This should never be reached."

        shadow_backends = []
        for backend, backend_weight in self.shadow_backends:
            if val < backend_weight:
                shadow_backends.append(backend)

        return chosen_backend, shadow_backends

    def assign(self, query):
        if len(self.backends) == 0:
            raise ValueError("No backends to assign traffic to.")

        if query.metadata.shard_key is None:
            value = np.random.random()
        else:
            value = deterministic_hash(
                query.metadata.shard_key.encode("utf-8"))

        chosen_backend, shadow_backends = self._select_backends(value)
        logger.debug(f"Assigning query {query.metadata.request_id} "
                     f"to backend {chosen_backend}.")
        return [chosen_backend] + shadow_backends
