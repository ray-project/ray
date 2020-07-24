from abc import ABCMeta, abstractmethod
import copy
from hashlib import sha256

import numpy as np

from ray.serve.utils import logger


class EndpointPolicy:
    """Defines the interface for a routing policy for a single endpoint.

    To add a new routing policy, a class should be defined that provides this
    interface. The class may be stateful, in which case it may also want to
    provide a non-default constructor. However, this state will be lost when
    the policy is updated (e.g., a new backend is added).
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def flush(self, endpoint_queue, backend_queues):
        """Flush the endpoint queue into the given backend queues.

        This method should assign each query in the endpoint_queue to a
        backend in the backend_queues. Queries are assigned by popping them
        from the endpoint queue and pushing them onto a backend queue. The
        method must also return a set of all backend tags so that the caller
        knows which backend_queues to flush.

        Arguments:
            endpoint_queue: deque containing queries to assign.
            backend_queues: Dict(str, deque) mapping backend tags to
            their corresponding query queues.

        Returns:
            Set of backend tags that had queries added to their queues.
        """
        assigned_backends = set()
        return assigned_backends


class RandomEndpointPolicy(EndpointPolicy):
    """
    A stateless policy that makes a weighted random decision to map each query
    to a backend using the specified weights.

    If a shard key is provided in a query, the weighted random selection will
    be made deterministically based on the hash of the shard key.
    """

    def __init__(self, traffic_policy):
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

    def flush(self, endpoint_queue, backend_queues):
        if len(self.backends) == 0:
            logger.info("No backends to assign traffic to.")
            return set()

        assigned_backends = set()
        while len(endpoint_queue) > 0:
            query = endpoint_queue.pop()
            if query.shard_key is None:
                rstate = np.random
            else:
                sha256_seed = sha256(query.shard_key.encode("utf-8"))
                seed = np.frombuffer(sha256_seed.digest(), dtype=np.uint32)
                # Note(simon): This constructor takes 100+us, maybe cache this?
                rstate = np.random.RandomState(seed)

            chosen_backend, shadow_backends = self._select_backends(
                rstate.random())

            assigned_backends.add(chosen_backend)
            backend_queues[chosen_backend].add(query)
            if len(shadow_backends) > 0:
                shadow_query = copy.copy(query)
                shadow_query.async_future = None
                shadow_query.is_shadow_query = True
                for shadow_backend in shadow_backends:
                    assigned_backends.add(shadow_backend)
                    backend_queues[shadow_backend].add(shadow_query)

        return assigned_backends
