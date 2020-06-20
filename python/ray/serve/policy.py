from abc import ABCMeta, abstractmethod
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

    def __init__(self, traffic_dict):
        self.backend_names, self.backend_weights = zip(
            *sorted(traffic_dict.items()))

    def flush(self, endpoint_queue, backend_queues):
        if len(self.backend_names) == 0:
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

            chosen_backend = rstate.choice(
                self.backend_names, replace=False,
                p=self.backend_weights).squeeze()

            assigned_backends.add(chosen_backend)
            backend_queues[chosen_backend].add(query)

        return assigned_backends
