from enum import Enum
from ray.experimental.serve.queues import (
    RoundRobinPolicyQueueActor, RandomPolicyQueueActor,
    PowerOfTwoPolicyQueueActor, FixedPackingPolicyQueueActor)


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
