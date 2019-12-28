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

    Args:
        Random(RandomPolicyQueue): Random policy for backend selection
        RoundRobin(RoundRobinPolicyQueue): Round Robin policy for
            backend selection
        PowerOfTwo(PowerOfTwoPolicyQueue): Power of two policy for
            backend selection
        FixedPacking(FixedPackingPolicyQueue): Fixed packing policy for
            backend selection
    """
    Random = RandomPolicyQueueActor
    RoundRobin = RoundRobinPolicyQueueActor
    PowerOfTwo = PowerOfTwoPolicyQueueActor
    FixedPacking = FixedPackingPolicyQueueActor
