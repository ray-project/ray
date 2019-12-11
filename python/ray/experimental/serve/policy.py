from enum import Enum
from ray.experimental.serve.queues import RoundRobinPolicyQueueActor,RandomPolicyQueueActor
class Policy(Enum):
    """Policy constants for centralized router"""
    random = RandomPolicyQueueActor
    roundRobin = RoundRobinPolicyQueueActor