from enum import Enum
class Policy(Enum):
    """Policy constants for centralized router"""
    random = 'random'
    roundRobin = 'roundRobin'