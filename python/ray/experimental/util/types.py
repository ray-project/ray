from enum import Enum
from dataclasses import dataclass


class ReduceOp(Enum):
    SUM = 0
    PRODUCT = 1
    MAX = 2
    MIN = 3
    AVG = 4


@dataclass
class AllGatherOp:
    pass


@dataclass
class AllReduceOp:
    reduceOp: ReduceOp = ReduceOp.SUM


@dataclass
class ReduceScatterOp:
    reduceOp: ReduceOp = ReduceOp.SUM
