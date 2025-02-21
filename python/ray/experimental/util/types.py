from enum import Enum
from dataclasses import dataclass


class _CollectiveOp:
    pass


class ReduceOp(Enum):
    SUM = 0
    PRODUCT = 1
    MAX = 2
    MIN = 3
    AVG = 4


@dataclass
class AllGatherOp(_CollectiveOp):
    pass


@dataclass
class AllReduceOp(_CollectiveOp):
    reduceOp: ReduceOp = ReduceOp.SUM


@dataclass
class ReduceScatterOp(_CollectiveOp):
    reduceOp: ReduceOp = ReduceOp.SUM
