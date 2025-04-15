from dataclasses import dataclass
from enum import Enum

from ray.util.annotations import PublicAPI


class _CollectiveOp:
    pass


@PublicAPI
class ReduceOp(Enum):
    SUM = 0
    PRODUCT = 1
    MAX = 2
    MIN = 3
    AVG = 4


@PublicAPI
@dataclass
class AllGatherOp(_CollectiveOp):
    pass


@PublicAPI
@dataclass
class AllReduceOp(_CollectiveOp):
    reduceOp: ReduceOp = ReduceOp.SUM


@PublicAPI
@dataclass
class ReduceScatterOp(_CollectiveOp):
    reduceOp: ReduceOp = ReduceOp.SUM


@PublicAPI(stability="alpha")
class Device(Enum):
    DEFAULT = "default"
    CPU = "cpu"
    GPU = "gpu"
    CUDA = "cuda"

    def __str__(self):
        return self.value
