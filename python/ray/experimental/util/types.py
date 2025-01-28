from enum import Enum

from ray.util.annotations import PublicAPI


class _NcclOpType(Enum):
    pass


@PublicAPI
class P2POp(_NcclOpType):
    SEND = 0
    RECV = 1


class _CollectiveOp(_NcclOpType):
    pass


@PublicAPI
class ReduceOp(_CollectiveOp):
    SUM = 0
    PRODUCT = 1
    MAX = 2
    MIN = 3
    AVG = 4

    def __str__(self):
        return f"{self.name.lower()}"
