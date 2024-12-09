from enum import Enum

from ray.util.annotations import PublicAPI


class _CollectiveOpType(Enum):
    pass


@PublicAPI
class ReduceOp(_CollectiveOpType):
    SUM = 0
    PRODUCT = 1
    MAX = 2
    MIN = 3
    AVG = 4

    def __str__(self):
        return f"{self.name.lower()}"
