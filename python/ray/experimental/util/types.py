from enum import Enum

from ray.util.annotations import DeveloperAPI


class _CollectiveOp(Enum):
    pass


@DeveloperAPI
class ReduceOp(_CollectiveOp):
    SUM = 0
    PRODUCT = 1
    MAX = 2
    MIN = 3
    AVG = 4

    def __str__(self):
        return f"{self.name.lower()}"
