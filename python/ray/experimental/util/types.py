from enum import Enum

from ray.util.annotations import PublicAPI


class _CollectiveOp(Enum):
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


@PublicAPI(stability="alpha")
class Device(Enum):
    DEFAULT = "default"
    CPU = "cpu"
    GPU = "gpu"
    CUDA = "cuda"

    def __str__(self):
        return self.value
