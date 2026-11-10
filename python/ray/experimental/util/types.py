from dataclasses import dataclass
from enum import Enum

from ray.util.annotations import PublicAPI


class _CollectiveOp:
    pass


@PublicAPI
class ReduceOp(Enum):
    """The reduction that a compiled graph allreduce or reduce-scatter applies."""

    SUM = 0
    PRODUCT = 1
    MAX = 2
    MIN = 3
    AVG = 4


@PublicAPI
@dataclass
class AllGatherOp(_CollectiveOp):
    """A compiled graph allgather operation."""

    pass


@PublicAPI
@dataclass
class AllReduceOp(_CollectiveOp):
    """A compiled graph allreduce operation that applies ``reduceOp``."""

    reduceOp: ReduceOp = ReduceOp.SUM


@PublicAPI
@dataclass
class ReduceScatterOp(_CollectiveOp):
    """A compiled graph reduce-scatter operation that applies ``reduceOp``."""

    reduceOp: ReduceOp = ReduceOp.SUM


@PublicAPI(stability="alpha")
class Device(Enum):
    """The target device for transporting a tensor in a compiled graph.

    ``DEFAULT`` keeps the sender's device type. ``CPU`` moves the tensor to the
    CPU on the receiver. ``GPU`` and ``CUDA`` move it to the GPU on the
    receiver.
    """

    DEFAULT = "default"
    CPU = "cpu"
    GPU = "gpu"
    CUDA = "cuda"

    def __str__(self):
        return self.value
