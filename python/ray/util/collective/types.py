"""Types conversion between different backends."""

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING

_NUMPY_AVAILABLE = True
_TORCH_AVAILABLE = True
_CUPY_AVAILABLE = True

if TYPE_CHECKING:
    pass

try:
    import torch as th  # noqa: F401
except ImportError:
    _TORCH_AVAILABLE = False

try:
    import cupy as cp  # noqa: F401
except ImportError:
    _CUPY_AVAILABLE = False


def cupy_available():
    return _CUPY_AVAILABLE


def torch_available():
    return _TORCH_AVAILABLE


class Backend(object):
    """A class to represent different backends."""

    NCCL = "NCCL"
    GLOO = "GLOO"
    UNRECOGNIZED = "unrecognized"

    def __new__(cls, name: str):
        upper_name = name.upper()
        backend = getattr(Backend, upper_name, Backend.UNRECOGNIZED)
        if backend == Backend.UNRECOGNIZED:
            if upper_name == "TORCH_GLOO":
                return Backend.GLOO
            raise ValueError(
                "Unrecognized backend: '{}'. Only NCCL and GLOO are supported".format(
                    name
                )
            )
        return backend


class ReduceOp(Enum):
    SUM = 0
    PRODUCT = 1
    MIN = 2
    MAX = 3


unset_timeout_ms = timedelta(milliseconds=-1)


@dataclass
class AllReduceOptions:
    reduceOp: ReduceOp = ReduceOp.SUM
    timeout_ms: timedelta = unset_timeout_ms


@dataclass
class BarrierOptions:
    timeout_ms: timedelta = unset_timeout_ms


@dataclass
class ReduceOptions:
    reduceOp: ReduceOp = ReduceOp.SUM
    root_rank: int = 0
    root_tensor: int = 0  # index for multi-gpu reduce operations
    timeout_ms: timedelta = unset_timeout_ms


@dataclass
class AllGatherOptions:
    timeout_ms: timedelta = unset_timeout_ms


#
# @dataclass
# class GatherOptions:
#     root_rank = 0
#     timeout = unset_timeout


@dataclass
class BroadcastOptions:
    root_rank: int = 0
    root_tensor: int = 0
    timeout_ms: timedelta = unset_timeout_ms


@dataclass
class ReduceScatterOptions:
    reduceOp: ReduceOp = ReduceOp.SUM
    timeout_ms: timedelta = unset_timeout_ms


@dataclass
class SendOptions:
    dst_rank: int = 0
    dst_gpu_index: int = 0
    n_elements: int = 0
    timeout_ms: timedelta = unset_timeout_ms


@dataclass
class RecvOptions:
    src_rank: int = 0
    src_gpu_index: int = 0
    n_elements: int = 0
    timeout_ms: timedelta = unset_timeout_ms
