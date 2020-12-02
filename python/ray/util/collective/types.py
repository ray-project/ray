"""Types conversion between different backends."""
from enum import Enum
from datetime import timedelta

_NUMPY_AVAILABLE = True
_TORCH_AVAILABLE = True
_CUPY_AVAILABLE = True

try:
    import numpy
except ImportError:
    _NUMPY_AVAILABLE = False

try:
    import torch
except ImportError:
    _TORCH_AVAILABLE = False

try:
    import cupy
except ImportError:
    _CUPY_AVAILABLE = False


def numpy_available():
    return _NUMPY_AVAILABLE


def cupy_available():
    return _CUPY_AVAILABLE


def torch_available():
    return _TORCH_AVAILABLE


# TODO(Hao): extend this to support more MPI types
class ReduceOp(Enum):
    SUM = 0
    PRODUCT = 1
    MIN = 2
    MAX = 3


unset_timeout = timedelta(milliseconds=-1)


class AllReduceOptions:
    reduceOp = ReduceOp.SUM
    timeout = unset_timeout


class BarrierOptions:
    timeout = unset_timeout
