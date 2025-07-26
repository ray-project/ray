"""Types conversion between different backends."""

from enum import Enum
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional, List, Tuple, TYPE_CHECKING

_NUMPY_AVAILABLE = True
_TORCH_AVAILABLE = True
_CUPY_AVAILABLE = True

if TYPE_CHECKING:
    import torch

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

    NCCL = "nccl"
    MPI = "mpi"
    GLOO = "gloo"
    # Use gloo through torch.distributed.
    TORCH_GLOO = "torch_gloo"
    NIXL = "nixl"
    UNRECOGNIZED = "unrecognized"

    def __new__(cls, name: str):
        backend = getattr(Backend, name.upper(), Backend.UNRECOGNIZED)
        if backend == Backend.UNRECOGNIZED:
            raise ValueError(
                "Unrecognized backend: '{}'. Only NCCL is supported".format(name)
            )
        if backend == Backend.MPI:
            raise RuntimeError("Ray does not support MPI backend.")
        return backend


@dataclass
class TensorTransportMetadata:
    """Metadata for tensors stored in the GPU object store.

    Args:
        tensor_meta: A list of tuples, each containing the shape and dtype of a tensor.
        src_rank: The source rank that the tensor is being transported from. It's
            used in non-NIXL transport.
        nixl_serialized_descs: Serialized tensor descriptors for NIXL transport.
        nixl_agent_meta: The additional metadata of the remote NIXL agent.
    """

    tensor_meta: List[Tuple["torch.Size", "torch.dtype"]]
    src_rank: Optional[int] = None
    nixl_serialized_descs: Optional[bytes] = None
    nixl_agent_meta: Optional[bytes] = None


class ReduceOp(Enum):
    SUM = 0
    PRODUCT = 1
    MIN = 2
    MAX = 3


unset_timeout_ms = timedelta(milliseconds=-1)


@dataclass
class AllReduceOptions:
    reduceOp = ReduceOp.SUM
    timeout_ms = unset_timeout_ms


@dataclass
class BarrierOptions:
    timeout_ms = unset_timeout_ms


@dataclass
class ReduceOptions:
    reduceOp = ReduceOp.SUM
    root_rank = 0
    root_tensor = 0  # index for multi-gpu reduce operations
    timeout_ms = unset_timeout_ms


@dataclass
class AllGatherOptions:
    timeout_ms = unset_timeout_ms


#
# @dataclass
# class GatherOptions:
#     root_rank = 0
#     timeout = unset_timeout


@dataclass
class BroadcastOptions:
    root_rank = 0
    root_tensor = 0
    timeout_ms = unset_timeout_ms


@dataclass
class ReduceScatterOptions:
    reduceOp = ReduceOp.SUM
    timeout_ms = unset_timeout_ms


@dataclass
class SendOptions:
    dst_rank = 0
    dst_gpu_index = 0
    n_elements = 0
    timeout_ms = unset_timeout_ms


@dataclass
class RecvOptions:
    src_rank = 0
    src_gpu_index = 0
    n_elements = 0
    unset_timeout_ms = unset_timeout_ms
