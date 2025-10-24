"""Types conversion between different backends."""

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any, List, Optional, Tuple

from numpy import int32

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
    # `pygloo` is deprecated. Use gloo through torch.distributed for both
    # `GLOO` and `TORCH_GLOO`.
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
        tensor_device: The device of the tensor. Currently, we require all tensors in the
        list have the same device type.
    """

    tensor_meta: List[Tuple["torch.Size", "torch.dtype"]]
    tensor_device: Optional["torch.device"] = None


@dataclass
class NixlTransportMetadata(TensorTransportMetadata):
    """Metadata for tensors stored in the GPU object store for NIXL transport.

    Args:
        nixl_serialized_descs: Serialized tensor descriptors for NIXL transport.
        nixl_agent_meta: The additional metadata of the remote NIXL agent.
    """

    nixl_reg_descs: Optional[Any] = None
    nixl_serialized_descs: Optional[bytes] = None
    nixl_agent_meta: Optional[bytes] = None


@dataclass
class CollectiveTransportMetadata(TensorTransportMetadata):
    """Metadata for tensors stored in the GPU object store for collective transport."""


@dataclass
class CommunicatorMetadata:
    """Metadata for the communicator.

    Args:
        communicator_name: The name of the communicator.
    """

    communicator_name: str = ""


@dataclass
class CollectiveCommunicatorMetadata(CommunicatorMetadata):
    """Metadata for the collective communicator (e.g. NCCL, GLOO).

    Args:
        src_rank: The rank of the source actor.
        dst_rank: The rank of the destination actor.
    """

    src_rank: Optional[int32] = None
    dst_rank: Optional[int32] = None


@dataclass
class NixlCommunicatorMetadata(CommunicatorMetadata):
    """Metadata for the NIXL communicator."""


class ReduceOp(Enum):
    SUM = 0
    PRODUCT = 1
    MIN = 2
    MAX = 3


unset_timeout_ms = timedelta(milliseconds=-1)

# This is used to identify the collective group for NIXL.
NIXL_GROUP_NAME = "ray_internal_nixl_group"


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
