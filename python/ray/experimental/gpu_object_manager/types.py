from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional, Tuple

if TYPE_CHECKING:
    import torch


@dataclass
class CommunicatorMetadata:
    """Metadata for the communicator."""


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
