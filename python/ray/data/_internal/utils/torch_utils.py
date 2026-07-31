from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any, Optional, Union

import torch

from ray.data._internal.block_batching.interfaces import FinalizedData
from ray.data.collate_fn import (
    TensorBatchType,
    is_tensor_batch_type,
)
from ray.data.util.torch_utils import (
    DEFAULT_TENSOR_NON_BLOCKING_TRANSFER,
    move_tensors_to_device,
)

CustomBatchType = Any


class FinalizeFn(ABC):
    """Base finalize_fn interface for ``iter_torch_batches``."""

    @abstractmethod
    def __call__(self, batch: Union[TensorBatchType, CustomBatchType]) -> FinalizedData:
        ...


class DefaultFinalizeFn(FinalizeFn):
    """Default finalize_fn for ``iter_torch_batches``.

    Move tensor batches to the target device on the current stream (can be non-blocking).
    """

    def __init__(self, device: torch.device):
        """Construct the DefaultFinalizeFn.

        Args:
            device: The device to transfer tensor batches to.
        """
        self._device = device

    @torch.no_grad()
    def __call__(self, batch: Union[TensorBatchType, CustomBatchType]) -> FinalizedData:
        if is_tensor_batch_type(batch):
            batch = move_tensors_to_device(
                batch,
                device=self._device,
                non_blocking=DEFAULT_TENSOR_NON_BLOCKING_TRANSFER,
            )
        return FinalizedData(data=batch)


def _iter_tensors(batch: TensorBatchType):
    """Yield every tensor in a TensorBatchType, recursively."""
    if isinstance(batch, torch.Tensor):
        yield batch
    elif isinstance(batch, Mapping):
        for value in batch.values():
            yield from _iter_tensors(value)
    elif isinstance(batch, (list, tuple)):
        for value in batch:
            yield from _iter_tensors(value)


def find_tensor_off_device(
    batch: TensorBatchType, device: torch.device
) -> Optional[torch.Tensor]:
    """Return the first tensor in ``batch`` not on ``device``, else None.

    A ``device`` without an index (e.g. plain ``cuda``) matches any index of
    that device type.
    """
    for tensor in _iter_tensors(batch):
        if tensor.device.type != device.type or (
            device.index is not None and tensor.device.index != device.index
        ):
            return tensor
    return None
