from abc import ABC, abstractmethod
from typing import Any, Union

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
