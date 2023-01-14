from typing import TYPE_CHECKING, Dict, Union

from torch.utils.data import IterableDataset

if TYPE_CHECKING:
    import torch


TorchTensorBatchType = Union["torch.Tensor", Dict[str, "torch.Tensor"]]


class TorchIterableDataset(IterableDataset):
    def __init__(self, generator_func):
        self.generator_func = generator_func

    def __iter__(self):
        it = self.generator_func()
        yield from it
