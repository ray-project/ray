from typing import TYPE_CHECKING, Dict, List, Optional, Union, Iterator

from ray.data.dataset_iterator import DatasetIterator
from ray.data._internal.block_batching import BatchType
from ray.data._internal.torch_iterable_dataset import TorchTensorBatchType

if TYPE_CHECKING:
    import tf
    import torch
    from ray.data import Dataset


class BulkDatasetIterator(DatasetIterator):
    def __init__(
        self,
        base_dataset: "Dataset",
    ):
        self._base_dataset = base_dataset

    def iter_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: Optional[int] = 256,
        batch_format: str = "default",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterator[BatchType]:
        return self._base_dataset.iter_batches(
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            batch_format=batch_format,
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        )

    def iter_torch_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: Optional[int] = 256,
        dtypes: Optional[Union["torch.dtype", Dict[str, "torch.dtype"]]] = None,
        device: Optional[str] = None,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterator[TorchTensorBatchType]:
        return self._base_dataset.iter_torch_batches(
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            dtypes=dtypes,
            device=device,
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        )

    def to_tf(
        self,
        feature_columns: Union[str, List[str]],
        label_columns: Union[str, List[str]],
        *,
        prefetch_blocks: int = 0,
        batch_size: int = 1,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> "tf.data.Dataset":
        return self._base_dataset.to_tf(
            feature_columns,
            label_columns,
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        )

    def stats(self) -> str:
        return self._base_dataset.stats()
