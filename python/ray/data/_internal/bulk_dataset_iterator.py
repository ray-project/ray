from typing import TYPE_CHECKING, Dict, List, Optional, Union, Iterator
import warnings

from ray.data.block import DataBatch
from ray.data.dataset_iterator import DatasetIterator

if TYPE_CHECKING:
    import tensorflow as tf
    import torch
    from ray.data import Dataset
    from ray.data._internal.torch_iterable_dataset import TorchTensorBatchType


class BulkDatasetIterator(DatasetIterator):
    def __init__(
        self,
        base_dataset: "Dataset",
    ):
        self._base_dataset = base_dataset

    def __repr__(self) -> str:
        return f"DatasetIterator({self._base_dataset})"

    def iter_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: Optional[int] = 256,
        batch_format: str = "default",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterator[DataBatch]:
        # TODO(swang): Delegate Dataset.iter_batches to
        # DatasetIterator.iter_batches instead of the other way around.
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
    ) -> Iterator["TorchTensorBatchType"]:
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

    def _with_backward_compat(self) -> DatasetIterator:
        return BulkDatasetIteratorWithBackwardCompat(self)


class BulkDatasetIteratorWithBackwardCompat(BulkDatasetIterator):
    def __init__(
        self,
        dataset_iterator: BulkDatasetIterator,
    ):
        self._dataset_iterator = dataset_iterator

    def __getattr__(self, name):
        if name == "_dataset_iterator":
            raise AttributeError

        if hasattr(self._dataset_iterator, name):
            return getattr(self._dataset_iterator, name)

        warnings.warn(
            "session.get_dataset_shard returns a ray.data.DatasetIterator "
            "instead of a Dataset as of Ray v2.3. "
            "Use iter_torch_batches(), to_tf(), or iter_batches() to "
            "iterate over one epoch. See "
            "https://docs.ray.io/en/latest/data/api/dataset_iterator.html "
            "for full DatasetIterator docs."
        )

        return getattr(self._dataset_iterator._base_dataset, name)
