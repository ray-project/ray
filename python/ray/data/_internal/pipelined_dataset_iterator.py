from typing import TYPE_CHECKING, Dict, List, Optional, Union, Iterator
import warnings

from ray.data import Dataset
from ray.data.block import DataBatch
from ray.data.dataset_iterator import DatasetIterator

if TYPE_CHECKING:
    import tensorflow as tf
    import torch
    from ray.data import DatasetPipeline
    from ray.data._internal.torch_iterable_dataset import TorchTensorBatchType


class PipelinedDatasetIterator(DatasetIterator):
    def __init__(
        self,
        base_dataset_pipeline: "DatasetPipeline",
    ):
        self._base_dataset_pipeline = base_dataset_pipeline
        self._epoch_iterator = None

    def __repr__(self) -> str:
        return f"DatasetIterator({self._base_dataset_pipeline})"

    def _get_next_dataset(self) -> "Dataset":
        if self._epoch_iterator is None:
            self._epoch_iterator = self._base_dataset_pipeline.iter_epochs()
        ds = next(self._epoch_iterator)
        return ds

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
        ds = self._get_next_dataset()
        return ds.iter_batches(
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
        from ray.air._internal.torch_utils import (
            convert_ndarray_batch_to_torch_tensor_batch,
        )

        ds = self._get_next_dataset()
        for batch in ds.iter_batches(
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            batch_format="numpy",
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        ):
            yield convert_ndarray_batch_to_torch_tensor_batch(
                batch,
                dtypes=dtypes,
                device=device,
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
        ds = self._get_next_dataset()
        return Dataset.to_tf(
            ds,
            feature_columns,
            label_columns,
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        )

    def stats(self) -> str:
        return self._base_dataset_pipeline.stats()

    def _with_backward_compat(self) -> DatasetIterator:
        return PipelinedDatasetIteratorWithBackwardCompat(self)


class PipelinedDatasetIteratorWithBackwardCompat(PipelinedDatasetIterator):
    def __init__(
        self,
        dataset_iterator: PipelinedDatasetIterator,
    ):
        self._dataset_iterator = dataset_iterator

    def __getattr__(self, name):
        if name == "_dataset_iterator":
            raise AttributeError

        if hasattr(self._dataset_iterator, name):
            return getattr(self._dataset_iterator, name)

        warnings.warn(
            "session.get_dataset_shard returns a ray.data.DatasetIterator "
            "instead of a DatasetPipeline as of Ray v2.3. "
            "Use iter_torch_batches(), to_tf(), or iter_batches() to "
            "iterate over one epoch. See "
            "https://docs.ray.io/en/latest/data/api/dataset_iterator.html "
            "for full DatasetIterator docs."
        )

        return getattr(self._dataset_iterator._base_dataset_pipeline, name)
