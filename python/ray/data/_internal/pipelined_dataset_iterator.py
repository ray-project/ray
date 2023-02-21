import numpy as np
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Union, Iterator

from ray.data.block import DataBatch
from ray.data.dataset_iterator import DatasetIterator

if TYPE_CHECKING:
    import torch
    from ray.data import DatasetPipeline
    from ray.data._internal.torch_iterable_dataset import TorchTensorBatchType
    from ray.train._internal.dataset_iterator import TrainDatasetIterator


class PipelinedDatasetIterator(DatasetIterator):
    def __init__(
        self,
        base_dataset_pipeline: "DatasetPipeline",
    ):
        self._base_dataset_pipeline = base_dataset_pipeline
        self._epoch_iterator = None

    def __repr__(self) -> str:
        return f"DatasetIterator({self._base_dataset_pipeline})"

    def _get_next_dataset(self) -> "DatasetPipeline":
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
        collate_fn: Optional[
            Callable[[Union[np.ndarray, Dict[str, np.ndarray]]], Any]
        ] = None,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterator["TorchTensorBatchType"]:

        ds = self._get_next_dataset()
        return ds.iter_torch_batches(
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            dtypes=dtypes,
            device=device,
            drop_last=drop_last,
            collate_fn=collate_fn,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        )

    def stats(self) -> str:
        return self._base_dataset_pipeline.stats()

    @property
    def _base_dataset_or_pipeline(self) -> "DatasetPipeline":
        return self._base_dataset_pipeline

    def _to_train_iterator(self) -> "TrainDatasetIterator":
        from ray.train._internal.dataset_iterator import TrainDatasetIterator

        return TrainDatasetIterator(self)
