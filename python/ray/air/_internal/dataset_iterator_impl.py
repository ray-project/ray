from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Union, Iterator

# if TYPE_CHECKING:
#    from ray.data import Dataset

from ray.air import DatasetIterator
from ray.air.data_batch_type import DataBatchType


class BulkDatasetIterator(DatasetIterator):
    def __init__(
        self,
        # base_dataset: Dataset,
        base_dataset,
        per_epoch_preprocessor: Optional["Preprocessor"] = None,
    ):
        self._base_dataset = base_dataset
        self._epoch = 0
        self._per_epoch_preprocessor = per_epoch_preprocessor

    def iter_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: Optional[int] = 256,
        batch_format: str = "default",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterator[DataBatchType]:
        self._epoch += 1

        ds = self._base_dataset
        if self._per_epoch_preprocessor is not None:
            ds = self._per_epoch_preprocessor.transform(ds)

        return ds.iter_batches(
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            batch_format=batch_format,
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        )

    def stats(self) -> str:
        return self._base_dataset.stats()


class PipelinedDatasetIterator(DatasetIterator):
    def __init__(
        self,
        # base_dataset: Dataset,
        base_dataset_pipeline,
    ):
        self._base_dataset_pipeline = base_dataset_pipeline
        self._epoch_iterator = None
        self._epoch = 0

    def iter_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: Optional[int] = 256,
        batch_format: str = "default",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterator[DataBatchType]:
        self._epoch += 1
        if self._epoch_iterator is None:
            self._epoch_iterator = self._base_dataset_pipeline.iter_epochs()
        ds = next(self._epoch_iterator)
        return ds.iter_batches(
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            batch_format=batch_format,
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        )

    def stats(self) -> str:
        return self._base_dataset_pipeline.stats()
