from typing import TYPE_CHECKING, Dict, Optional, Union, Iterator

from ray.data.block import DataBatch
from ray.data.dataset_iterator import DatasetIterator

if TYPE_CHECKING:
    import torch
    from ray.data import Dataset
    from ray.data._internal.torch_iterable_dataset import TorchTensorBatchType
    from ray.train._internal.dataset_iterator import TrainDatasetIterator


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

    def stats(self) -> str:
        return self._base_dataset.stats()

    @property
    def _base_dataset_or_pipeline(self) -> "Dataset":
        return self._base_dataset

    def _to_train_iterator(self) -> "TrainDatasetIterator":
        from ray.train._internal.dataset_iterator import TrainDatasetIterator

        return TrainDatasetIterator(self)
