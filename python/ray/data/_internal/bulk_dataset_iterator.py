import numpy as np
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Union, Iterator

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

        ds = self._base_dataset
        block_iterator, stats, executor = ds._plan.execute_to_iterator()
        ds._current_executor = executor
        time_start = time.perf_counter()

        yield from batch_block_refs(
            block_iterator,
            stats=stats,
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            batch_format=batch_format,
            drop_last=drop_last,
            collate_fn=_collate_fn,
            shuffle_buffer_min_size=local_shuffle_buffer_size,
            shuffle_seed=local_shuffle_seed,
        )

        stats.iter_total_s.add(time.perf_counter() - time_start)

    def stats(self) -> str:
        return self._base_dataset.stats()

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        return self._base_dataset.schema()
