from typing import TYPE_CHECKING, Optional, Union, Iterator, Callable, Any
import time
import warnings

from ray.data.block import DataBatch
from ray.data.context import DatasetContext
from ray.data.dataset_iterator import DatasetIterator
from ray.data._internal.block_batching import batch_block_refs

if TYPE_CHECKING:
    import pyarrow
    from ray.data import Dataset


class DatasetIteratorImpl(DatasetIterator):
    def __init__(
        self,
        base_dataset: "Dataset",
    ):
        self._base_dataset = base_dataset
        self._base_context = DatasetContext.get_current()

    def __repr__(self) -> str:
        return f"DatasetIterator({self._base_dataset})"

    def iter_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: Optional[int] = 256,
        batch_format: Optional[str] = "default",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
        _collate_fn: Optional[Callable[[DataBatch], Any]] = None,
    ) -> Iterator[DataBatch]:

        DatasetContext._set_current(self._base_context)

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

    def __getattr__(self, name):
        if name == "_base_dataset":
            raise AttributeError()

        if hasattr(self._base_dataset, name) and not name.startswith("_"):
            # Warning for backwards compatibility. TODO: remove this method in 2.5.
            warnings.warn(
                "session.get_dataset_shard returns a ray.data.DatasetIterator "
                "instead of a Dataset/DatasetPipeline as of Ray v2.3. "
                "Use iter_torch_batches(), to_tf(), or iter_batches() to "
                "iterate over one epoch. See "
                "https://docs.ray.io/en/latest/data/api/dataset_iterator.html "
                "for full DatasetIterator docs.",
                stacklevel=4,
            )

            return getattr(self._base_dataset, name)

        raise AttributeError()
