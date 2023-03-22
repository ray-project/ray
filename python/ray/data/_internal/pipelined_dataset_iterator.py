from typing import TYPE_CHECKING, Any, Callable, Optional, Union, Iterator
import warnings

from ray.data.block import DataBatch
from ray.data.dataset_iterator import DatasetIterator

if TYPE_CHECKING:
    import pyarrow
    from ray.data import DatasetPipeline


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
        batch_format: Optional[str] = "default",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
        _collate_fn: Optional[Callable[[DataBatch], Any]] = None,
    ) -> Iterator[DataBatch]:
        ds = self._get_next_dataset()
        return ds.iter_batches(
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            batch_format=batch_format,
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
            _collate_fn=_collate_fn,
        )

    def stats(self) -> str:
        return self._base_dataset_pipeline.stats()

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        return self._base_dataset_pipeline.schema()

    def __getattr__(self, name):
        if name == "_base_dataset_pipeline":
            raise AttributeError

        if hasattr(self._base_dataset_pipeline, name) and not name.startswith("_"):
            # Warning for backwards compatibility. TODO: remove this method in 2.5.
            warnings.warn(
                "session.get_dataset_shard returns a ray.data.DatasetIterator "
                "instead of a Dataset/DatasetPipeline as of Ray v2.3. "
                "Use iter_torch_batches(), to_tf(), or iter_batches() to "
                "iterate over one epoch. See "
                "https://docs.ray.io/en/latest/data/api/dataset_iterator.html "
                "for full DatasetIterator docs."
            )

            return getattr(self._base_dataset_pipeline, name)
        else:
            return super().__getattr__(name)
