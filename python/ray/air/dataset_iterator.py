from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Union, Iterator

#if TYPE_CHECKING:
#    from ray.data import Dataset
from ray.air.data_batch_type import DataBatchType


class DatasetIterator:

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
        raise NotImplementedError

    def count(self) -> int:
        # Should we support this?
        raise NotImplementedError


# TODO(swang): Move to internal.
class _DatasetIterator(DatasetIterator):
    def __init__(
            self,
            #base_dataset: Dataset,
            base_dataset,
            ):
        self._base_dataset = base_dataset
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
        return self._base_dataset.iter_batches(
                prefetch_blocks=prefetch_blocks,
                batch_size=batch_size,
                batch_format=batch_format,
                drop_last=drop_last,
                local_shuffle_buffer_size=local_shuffle_buffer_size,
                local_shuffle_seed=local_shuffle_seed)

    def count(self) -> int:
        # TODO(swang): We could cache this for datasets whose size does not
        # change.
        return self._base_dataset.count()
