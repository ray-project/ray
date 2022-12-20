from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Union, Iterator

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

    def stats(self) -> str:
        return NotImplementedError
