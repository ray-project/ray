import abc
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Union, Iterator
from typing_extensions import Literal

from ray.air.data_batch_type import DataBatchType


class DatasetIterator(abc.ABC):
    @abc.abstractmethod
    def iter_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: int = 256,
        batch_format: Literal["default", "numpy", "pandas"] = "default",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterator[DataBatchType]:
        raise NotImplementedError

    @abc.abstractmethod
    def stats(self) -> str:
        return NotImplementedError
