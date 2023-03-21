from typing import TYPE_CHECKING, Union, Iterator, Optional, Tuple
import warnings
import sys

from ray.types import ObjectRef
from ray.data._internal.util import _default_batch_format
from ray.data._internal.stats import DatasetStats
from ray.data.block import Block, BlockMetadata
from ray.data.dataset_iterator import DatasetIterator

if TYPE_CHECKING:
    import pyarrow
    from ray.data import Dataset

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


class DatasetIteratorImpl(DatasetIterator):
    def __init__(
        self,
        base_dataset: "Dataset",
    ):
        self._base_dataset = base_dataset

    def __repr__(self) -> str:
        return f"DatasetIterator({self._base_dataset})"

    def _to_block_iterator(
        self,
    ) -> Tuple[
        Iterator[Tuple[ObjectRef[Block], BlockMetadata]], Optional[DatasetStats]
    ]:
        ds = self._base_dataset
        block_iterator, stats, executor = ds._plan.execute_to_iterator()
        ds._current_executor = executor
        return block_iterator, stats

    def stats(self) -> str:
        return self._base_dataset.stats()

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        return self._base_dataset.schema()

    def _default_batch_format(self) -> Literal["default", "pandas", "pyarrow", "numpy"]:
        return _default_batch_format(self._base_dataset)

    def __getattr__(self, name):
        if name == "_base_dataset":
            raise AttributeError()

        if hasattr(self._base_dataset, name):
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
        else:
            return super().__getattr__(name)
