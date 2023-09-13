from typing import TYPE_CHECKING, Iterator, Optional, Tuple, Union

from ray.data._internal.stats import DatasetStats
from ray.data.block import Block, BlockMetadata
from ray.data.iterator import DataIterator
from ray.types import ObjectRef

if TYPE_CHECKING:
    import pyarrow

    from ray.data import Dataset


class DataIteratorImpl(DataIterator):
    def __init__(
        self,
        base_dataset: "Dataset",
    ):
        self._base_dataset = base_dataset

    def __repr__(self) -> str:
        return f"DataIterator({self._base_dataset})"

    def _to_block_iterator(
        self,
    ) -> Tuple[
        Iterator[Tuple[ObjectRef[Block], BlockMetadata]],
        Optional[DatasetStats],
        bool,
    ]:
        ds = self._base_dataset
        block_iterator, stats, executor = ds._plan.execute_to_iterator()
        ds._current_executor = executor
        return block_iterator, stats, False

    def stats(self) -> str:
        return self._base_dataset.stats()

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        return self._base_dataset.schema()

    def __getattr__(self, name):
        if name == "_base_dataset":
            raise AttributeError()

        if hasattr(self._base_dataset, name) and not name.startswith("_"):
            # Raise error for backwards compatibility.
            # TODO: remove this method in 2.6.
            raise DeprecationWarning(
                "ray.train.get_dataset_shard returns a ray.data.DataIterator "
                "instead of a Dataset/DatasetPipeline as of Ray v2.3. "
                "Use iter_torch_batches(), to_tf(), or iter_batches() to "
                "iterate over one epoch. See "
                "https://docs.ray.io/en/latest/data/api/dataset_iterator.html "
                "for full DataIterator docs.",
            )

        raise AttributeError()
