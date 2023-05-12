from typing import TYPE_CHECKING, Optional, Union, Iterator, Tuple

from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.data.iterator import DataIterator
from ray.data._internal.stats import DatastreamStats

if TYPE_CHECKING:
    import pyarrow
    from ray.data import Datastream


class DataIteratorImpl(DataIterator):
    def __init__(
        self,
        base_datastream: "Datastream",
    ):
        self._base_datastream = base_datastream
        self._base_context = DataContext.get_current()

    def __repr__(self) -> str:
        return f"DataIterator({self._base_datastream})"

    def _to_block_iterator(
        self,
    ) -> Tuple[
        Iterator[Tuple[ObjectRef[Block], BlockMetadata]],
        Optional[DatastreamStats],
        bool,
    ]:
        ds = self._base_datastream
        block_iterator, stats, executor = ds._plan.execute_to_iterator()
        ds._current_executor = executor
        return block_iterator, stats, False

    def stats(self) -> str:
        return self._base_datastream.stats()

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        return self._base_datastream.schema()

    def __getattr__(self, name):
        if name == "_base_datastream":
            raise AttributeError()

        if hasattr(self._base_datastream, name) and not name.startswith("_"):
            # Raise error for backwards compatibility.
            # TODO: remove this method in 2.6.
            raise DeprecationWarning(
                "session.get_dataset_shard returns a ray.data.DataIterator "
                "instead of a Datastream/DatasetPipeline as of Ray v2.3. "
                "Use iter_torch_batches(), to_tf(), or iter_batches() to "
                "iterate over one epoch. See "
                "https://docs.ray.io/en/latest/data/api/dataset_iterator.html "
                "for full DataIterator docs.",
            )

        raise AttributeError()
