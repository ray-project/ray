from typing import TYPE_CHECKING, Iterator, Optional, Tuple, Union

from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.stats import DatasetStats
from ray.data.context import DataContext
from ray.data.iterator import DataIterator

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

    def _to_ref_bundle_iterator(
        self,
    ) -> Tuple[Iterator[RefBundle], Optional[DatasetStats], bool]:
        ref_bundles_iterator, stats = self._base_dataset._execute_to_iterator()
        return ref_bundles_iterator, stats, False

    def stats(self) -> str:
        return self._base_dataset.stats()

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        return self._base_dataset.schema()

    def get_context(self) -> DataContext:
        return self._base_dataset.context

    def _get_dataset_tag(self):
        return self._base_dataset.get_dataset_id()
