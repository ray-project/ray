from typing import TYPE_CHECKING, Iterator, Optional, Tuple, Union

from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.stats import DatasetStats
from ray.data._internal.util import create_dataset_tag
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
        ds = self._base_dataset
        ref_bundles_iterator, stats, executor = ds._plan.execute_to_iterator()
        ds._current_executor = executor
        return ref_bundles_iterator, stats, False

    def stats(self) -> str:
        return self._base_dataset.stats()

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        return self._base_dataset.schema()

    def _get_dataset_tag(self):
        return create_dataset_tag(
            self._base_dataset._plan._dataset_name, self._base_dataset._uuid
        )
