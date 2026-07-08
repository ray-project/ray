from typing import TYPE_CHECKING, Dict, Iterator, Optional, Tuple

from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.stats import (
    DATASET_METRICS_TAG_KEY,
    DEFAULT_METRICS_RANK,
    RANK_METRICS_TAG_KEY,
    DatasetStats,
)
from ray.data.context import DataContext
from ray.data.iterator import DataIterator

if TYPE_CHECKING:

    from ray.data._internal.execution.streaming_executor import StreamingExecutor
    from ray.data.dataset import Dataset, Schema


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
    ) -> Tuple[
        Iterator[RefBundle],
        Optional[DatasetStats],
        bool,
        Optional["StreamingExecutor"],
    ]:
        (
            ref_bundles_iterator,
            stats,
            executor,
        ) = self._base_dataset._execute_to_iterator()
        return ref_bundles_iterator, stats, False, executor

    def stats(self) -> str:
        return self._base_dataset.stats()

    def schema(self) -> Optional["Schema"]:
        return self._base_dataset.schema()

    def get_context(self) -> DataContext:
        return self._base_dataset.context

    def _get_metrics_tags(self) -> Dict[str, str]:
        # Plain iterators have no consumer/rank dimension, so use the default
        # rank sentinel. This keeps the iteration metric label set uniform with
        # the stream-split path while collapsing back to a single series.
        return {
            DATASET_METRICS_TAG_KEY: self._base_dataset.get_dataset_id(),
            RANK_METRICS_TAG_KEY: DEFAULT_METRICS_RANK,
        }
