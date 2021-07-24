import functools
import time
from typing import Any, Callable, List, Iterator, Generic, Union, TYPE_CHECKING

import ray
from ray.experimental.data.dataset import Dataset, T, U, BatchType
from ray.experimental.data.impl.pipeline_executor import PipelineExecutor, \
    PipelineSplitExecutorCoordinator
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow

# Operations that can be naively applied per dataset in the pipeline.
PER_DATASET_OPS = [
    "map", "map_batches", "flat_map", "filter", "repartition", "sort", "limit"
]

# Operations that operate over the stream of output batches from the pipeline.
OUTPUT_ITER_OPS = ["take", "show", "iter_rows", "to_tf", "to_torch"]


@PublicAPI(stability="beta")
class DatasetPipeline(Generic[T]):
    def __init__(
            self,
            base_iterator: Iterator[Callable[[], Dataset[T]]],
            stage_transforms: List[Callable[[Dataset[T]], Dataset[U]]] = None,
            length: int = None):
        """Construct a DatasetPipeline (internal API).

        The constructor is not part of the DatasetPipeline API. Use the
        ``Dataset.repeat()`` and ``Dataset.pipeline()`` methods to construct a
        dataset pipeline.
        """
        self._base_iterator = base_iterator
        self._stage_transforms = stage_transforms or []
        self._length = length

    def iter_batches(self,
                     prefetch_blocks: int = 0,
                     batch_size: int = None,
                     batch_format: str = "pandas",
                     drop_last: bool = False) -> Iterator[BatchType]:
        def gen_batches() -> Iterator[BatchType]:
            for ds in self.iter_datasets():
                for batch in ds.iter_batches(prefetch_blocks, batch_size,
                                             batch_format, drop_last):
                    yield batch

        return gen_batches()

    def iter_datasets(self) -> Iterator[Dataset[T]]:
        return PipelineExecutor(self)

    def foreach_dataset(self, fn: Callable[[Dataset[T]], Dataset[U]]
                        ) -> "DatasetPipeline[U]":
        return DatasetPipeline(self._base_iterator,
                               self._stage_transforms + [fn], self._length)

    def split(self, n: int,
              locality_hints: List[Any] = None) -> List["DatasetPipeline[T]"]:
        coordinator = PipelineSplitExecutorCoordinator.remote(
            self, n, locality_hints)

        class SplitIterator:
            def __init__(self, split_index):
                self.split_index = split_index

            def __next__(self):
                ds = None
                while ds is None:
                    ds = ray.get(
                        coordinator.next_dataset_if_ready.remote(
                            self.split_index))
                    # Wait for other shards to catch up reading.
                    if not ds:
                        time.sleep(.1)
                return ds

        splits = []
        for i in range(n):
            splits.append(DatasetPipeline(SplitIterator(i)))

        return splits

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        return next(self.iter_datasets()).schema()

    # TODO(ekl) add write APIs (need to assign uuids to avoid name conflicts)


for method in PER_DATASET_OPS:

    def make_impl(method):
        delegate = getattr(Dataset, method)

        @functools.wraps(delegate)
        def impl(self, *args, **kwargs):
            return self.foreach_dataset(
                lambda ds: getattr(ds, method)(*args, **kwargs))

        if "return" in impl.__annotations__:
            impl.__annotations__["return"] = impl.__annotations__[
                "return"].replace("Dataset", "DatasetPipeline")

        return impl

    setattr(DatasetPipeline, method, make_impl(method))

for method in OUTPUT_ITER_OPS:

    def make_impl(method):
        delegate = getattr(Dataset, method)

        @functools.wraps(delegate)
        def impl(self, *args, **kwargs):
            return delegate(self, *args, **kwargs)

        return impl

    setattr(DatasetPipeline, method, make_impl(method))
