import functools
from typing import Any, Callable, List, Iterator, Generic, Union, TYPE_CHECKING

import ray
from ray.experimental.data.dataset import Dataset, T, U, BatchType
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow

PER_DATASET_OPS = [
    "map", "map_batches", "flat_map", "filter", "repartition", "sort", "limit"
]

OUTPUT_ITER_OPS = ["take", "show", "iter_rows", "to_tf", "to_torch"]


# TODO(ekl) make this serializable [initial version]
@PublicAPI(stability="beta")
class DatasetPipeline(Generic[T]):
    def __init__(self, dataset_generator: Iterator[Callable[[], Dataset[T]]]):
        self._dataset_generator = dataset_generator
        self._active_dataset = None
        self._next_dataset = None
        self._generated_datasets = []

    def split(self, n: int,
              locality_hints: List[Any] = None) -> List["DatasetPipeline[T]"]:
        # TODO(ekl) this should return serializable pipeline readers, probably
        # need a coordinating actor here
        # (not in initial version)
        raise NotImplementedError

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        return next(self.iter_datasets()).schema()

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
        @ray.remote
        def compute_single(fn: Callable[[], Dataset[T]]) -> Dataset[T]:
            return fn()

        # TODO(ekl) pipeline individual transforms? config depth?
        # .pipeline(blocks_per_stage=10)
        def gen_datasets() -> Iterator[Dataset[T]]:
            i = 0
            self._active_dataset = compute_single.remote(
                next(self._dataset_generator))
            try:
                self._next_dataset = compute_single.remote(
                    next(self._dataset_generator))
            except StopIteration:
                self._next_dataset = None

            while self._active_dataset:
                print("Returning pipeline index", i)
                i += 1
                yield ray.get(self._active_dataset)
                self._active_dataset = self._next_dataset
                try:
                    self._next_dataset = compute_single.remote(
                        next(self._dataset_generator))
                except StopIteration:
                    self._next_dataset = None
                    print("No more datasets left")

        return gen_datasets()

    def foreach_dataset(self, fn: Callable[[Dataset[T]], Dataset[U]]
                        ) -> "DatasetPipeline[U]":
        def make_dataset_generator() -> Iterator[Callable[[], Dataset[U]]]:
            for item in self._dataset_generator:
                yield lambda item=item: fn(item())

        return DatasetPipeline(make_dataset_generator())


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
