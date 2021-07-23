from typing import Any, Callable, List, Iterator, Optional, Generic, Union

import ray
from ray.experimental.data.dataset import Dataset, T, U, BatchType
from ray.experimental.data.impl.compute import CallableClass
from ray.util.annotations import DeveloperAPI, PublicAPI


# TODO(ekl) make this serializable?
@PublicAPI(stability="beta")
class DatasetPipeline(Generic[T]):
    def __init__(self, dataset_generator: Iterator[Callable[[], Dataset[T]]]):
        self._dataset_generator = dataset_generator
        self._active_dataset = None
        self._next_dataset = None
        self._generated_datasets = []

    def map(self,
            fn: Union[CallableClass, Callable[[T], U]],
            compute: Optional[str] = None,
            **ray_remote_args) -> "DatasetPipeline[U]":
        return self.foreach_dataset(
            lambda ds: ds.map(fn, compute, **ray_remote_args))

    def repartition(self, num_blocks: int) -> "DatasetPipeline[T]":
        return self.foreach_dataset(lambda ds: ds.repartition(num_blocks))

    def split(self, n: int,
              locality_hints: List[Any] = None) -> List["DatasetPipeline[T]"]:
        # TODO(ekl) this should return serializable pipeline readers, probably
        # need a coordinating actor here
        raise NotImplementedError

    def iter_rows(self, prefetch_blocks: int = 0) -> Iterator[T]:
        return Dataset.iter_rows(self, prefetch_blocks)

    def take(self, limit: int = 20) -> List[T]:
        return Dataset.take(self, limit)

    def show(self, limit: int = 20) -> None:
        return Dataset.show(self, limit)

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

    @DeveloperAPI
    def iter_datasets(self) -> Iterator[Dataset[T]]:
        @ray.remote
        def compute_single(fn: Callable[[], Dataset[T]]) -> Dataset[T]:
            return fn()

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

    @DeveloperAPI
    def foreach_dataset(self, fn: Callable[[Dataset[T]], Dataset[U]]
                        ) -> "DatasetPipeline[U]":
        def make_dataset_generator() -> Iterator[Callable[[], Dataset[U]]]:
            for item in self._dataset_generator:
                yield lambda item=item: fn(item())

        return DatasetPipeline(make_dataset_generator())
