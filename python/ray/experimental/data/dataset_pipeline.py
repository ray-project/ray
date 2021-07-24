import functools
from typing import Any, Callable, List, Iterator, Generic, Union, TYPE_CHECKING

import ray
from ray.experimental.data.dataset import Dataset, T, U, BatchType
from ray.experimental.data.impl.progress_bar import ProgressBar
from ray.util.annotations import PublicAPI
from ray.types import ObjectRef

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
        @ray.remote
        def do(fn: Callable[[], Dataset[T]]) -> Dataset[T]:
            return fn()

        class PipelineExecutor:
            def __init__(self, pipeline: DatasetPipeline[T]):
                self._pipeline: DatasetPipeline[T] = pipeline
                self._stages: List[ObjectRef[Dataset[Any]]] = [None] * (
                    len(self._pipeline._stage_transforms) + 1)
                self._stages[0] = do.remote(
                    next(self._pipeline._base_iterator))
                self._bars = [
                    ProgressBar("Stage {}".format(i), self._pipeline._length or 1, position=i)
                    for i in range(len(self._stages))
                ]

            def __iter__(self):
                return self

            def __next__(self):
                output = None
                ready = []

                while output is None:
                    if all(s is None for s in self._stages):
                        raise StopIteration

                    # Wait for any completed stages.
                    pending = [s for s in self._stages if s is not None]
                    ready, _ = ray.wait(
                        pending, timeout=0.1, num_returns=len(pending))

                    # Bubble elements down the pipeline as they become ready.
                    for i in range(len(self._stages))[::-1]:
                        is_last = i + 1 >= len(self._stages)
                        next_slot_free = is_last or self._stages[i + 1] is None
                        if not next_slot_free:
                            continue

                        slot_ready = self._stages[i] in ready
                        if not slot_ready:
                            continue

                        # Bubble.
                        result = ray.get(self._stages[i])
                        self._bars[i].update(1)
                        self._stages[i] = None
                        if is_last:
                            output = result
                        else:
                            fn = self._pipeline._stage_transforms[i]
                            self._stages[i + 1] = do.remote(lambda: fn(result))

                    # Pull a new element for the initial slot if possible.
                    if self._stages[0] is None:
                        try:
                            self._stages[0] = do.remote(
                                next(self._pipeline._base_iterator))
                        except StopIteration:
                            pass

                return output

        return PipelineExecutor(self)

    def foreach_dataset(self, fn: Callable[[Dataset[T]], Dataset[U]]
                        ) -> "DatasetPipeline[U]":
        return DatasetPipeline(self._base_iterator,
                               self._stage_transforms + [fn],
                               self._length)

    def split(self, n: int,
              locality_hints: List[Any] = None) -> List["DatasetPipeline[T]"]:
        # TODO(ekl) implement this in the next PR. We'll need some kind of
        # coordinator actor to make this work.
        raise NotImplementedError

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
