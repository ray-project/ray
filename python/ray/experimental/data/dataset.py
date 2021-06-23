from typing import List, Any, Callable, Iterator, Generic, TypeVar, \
    Generator, Optional, Union, TYPE_CHECKING

import pyarrow

if TYPE_CHECKING:
    import pandas
    import modin
    import dask
    import pyspark
    import ray.util.sgd

import ray
from ray.experimental.data.impl.compute import get_compute
from ray.experimental.data.impl.block import ObjectRef, Block

T = TypeVar("T")
U = TypeVar("U")
BatchType = Union["pandas.DataFrame", pyarrow.Table]


class Dataset(Generic[T]):
    def __init__(self, blocks: List[ObjectRef[Block]], block_cls: Any):
        self._blocks: List[ObjectRef[Block]] = blocks
        self._block_cls = block_cls

    def map(self, fn: Callable[[T], U], compute="tasks",
            **ray_remote_args) -> "Dataset[U]":
        def transform(serialized: Any) -> Any:
            block = self._block_cls.deserialize(serialized)
            builder = block.builder()
            for row in block.iter_rows():
                builder.add(fn(row))
            return builder.build().serialize()

        compute = get_compute(compute)

        return Dataset(
            compute.apply(transform, ray_remote_args, self._blocks),
            self._block_cls)

    def map_batches(self,
                    fn: Callable[[BatchType], BatchType],
                    batch_size: int = None,
                    compute: str = "tasks",
                    batch_format: str = "pandas",
                    **ray_remote_args) -> "Dataset[dict]":
        raise NotImplementedError  # P0

    def flat_map(self,
                 fn: Callable[[T], Iterator[U]],
                 compute="tasks",
                 **ray_remote_args) -> "Dataset[U]":
        def transform(serialized: Any) -> Any:
            block = self._block_cls.deserialize(serialized)
            builder = block.builder()
            for row in block.iter_rows():
                for r2 in fn(row):
                    builder.add(r2)
            return builder.build().serialize()

        compute = get_compute(compute)

        return Dataset(
            compute.apply(transform, ray_remote_args, self._blocks),
            self._block_cls)

    def filter(self,
               fn: Callable[[T], bool],
               compute="tasks",
               **ray_remote_args) -> "Dataset[T]":
        def transform(serialized: Any) -> Any:
            block = self._block_cls.deserialize(serialized)
            builder = block.builder()
            for row in block.iter_rows():
                if fn(row):
                    builder.add(row)
            return builder.build().serialize()

        compute = get_compute(compute)

        return Dataset(
            compute.apply(transform, ray_remote_args, self._blocks),
            self._block_cls)

    def take(self, limit: int = 20) -> List[T]:
        output = []
        for row in self.to_local_iterator():
            output.append(row)
            if len(output) >= limit:
                break
        return output

    def show(self, limit: int = 20) -> None:
        for row in self.take(limit):
            print(row)

    def to_local_iterator(self) -> Generator:
        for b in self._blocks:
            block = self._block_cls.deserialize(ray.get(b))
            for row in block.iter_rows():
                yield row

    def count(self) -> int:
        @ray.remote
        def count(serialized: Any) -> int:
            block = self._block_cls.deserialize(serialized)
            return block.num_rows()

        return sum(ray.get([count.remote(block) for block in self._blocks]))

    def schema(self) -> Union[type, pyarrow.lib.Schema]:
        raise NotImplementedError  # P0

    def size_bytes(self) -> int:
        raise NotImplementedError  # P0

    def input_files(self) -> List[str]:
        raise NotImplementedError  # P0

    def write_parquet(path: str,
                      filesystem: Optional[pyarrow.fs.FileSystem] = None
                      ) -> None:
        raise NotImplementedError  # P0

    def write_json(path: str,
                   filesystem: Optional[pyarrow.fs.FileSystem] = None) -> None:
        raise NotImplementedError  # P0

    def write_csv(path: str,
                  filesystem: Optional[pyarrow.fs.FileSystem] = None) -> None:
        raise NotImplementedError  # P0

    def to_torch(self, **todo) -> "ray.util.sgd.torch.TorchMLDataset":
        raise NotImplementedError  # P1

    def to_tf(self, **todo) -> "ray.util.sgd.tf.TFMLDataset":
        raise NotImplementedError  # P1

    def to_batch_iterators(
            self,
            num_shards: int,
            batch_size: int = None,
            output_location_prefs: List[Any] = None,
            batch_format: str = "pandas",
            repeatable: bool = False) -> List[Iterator[BatchType]]:
        raise NotImplementedError  # P1

    def to_dask(self) -> "dask.DataFrame":
        raise NotImplementedError  # P1

    def to_modin(self) -> "modin.DataFrame":
        raise NotImplementedError  # P1

    def to_pandas(self) -> Iterator[ObjectRef["pandas.DataFrame"]]:
        raise NotImplementedError  # P1

    def to_spark(self) -> "pyspark.sql.DataFrame":
        raise NotImplementedError  # P2
