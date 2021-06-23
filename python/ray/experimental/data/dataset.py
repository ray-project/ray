from typing import List, Any, Callable, Iterable, Generic, TypeVar

import ray
from ray.experimental.data.impl.compute import get_compute
from ray.experimental.data.impl.block import BlockRef

T = TypeVar("T")
U = TypeVar("U")


class Dataset(Generic[T]):
    def __init__(self, blocks: List[BlockRef], block_cls: Any):
        self._blocks: List[BlockRef] = blocks
        self._block_cls = block_cls

    def map(self, fn: Callable[[T], U], compute="tasks",
            **remote_args) -> "Dataset[U]":
        def transform(serialized: Any) -> Any:
            block = self._block_cls.deserialize(serialized)
            builder = block.builder()
            for row in block.iter_rows():
                builder.add(fn(row))
            return builder.build().serialize()

        compute = get_compute(compute)

        return Dataset(
            compute.apply(transform, remote_args, self._blocks),
            self._block_cls)

    def flat_map(self,
                 fn: Callable[[T], Iterable[U]],
                 compute="tasks",
                 **remote_args) -> "Dataset[U]":
        def transform(serialized: Any) -> Any:
            block = self._block_cls.deserialize(serialized)
            builder = block.builder()
            for row in block.iter_rows():
                for r2 in fn(row):
                    builder.add(r2)
            return builder.build().serialize()

        compute = get_compute(compute)

        return Dataset(
            compute.apply(transform, remote_args, self._blocks),
            self._block_cls)

    def filter(self, fn: Callable[[T], bool], compute="tasks",
               **remote_args) -> "Dataset[T]":
        def transform(serialized: Any) -> Any:
            block = self._block_cls.deserialize(serialized)
            builder = block.builder()
            for row in block.iter_rows():
                if fn(row):
                    builder.add(row)
            return builder.build().serialize()

        compute = get_compute(compute)

        return Dataset(
            compute.apply(transform, remote_args, self._blocks),
            self._block_cls)

    def take(self, limit: int = 20) -> List[T]:
        output = []
        for b in self._blocks:
            block = self._block_cls.deserialize(ray.get(b))
            for row in block.iter_rows():
                output.append(row)
                if len(output) >= limit:
                    break
            if len(output) >= limit:
                break
        return output

    def show(self, limit: int = 20) -> None:
        for row in self.take(limit):
            print(row)

    def count(self) -> int:
        @ray.remote
        def count(serialized: Any) -> int:
            block = self._block_cls.deserialize(serialized)
            return block.num_rows()

        return sum(ray.get([count.remote(block) for block in self._blocks]))

    def sum(self) -> int:
        @ray.remote
        def _sum(serialized: Any) -> int:
            block = self._block_cls.deserialize(serialized)
            return sum(block.iter_rows())

        return sum(ray.get([_sum.remote(block) for block in self._blocks]))
