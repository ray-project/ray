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
        def transform(block):
            return self._block_cls([fn(row) for row in block])

        compute = get_compute(compute)

        return Dataset(
            compute.apply(transform, remote_args, self._blocks),
            self._block_cls)

    def flat_map(self,
                 fn: Callable[[T], Iterable[U]],
                 compute="tasks",
                 **remote_args) -> "Dataset[U]":
        def transform(block):
            output = []
            for row in block:
                for r2 in fn(row):
                    output.append(r2)
            return self._block_cls(output)

        compute = get_compute(compute)

        return Dataset(
            compute.apply(transform, remote_args, self._blocks),
            self._block_cls)

    def filter(self, fn: Callable[[T], bool], compute="tasks",
               **remote_args) -> "Dataset[T]":
        def transform(block):
            return self._block_cls([row for row in block if fn(row)])

        compute = get_compute(compute)

        return Dataset(
            compute.apply(transform, remote_args, self._blocks),
            self._block_cls)

    def take(self, limit: int = 20) -> List[T]:
        output = []
        for b in self._blocks:
            for row in ray.get(b):
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
        def count(block):
            return len(block)

        return sum(ray.get([count.remote(block) for block in self._blocks]))

    def sum(self) -> int:
        @ray.remote
        def _sum(block):
            return sum(block)

        return sum(ray.get([_sum.remote(block) for block in self._blocks]))
