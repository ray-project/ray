import builtins
from typing import Any, Generic, List, Callable, Union, TypeVar

from ray.experimental.data.impl.arrow_block import ArrowRow, \
    DelegatingArrowBlockBuilder
from ray.experimental.data.impl.block import Block, ListBlock
from ray.experimental.data.impl.block_list import BlockList, BlockMetadata

T = TypeVar("T")
R = TypeVar("WriteResult")


class Datasource(Generic[T, R]):
    def prepare_read(self, parallelism: int,
                     **read_args) -> List["ReadTask[T]"]:
        raise NotImplementedError

    def prepare_write(self, blocks: BlockList,
                      **write_args) -> List["WriteTask[T, R]"]:
        raise NotImplementedError

    def on_write_complete(self, write_tasks: List["WriteTask[T, R]"],
                          write_task_outputs: List[R]) -> None:
        pass

    def on_write_failed(self, write_tasks: List["WriteTask[T, R]"],
                        error: Exception) -> None:
        pass


class ReadTask(Callable[[], Block[T]]):
    def __init__(self, read_fn: Callable[[], Block[T]],
                 metadata: BlockMetadata):
        self._metadata = metadata
        self._read_fn = read_fn

    def get_metadata(self) -> BlockMetadata:
        return self._metadata

    def __call__(self) -> Block[T]:
        return self._read_fn()


class WriteTask(Callable[[Block[T]], R]):
    def __init__(self, write_fn: Callable[[Block[T]], R]):
        self._write_fn = write_fn

    def __call__(self) -> R:
        self._write_fn()


class RangeDatasource(Datasource[Union[ArrowRow, int]]):
    def prepare_read(self, parallelism: int, n: int,
                     use_arrow: bool) -> List[ReadTask]:
        read_tasks: List[ReadTask] = []
        block_size = max(1, n // parallelism)

        def make_block(start: int, count: int) -> ListBlock:
            builder = DelegatingArrowBlockBuilder.builder()
            for value in builtins.range(start, start + count):
                if use_arrow:
                    builder.add(value)
                else:
                    builder.add({"value": value})
            return builder.build()

        i = 0
        while i < n:

            def bind_lambda_args(fn: Any, start: int, count: int) -> Any:
                return lambda: fn(start, count)

            count = min(block_size, n - i)
            read_tasks.append(
                bind_lambda_args(make_block, i, count),
                BlockMetadata(
                    num_rows=count,
                    size_bytes=8 * count,
                    schema=int,
                    input_files=None))
            i += block_size

        return read_tasks
