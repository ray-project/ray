import builtins
from typing import Generic, List, Callable, Union, TypeVar

import ray
from ray.experimental.data.impl.arrow_block import ArrowRow, \
    DelegatingArrowBlockBuilder
from ray.experimental.data.impl.block import Block, ListBlock
from ray.experimental.data.impl.block_list import BlockList, BlockMetadata

T = TypeVar("T")
R = TypeVar("WriteResult")


class Datasource(Generic[T]):
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
            builder = DelegatingArrowBlockBuilder()
            for value in builtins.range(start, start + count):
                if use_arrow:
                    builder.add({"value": value})
                else:
                    builder.add(value)
            return builder.build()

        i = 0
        while i < n:
            count = min(block_size, n - i)
            if use_arrow:
                import pyarrow
                schema = pyarrow.Table.from_pydict({"value": [0]}).schema
            else:
                schema = int
            read_tasks.append(
                ReadTask(
                    lambda i=i, count=count: make_block(i, count),
                    BlockMetadata(
                        num_rows=count,
                        size_bytes=8 * count,
                        schema=schema,
                        input_files=None)))
            i += block_size

        return read_tasks


class DebugOutput(Datasource[Union[ArrowRow, int]]):
    def __init__(self):
        @ray.remote
        class DataSink:
            def __init__(self):
                self.rows_written = 0
                self.enabled = True

            def write(self, block: Block[T]) -> None:
                if not self.enabled:
                    raise ValueError("disabled")
                self.rows_written += block.num_rows()

            def get_rows_written(self):
                return self.rows_written

            def set_enabled(self, enabled):
                self.enabled = enabled

        self.data_sink = DataSink.remote()
        self.num_ok = 0
        self.num_failed = 0

    def prepare_write(self, blocks: BlockList,
                      **write_args) -> List["WriteTask[T, R]"]:
        tasks = []
        for b in blocks:
            tasks.append(
                WriteTask(lambda b=b: ray.get(self.data_sink.write.remote(b))))
        return tasks

    def on_write_complete(self, write_tasks: List["WriteTask[T, R]"],
                          write_task_outputs: List[R]) -> None:
        self.num_ok += 1

    def on_write_failed(self, write_tasks: List["WriteTask[T, R]"],
                        error: Exception) -> None:
        self.num_failed += 1
