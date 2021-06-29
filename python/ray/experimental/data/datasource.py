
class Datasource(Generic[T]):
    def prepare_read(self, parallelism: int = 200, **kwargs) -> List[ReadTask[T]]:
        raise NotImplementedError

    def prepare_write(self, blocks: BlockList) -> List[WriteTask[T]]:
        raise NotImplementedError


class ReadTask(Callable[[], Block[T]]):
    def __init__(self, read_fn: Callable[[], Block[T]], metadata: BlockMetadata):
        self._metadata = metadata
        self._read_fn = read_fn

    def get_metadata(self) -> BlockMetadata:
        return self._metadata

    def __call__(self) -> Block[T]:
        return self._read_fn()


class WriteTask(Callable[[Block[T]], None]):
    def __init__(self, write_fn: Callable[[Block[T]], None]):
        self.write_fn = write_fn

    def __call__(self) -> None:
        self._write_fn()


class RangeDatasource(Datasource[Union[ArrowRow, int]]):
    def __init__(self, n: int, use_arrow: bool):
        self.n = n
        self.use_arrow = use_arrow

    def prepare_read(self, parallelism: int = 200, **kwargs) -> List[ReadTask]:
        read_tasks: List[ReadTask] = []
        block_size = max(1, self.n // parallelism)

        def make_py_block(start: int, count: int) -> ListBlock:
            builder = ListBlock.builder()
            for value in builtins.range(start, start + count):
                builder.add(value)
            return builder.build()

        def make_arrow_block(start: int, count: int) -> "ArrowBlock":
            return ArrowBlock(
                pyarrow.Table.from_pydict({
                    "value": list(builtins.range(start, start + count))
                }))

        i = 0
        while i < self.n:

            def bind_lambda_args(fn: Any, start: int, count: int) -> Any:
                return lambda: fn(start, count)

            count = min(block_size, self.n - i)
            read_tasks.append(
                bind_lambda_args(make_arrow_block if self.use_arrow else make_py_block, i, count),
                BlockMetadata(
                    num_rows=count,
                    size_bytes=8 * count,
                    schema=int,
                    input_files=None))
            i += block_size

        return read_tasks


if __name__ == "__main__":
    ds = ray.experimental.data.from_datasource(
        RangeDatasource(n=10000, use_arrow=True))
    print(ds)
    print(ds.take(10))
