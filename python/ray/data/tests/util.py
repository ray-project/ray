from contextlib import contextmanager

import os
import random
import tempfile

from ray.data.impl.arrow_block import ArrowRow, \
    DelegatingArrowBlockBuilder

@contextmanager
def gen_bin_files(n):
    with tempfile.TemporaryDirectory() as temp_dir:
        paths = []
        for i in range(n):
            path = os.path.join(temp_dir, f"{i}.bin")
            paths.append(path)
            fp = open(path, "wb")
            to_write = str(i) * 500
            fp.write(to_write.encode())
        yield (temp_dir, paths)


class LinearCombinationDatasource(Datasource):
    """An example datasource that generates rows with random int64 columns.

    Examples:
        >>> source = RandomIntRowDatasource()
        >>> ray.data.read_datasource(source, n=10, num_columns=2).take()
        ... {'c_0': 1717767200176864416, 'c_1': 999657309586757214}
        ... {'c_0': 4983608804013926748, 'c_1': 1160140066899844087}
    """

    def prepare_read(self, parallelism: int, n: int,
                     a: float, b: float) -> List[ReadTask]:
        _check_pyarrow_version()
        import pyarrow

        read_tasks: List[ReadTask] = []
        block_size = max(1, n // parallelism)

        def make_block(count: int, a: float, b: float) -> Block:
            builder = DelegatingArrowBlockBuilder()
            for _ in range(count):
                x = random.random()
                builder.add([{
                    "x": x,
                    "y": a * x + b,
                }])
            return builder.build()

        schema = pyarrow.Table.from_pydict(
            {"x": [0], "y": [0]}).schema

        i = 0
        while i < n:
            count = min(block_size, n - i)
            read_tasks.append(
                ReadTask(
                    lambda count=count, a=a, b=b:
                        make_block(count, a, b),
                    BlockMetadata(
                        num_rows=count,
                        size_bytes=8 * count * 2,
                        schema=schema,
                        input_files=None)))
            i += block_size

        return read_tasks