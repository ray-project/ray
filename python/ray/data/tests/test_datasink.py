from typing import Any, Iterable

import pytest

import ray
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block
from ray.data.datasource import Datasink


@pytest.mark.parametrize("num_rows_per_write", [5, 10, 50])
def test_num_rows_per_write(tmp_path, ray_start_regular_shared, num_rows_per_write):
    class MockDatasink(Datasink):
        def __init__(self, num_rows_per_write):
            self._num_rows_per_write = num_rows_per_write

        def write(self, blocks: Iterable[Block], ctx: TaskContext) -> Any:
            assert sum(len(block) for block in blocks) == self._num_rows_per_write

        @property
        def num_rows_per_write(self):
            return self._num_rows_per_write

    ray.data.range(100, parallelism=20).write_datasink(MockDatasink(num_rows_per_write))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
