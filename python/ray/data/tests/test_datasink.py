import pickle
from typing import Any, Iterable, List, Tuple, cast

import pytest

import ray
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block
from ray.data.datasource import Datasink
from ray.data.datasource.datasink import WriteResult


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

    ray.data.range(100, override_num_blocks=20).write_datasink(
        MockDatasink(num_rows_per_write)
    )


@pytest.mark.parametrize("num_rows_per_write", [5, 10, 50])
def test_on_write_complete(tmp_path, ray_start_regular_shared, num_rows_per_write):
    class MockDatasink(Datasink):
        def __init__(self, num_rows_per_write):
            self._num_rows_per_write = num_rows_per_write
            self.payloads = None

        def write(self, blocks: Iterable[Block], ctx: TaskContext) -> Any:
            assert sum(len(block) for block in blocks) == self._num_rows_per_write
            return f"task-{ctx.task_idx}"

        def on_write_complete(self, write_result_blocks: List[Block]) -> WriteResult:
            self.payloads = [
                pickle.loads(result["payload"].iloc[0])
                for result in write_result_blocks
            ]
            return super().on_write_complete(write_result_blocks)

        @property
        def num_rows_per_write(self):
            return self._num_rows_per_write

    sink = MockDatasink(num_rows_per_write)
    ray.data.range(100, override_num_blocks=20).write_datasink(sink)
    expect = [f"task-{i}" for i in range(100 // num_rows_per_write)]
    assert sink.payloads == expect


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
