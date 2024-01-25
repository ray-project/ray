from typing import Any, Iterable

import pytest

import ray
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block
from ray.data.datasource import Datasink


@pytest.mark.parametrize("num_rows_per_write", [1, 2, 10])
def test_num_rows_per_block(tmp_path, ray_start_regular_shared, num_rows_per_write):
    class MockDatasink(Datasink):
        def write(self, blocks: Iterable[Block], ctx: TaskContext) -> Any:
            print(sum(len(block) for block in blocks))

    ds = ray.data.range(100).write_datasink(
        MockDatasink(), num_rows_per_write=num_rows_per_write
    )
    assert ds
