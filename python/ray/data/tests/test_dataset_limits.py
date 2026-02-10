import sys
import time

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.conftest import (
    CoreExecutionMetrics,
    assert_core_execution_metrics_equals,
    get_initial_core_execution_metrics_snapshot,
)
from ray.data.tests.util import extract_values
from ray.tests.conftest import *  # noqa


def test_limit_execution(ray_start_regular):
    last_snapshot = get_initial_core_execution_metrics_snapshot()
    override_num_blocks = 20
    ds = ray.data.range(100, override_num_blocks=override_num_blocks)

    # Add some delay to the output to prevent all tasks from finishing
    # immediately.
    def delay(row):
        time.sleep(0.1)
        return row

    ds = ds.map(delay)
    last_snapshot = assert_core_execution_metrics_equals(
        CoreExecutionMetrics(task_count={}),
        last_snapshot=last_snapshot,
    )

    # During lazy execution, we should not execute too many more tasks than is
    # needed to produce the requested number of rows.
    for i in [1, 11]:
        assert extract_values("id", ds.limit(i).take(200)) == list(range(i))
        last_snapshot = assert_core_execution_metrics_equals(
            CoreExecutionMetrics(
                task_count={
                    "ReadRange->Map(delay)": lambda count: count
                    < override_num_blocks / 2,
                    "slice_fn": lambda count: count <= 1,
                }
            ),
            last_snapshot=last_snapshot,
        )

    # .materialize().limit() should only trigger execution once.
    ds = ray.data.range(100, override_num_blocks=20).materialize()
    last_snapshot = assert_core_execution_metrics_equals(
        CoreExecutionMetrics(
            task_count={
                "ReadRange": 20,
            }
        ),
        last_snapshot=last_snapshot,
    )
    for i in [1, 10]:
        assert extract_values("id", ds.limit(i).take(200)) == list(range(i))
        assert_core_execution_metrics_equals(
            CoreExecutionMetrics(task_count={"slice_fn": lambda count: count <= 1}),
            last_snapshot=last_snapshot,
        )


@pytest.mark.parametrize("lazy", [False, True])
def test_limit(ray_start_regular_shared, lazy):
    ds = ray.data.range(100, override_num_blocks=20)
    if not lazy:
        ds = ds.materialize()
    for i in range(100):
        assert extract_values("id", ds.limit(i).take(200)) == list(range(i))


# NOTE: We test outside the power-of-2 range in order to ensure that we're not reading
# redundant files due to exponential ramp-up.
@pytest.mark.parametrize("limit", [10, 20, 30, 60])
def test_limit_no_redundant_read(
    ray_start_regular_shared,
    limit,
):
    # Test that dataset truncation eliminates redundant reads.
    @ray.remote
    class Counter:
        def __init__(self):
            self.count = 0

        def increment(self):
            self.count += 1

        def get(self):
            return self.count

        def reset(self):
            self.count = 0

    class CountingRangeDatasource(Datasource):
        def __init__(self):
            self.counter = Counter.remote()

        def prepare_read(self, parallelism, n):
            def range_(i):
                ray.get(self.counter.increment.remote())
                return [
                    pd.DataFrame({"id": range(parallelism * i, parallelism * i + n)})
                ]

            return [
                ReadTask(
                    lambda i=i: range_(i),
                    BlockMetadata(
                        num_rows=n,
                        size_bytes=sum(
                            sys.getsizeof(i)
                            for i in range(parallelism * i, parallelism * i + n)
                        ),
                        input_files=None,
                        exec_stats=None,
                    ),
                    schema=pa.lib.Schema.from_pandas(pd.DataFrame({"id": []})),
                )
                for i in range(parallelism)
            ]

    source = CountingRangeDatasource()

    total_rows = 1000
    override_num_blocks = 100
    ds = ray.data.read_datasource(
        source,
        override_num_blocks=override_num_blocks,
        n=total_rows // override_num_blocks,
    )
    # Apply multiple limit ops.
    # Once the smallest limit is reached, the entire dataset should stop execution.
    ds = ds.limit(total_rows)
    ds = ds.limit(limit)
    ds = ds.limit(total_rows)
    # Check content.
    assert len(ds.take(limit)) == limit
    # Check number of read tasks launched.
    # min_read_tasks is the minimum number of read tasks needed for the limit.
    # We may launch more tasks than this number, in order to to maximize throughput.
    # But the actual number of read tasks should be less than the parallelism.
    count = ray.get(source.counter.get.remote())
    min_read_tasks = limit // (total_rows // override_num_blocks)
    assert min_read_tasks <= count < override_num_blocks


def test_limit_no_num_row_info(ray_start_regular_shared):
    # Test that datasources with no number-of-rows metadata available are still able to
    # be truncated, falling back to kicking off all read tasks.
    class DumbOnesDatasource(Datasource):
        def prepare_read(self, parallelism, n):
            return parallelism * [
                ReadTask(
                    lambda: [pd.DataFrame({"id": [1] * n})],
                    BlockMetadata(
                        num_rows=None,
                        size_bytes=sys.getsizeof(1) * n,
                        input_files=None,
                        exec_stats=None,
                    ),
                    schema=pa.lib.Schema.from_pandas(pd.DataFrame({"id": []})),
                )
            ]

    ds = ray.data.read_datasource(DumbOnesDatasource(), override_num_blocks=10, n=10)
    for i in range(1, 100):
        assert extract_values("id", ds.limit(i).take(100)) == [1] * i


def test_per_task_row_limit_basic(ray_start_regular_shared, restore_data_context):
    """Test basic per-block limiting functionality."""
    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    # Simple test that should work with the existing range datasource
    ds = ray.data.range(1000, override_num_blocks=10).limit(50)
    result = ds.take_all()

    # Verify we get the correct results
    assert len(result) == 50
    assert [row["id"] for row in result] == list(range(50))


def test_per_task_row_limit_with_custom_readtask(ray_start_regular_shared):
    """Test per-block limiting directly with ReadTask implementation."""

    def read_data_with_limit():
        # This simulates a ReadTask that reads 200 rows
        return [pd.DataFrame({"id": range(200)})]

    # Create ReadTask with per-block limit
    task_with_limit = ReadTask(
        read_fn=read_data_with_limit,
        metadata=BlockMetadata(
            num_rows=200, size_bytes=1600, input_files=None, exec_stats=None
        ),
        schema=pa.lib.Schema.from_pandas(pd.DataFrame({"id": []})),
        per_task_row_limit=50,
    )

    # Execute the ReadTask
    result_blocks = list(task_with_limit())

    # Should get only 50 rows due to per-block limiting
    assert len(result_blocks) == 1
    assert len(result_blocks[0]) == 50
    assert result_blocks[0]["id"].tolist() == list(range(50))


def test_per_task_row_limit_multiple_blocks_per_task(ray_start_regular_shared):
    """Test per-block limiting when ReadTasks return multiple blocks."""

    def read_multiple_blocks_with_limit():
        # This simulates a ReadTask that returns 3 blocks of 30 rows each
        return [
            pd.DataFrame({"id": range(0, 30)}),
            pd.DataFrame({"id": range(30, 60)}),
            pd.DataFrame({"id": range(60, 90)}),
        ]

    # Create ReadTask with per-block limit of 70 (should get 2.33 blocks)
    task = ReadTask(
        read_fn=read_multiple_blocks_with_limit,
        metadata=BlockMetadata(
            num_rows=90, size_bytes=720, input_files=None, exec_stats=None
        ),
        schema=pa.lib.Schema.from_pandas(pd.DataFrame({"id": []})),
        per_task_row_limit=70,
    )

    result_blocks = list(task())

    # Should get first 2 full blocks (60 rows) plus 10 rows from third block
    total_rows = sum(len(block) for block in result_blocks)
    assert total_rows == 70

    # Verify the data is correct
    all_ids = []
    for block in result_blocks:
        all_ids.extend(block["id"].tolist())
    assert all_ids == list(range(70))


def test_per_task_row_limit_larger_than_data(
    ray_start_regular_shared, restore_data_context
):
    """Test per-block limiting when limit is larger than available data."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    total_rows = 50
    ds = ray.data.range(total_rows, override_num_blocks=5)
    limited_ds = ds.limit(100)  # Limit larger than data
    result = limited_ds.take_all()

    assert len(result) == total_rows
    assert [row["id"] for row in result] == list(range(total_rows))


def test_per_task_row_limit_exact_block_boundary(
    ray_start_regular_shared, restore_data_context
):
    """Test per-block limiting when limit exactly matches block boundaries."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    rows_per_block = 20
    num_blocks = 5
    limit = rows_per_block * 2  # Exactly 2 blocks

    ds = ray.data.range(rows_per_block * num_blocks, override_num_blocks=num_blocks)
    limited_ds = ds.limit(limit)
    result = limited_ds.take_all()

    assert len(result) == limit
    assert [row["id"] for row in result] == list(range(limit))


@pytest.mark.parametrize("limit", [1, 5, 10, 25, 50, 99])
def test_per_task_row_limit_various_sizes(
    ray_start_regular_shared, limit, restore_data_context
):
    """Test per-block limiting with various limit sizes."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    total_rows = 100
    num_blocks = 10

    ds = ray.data.range(total_rows, override_num_blocks=num_blocks)
    limited_ds = ds.limit(limit)
    result = limited_ds.take_all()

    expected_len = min(limit, total_rows)
    assert len(result) == expected_len
    assert [row["id"] for row in result] == list(range(expected_len))


def test_per_task_row_limit_with_transformations(
    ray_start_regular_shared, restore_data_context
):
    """Test that per-block limiting works correctly with transformations."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    # Test with map operation after limit
    ds = ray.data.range(100, override_num_blocks=10)
    limited_ds = ds.limit(20).map(lambda x: {"doubled": x["id"] * 2})
    result = limited_ds.take_all()

    assert len(result) == 20
    assert [row["doubled"] for row in result] == [i * 2 for i in range(20)]

    # Test with map operation before limit
    ds = ray.data.range(100, override_num_blocks=10)
    limited_ds = ds.map(lambda x: {"doubled": x["id"] * 2}).limit(20)
    result = limited_ds.take_all()

    assert len(result) == 20
    assert [row["doubled"] for row in result] == [i * 2 for i in range(20)]


def test_per_task_row_limit_with_filter(ray_start_regular_shared, restore_data_context):
    """Test per-block limiting with filter operations."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    # Filter before limit - per-block limiting should still work at read level
    ds = ray.data.range(200, override_num_blocks=10)
    filtered_limited = ds.filter(lambda x: x["id"] % 2 == 0).limit(15)
    result = filtered_limited.take_all()

    assert len(result) == 15
    # Should get first 15 even numbers
    assert [row["id"] for row in result] == [i * 2 for i in range(15)]


def test_per_task_row_limit_readtask_properties(ray_start_regular_shared):
    """Test ReadTask per_block_limit property."""

    def dummy_read():
        return [pd.DataFrame({"id": [1, 2, 3]})]

    # Test ReadTask without per_block_limit
    task_no_limit = ReadTask(
        read_fn=dummy_read,
        metadata=BlockMetadata(
            num_rows=3, size_bytes=24, input_files=None, exec_stats=None
        ),
    )
    assert task_no_limit.per_task_row_limit is None

    # Test ReadTask with per_block_limit
    task_with_limit = ReadTask(
        read_fn=dummy_read,
        metadata=BlockMetadata(
            num_rows=3, size_bytes=24, input_files=None, exec_stats=None
        ),
        per_task_row_limit=10,
    )
    assert task_with_limit.per_task_row_limit == 10


def test_per_task_row_limit_edge_cases(ray_start_regular_shared, restore_data_context):
    """Test per-block limiting edge cases."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    # Test with single row
    ds = ray.data.range(1, override_num_blocks=1).limit(1)
    result = ds.take_all()
    assert len(result) == 1
    assert result[0]["id"] == 0

    # Test with limit of 1 on large dataset
    ds = ray.data.range(10000, override_num_blocks=100).limit(1)
    result = ds.take_all()
    assert len(result) == 1
    assert result[0]["id"] == 0

    # Test with very large limit
    ds = ray.data.range(100, override_num_blocks=10).limit(999999)
    result = ds.take_all()
    assert len(result) == 100
    assert [row["id"] for row in result] == list(range(100))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
