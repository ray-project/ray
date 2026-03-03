from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.limit_operator import LimitOperator
from ray.data._internal.execution.operators.map_operator import _per_block_limit_fn
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data._internal.execution.util import make_ref_bundles
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data.context import DataContext
from ray.data.tests.util import run_op_tasks_sync
from ray.tests.conftest import *  # noqa


def test_limit_operator(ray_start_regular_shared):
    """Test basic functionalities of LimitOperator."""
    num_refs = 3
    num_rows_per_block = 3
    total_rows = num_refs * num_rows_per_block
    # Test limits with different values, from 0 to more than input size.
    limits = list(range(0, total_rows + 2))
    for limit in limits:
        refs = make_ref_bundles([[i] * num_rows_per_block for i in range(num_refs)])
        input_op = InputDataBuffer(DataContext.get_current(), refs)
        limit_op = LimitOperator(limit, input_op, DataContext.get_current())
        limit_op.mark_execution_finished = MagicMock(
            wraps=limit_op.mark_execution_finished
        )
        if limit == 0:
            # If the limit is 0, the operator should be completed immediately.
            assert limit_op.has_completed()
            assert limit_op._limit_reached()
        cur_rows = 0
        loop_count = 0
        while input_op.has_next() and not limit_op._limit_reached():
            loop_count += 1
            assert not limit_op.has_completed(), limit
            assert not limit_op.has_execution_finished(), limit
            limit_op.add_input(input_op.get_next(), 0)
            while limit_op.has_next():
                # Drain the outputs. So the limit operator
                # will be completed when the limit is reached.
                limit_op.get_next()
            cur_rows += num_rows_per_block
            if cur_rows >= limit:
                assert limit_op.mark_execution_finished.call_count == 1, limit
                assert limit_op.has_completed(), limit
                assert limit_op._limit_reached(), limit
                assert limit_op.has_execution_finished(), limit
            else:
                assert limit_op.mark_execution_finished.call_count == 0, limit
                assert not limit_op.has_completed(), limit
                assert not limit_op._limit_reached(), limit
                assert not limit_op.has_execution_finished(), limit
        limit_op.mark_execution_finished()
        # After inputs done, the number of output bundles
        # should be the same as the number of `add_input`s.
        assert limit_op.num_outputs_total() == loop_count, limit
        assert limit_op.has_completed(), limit


def test_limit_operator_memory_leak_fix(ray_start_regular_shared, tmp_path):
    """Test that LimitOperator properly drains upstream output queues.

    This test verifies the memory leak fix by directly using StreamingExecutor
    to access the actual topology and check queued blocks after execution.
    """
    for i in range(100):
        data = [{"id": i * 5 + j, "value": f"row_{i * 5 + j}"} for j in range(5)]
        table = pa.Table.from_pydict(
            {"id": [row["id"] for row in data], "value": [row["value"] for row in data]}
        )
        parquet_file = tmp_path / f"test_data_{i}.parquet"
        pq.write_table(table, str(parquet_file))

    parquet_files = [str(tmp_path / f"test_data_{i}.parquet") for i in range(100)]

    ds = (
        ray.data.read_parquet(parquet_files, override_num_blocks=100)
        .limit(5)
        .map(lambda x: x)
    )

    execution_plan = ds._plan
    physical_plan = get_execution_plan(execution_plan._logical_plan)

    # Use StreamingExecutor directly to have access to the actual topology
    executor = StreamingExecutor(DataContext.get_current())
    output_iterator = executor.execute(physical_plan.dag)

    # Collect all results and count rows
    total_rows = 0
    for bundle in output_iterator:
        for block_ref in bundle.block_refs:
            block = ray.get(block_ref)
            total_rows += block.num_rows
    assert (
        total_rows == 5
    ), f"Expected exactly 5 rows after limit(5), but got {total_rows}"

    # Find the ReadParquet operator's OpState
    topology = executor._topology
    read_parquet_op_state = None
    for op, op_state in topology.items():
        if "ReadParquet" in op.name:
            read_parquet_op_state = op_state
            break

    # Check the output queue size
    output_queue_size = len(read_parquet_op_state.output_queue)
    assert output_queue_size == 0, f"Expected 0 items, but got {output_queue_size}."


def test_limit_estimated_num_output_bundles():
    # Test limit operator estimation
    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i, i] for i in range(100)])
    )
    op = LimitOperator(100, input_op, DataContext.get_current())

    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
        run_op_tasks_sync(op)
        assert op._estimated_num_output_bundles == 50

    op.all_inputs_done()

    # 2 rows per bundle, 100 / 2 = 50 blocks output
    assert op._estimated_num_output_bundles == 50

    # Test limit operator estimation where: limit > # of rows
    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i, i] for i in range(100)])
    )
    op = LimitOperator(300, input_op, DataContext.get_current())

    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
        run_op_tasks_sync(op)
        assert op._estimated_num_output_bundles == 100

    op.all_inputs_done()

    # all blocks are outputted
    assert op._estimated_num_output_bundles == 100


@pytest.mark.parametrize(
    "blocks_data,per_block_limit,expected_output",
    [
        # Test case 1: Single block, limit less than block size
        ([[1, 2, 3, 4, 5]], 3, [[1, 2, 3]]),
        # Test case 2: Single block, limit equal to block size
        ([[1, 2, 3]], 3, [[1, 2, 3]]),
        # Test case 3: Single block, limit greater than block size
        ([[1, 2]], 5, [[1, 2]]),
        # Test case 4: Multiple blocks, limit spans across blocks
        ([[1, 2], [3, 4], [5, 6]], 3, [[1, 2], [3]]),
        # Test case 5: Multiple blocks, limit exactly at block boundary
        ([[1, 2], [3, 4]], 2, [[1, 2]]),
        # Test case 6: Empty blocks
        ([], 5, []),
        # Test case 7: Zero limit
        ([[1, 2, 3]], 0, []),
    ],
)
def test_per_block_limit_fn(blocks_data, per_block_limit, expected_output):
    """Test the _per_block_limit_fn function with various inputs."""
    import pandas as pd

    # Convert test data to pandas blocks
    blocks = [pd.DataFrame({"value": data}) for data in blocks_data]

    # Create a mock TaskContext
    ctx = TaskContext(op_name="test", task_idx=0, target_max_block_size_override=None)

    # Call the function
    result_blocks = list(_per_block_limit_fn(blocks, ctx, per_block_limit))

    # Convert result back to lists for comparison
    result_data = []
    for block in result_blocks:
        block_data = block["value"].tolist()
        result_data.append(block_data)

    assert result_data == expected_output


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
