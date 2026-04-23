import pytest

from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.limit_operator import LimitOperator
from ray.data._internal.execution.operators.map_operator import _per_block_limit_fn
from ray.data._internal.execution.util import make_ref_bundles
from ray.data.context import DataContext
from ray.data.tests.util import run_op_tasks_sync


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
