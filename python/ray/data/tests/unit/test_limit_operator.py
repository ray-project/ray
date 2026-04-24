import pytest

from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import _per_block_limit_fn


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
