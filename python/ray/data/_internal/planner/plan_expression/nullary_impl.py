import uuid

import numpy as np
import pandas as pd
import pyarrow as pa

from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data.block import BlockColumn, BlockType


def eval_random(
    num_rows: int,
    block_type: BlockType,
    *,
    seed: int | None = None,
    reseed_after_execution: bool = True,
) -> BlockColumn:
    """Implementation of the random expression.

    Args:
        num_rows: The number of rows to generate random values for.
        block_type: The type of block to generate random values for.
        seed: The seed to use for the random number generator.
        reseed_after_execution: Whether to reseed the random number generator after each execution.


    Returns:
        A BlockColumn containing the random values.

    Raises:
        TypeError: If the block type is not supported.
    """

    if seed is not None:
        # Numpy allows using a seed sequence (list of integers) to initialize
        # a random number generator. This allows us to maintain reproduciblity while
        # ensuring randomness in parallel execution.
        # See https://numpy.org/doc/2.2/reference/random/parallel.html#sequence-of-integer-seeds
        # Below we uses three components to create a seed sequence (fastest changing component first):
        # 1. An index based on the remote task in Ray Data
        # 2. An incrementing index of Ray Dataset execution (e.g., multiple training epochs)
        # 3. A base seed fixed by the user

        ctx = TaskContext.get_current()

        if ctx is None:
            import warnings

            warnings.warn(
                "TaskContext is not available for random() expression with seed. "
                "Falling back to task_idx=0 for all tasks, which reduces the parallelism "
                "benefits of random number generation. If you see this warning, please "
                "report it as it may indicate an execution context issue.",
                stacklevel=2,
            )
            task_idx = 0
        else:
            task_idx = ctx.task_idx

        if reseed_after_execution:
            from ray.data.context import DataContext

            data_context = (
                DataContext.get_current()
            )  # get or create DataContext, never None
            execution_idx = data_context._execution_idx
        else:
            execution_idx = 0

        # Numpy recommends fastest changing component to be the first one element
        block_seed = [task_idx, execution_idx, seed]
    else:
        block_seed = None

    rng = np.random.default_rng(block_seed)
    random_values = rng.random(num_rows)

    # Convert to appropriate format based on block type
    if block_type == BlockType.PANDAS:
        return pd.Series(random_values, dtype=np.float64)
    elif block_type == BlockType.ARROW:
        return pa.array(random_values, type=pa.float64())

    raise TypeError(f"Unsupported block type: {block_type}")


def eval_uuid(
    num_rows: int,
    block_type: BlockType,
) -> BlockColumn:
    """Implementation of the uuid expression.

    Args:
        num_rows: The number of rows to generate uuid values for.
        block_type: The type of block to generate uuid values for.

    Returns:
        A BlockColumn containing the uuid values.

    Raises:
        TypeError: If the block type is not supported.
    """
    arr = [str(uuid.uuid4()) for _ in range(num_rows)]
    if block_type == BlockType.PANDAS:
        return pd.Series(arr, dtype=pd.StringDtype())
    elif block_type == BlockType.ARROW:
        return pa.array(arr, type=pa.string())

    raise TypeError(f"Unsupported block type: {block_type}")
