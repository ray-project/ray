import logging
import math
from typing import Optional, Tuple

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.interfaces import PhysicalPlan, Rule
from ray.data._internal.logical.operators.read_operator import Read
from ray.data.datasource.datasource import Reader

logger = logging.getLogger(__name__)


def compute_additional_split_factor(
    reader: Reader,
    parallelism: int,
    mem_size: int,
    target_max_block_size: int,
    cur_additional_split_factor: Optional[int],
) -> Tuple[int, Optional[int]]:
    num_read_tasks = len(reader.get_read_tasks(parallelism))
    expected_block_size = None
    if mem_size:
        expected_block_size = mem_size / num_read_tasks
        logger.debug(
            f"Expected in-memory size {mem_size}," f" block size {expected_block_size}"
        )
        size_based_splits = round(max(1, expected_block_size / target_max_block_size))
    else:
        size_based_splits = 1
    if cur_additional_split_factor:
        size_based_splits *= cur_additional_split_factor
    logger.debug(f"Size based split factor {size_based_splits}")
    estimated_num_blocks = num_read_tasks * size_based_splits
    logger.debug(f"Blocks after size splits {estimated_num_blocks}")

    # Add more output splitting for each read task if needed.
    # TODO(swang): For parallelism=-1 (user did not explicitly set
    # parallelism), and if the following stage produces much larger blocks,
    # we should scale down the target max block size here instead of using
    # splitting, which can have higher memory usage.
    if estimated_num_blocks < parallelism:
        k = math.ceil(parallelism / estimated_num_blocks)
        logger.info(
            f"To satisfy the requested parallelism of {parallelism}, "
            f"each read task output is split into {k} smaller blocks."
        )
        estimated_num_blocks = estimated_num_blocks * k
        return estimated_num_blocks, k

    return estimated_num_blocks, None


class SplitReadOutputBlocksRule(Rule):
    def apply(self, plan: PhysicalPlan) -> PhysicalPlan:
        ops = [plan.dag]

        while len(ops) == 1 and not isinstance(ops[0], InputDataBuffer):
            logical_op = plan.op_map[ops[0]]
            if isinstance(logical_op, Read):
                self._split_read_op_if_needed(ops[0], logical_op)
            ops = ops[0].input_dependencies

        return plan

    def _split_read_op_if_needed(self, op: PhysicalOperator, logical_op: Read):
        estimated_num_blocks, k = compute_additional_split_factor(
            logical_op._reader,
            logical_op._parallelism,
            logical_op._mem_size,
            op.actual_target_max_block_size,
            op._additional_split_factor,
        )

        if k is not None:
            op.set_additional_split_factor(k)

        logger.debug(f"Estimated num output blocks {estimated_num_blocks}")

        # Set the number of expected output blocks in the read input, so that
        # we can set the progress bar.
        assert len(op.input_dependencies) == 1
        up_op = op.input_dependencies[0]
        assert isinstance(up_op, InputDataBuffer)
        up_op._set_num_output_blocks(estimated_num_blocks)
