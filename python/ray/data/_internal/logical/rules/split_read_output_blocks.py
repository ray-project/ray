import logging
import math

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.interfaces import PhysicalPlan, Rule
from ray.data._internal.logical.operators.read_operator import Read

logger = logging.getLogger(__name__)


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
        num_read_tasks = len(logical_op._reader.get_read_tasks(logical_op._parallelism))
        expected_block_size = None
        if logical_op._mem_size:
            expected_block_size = logical_op._mem_size / num_read_tasks
            logger.warn(
                f"Expected in-memory size {logical_op._mem_size},"
                f" block size {expected_block_size}"
            )
            size_based_splits = round(
                max(1, expected_block_size / op.actual_target_max_block_size)
            )
        else:
            size_based_splits = 1
        if op._additional_split_factor:
            size_based_splits *= op._additional_split_factor
        logger.warn(f"Size based split factor {size_based_splits}")
        estimated_num_blocks = num_read_tasks * size_based_splits
        logger.warn(f"Blocks after size splits {estimated_num_blocks}")

        # Add more output splitting for each read task if needed.
        if estimated_num_blocks < logical_op._parallelism:
            k = math.ceil(logical_op._parallelism / estimated_num_blocks)
            logger.info(
                f"To satisfy the requested parallelism of {logical_op._parallelism}, "
                f"each read task output is split into {k} smaller blocks."
            )
            estimated_num_blocks = estimated_num_blocks * k
            op.set_additional_split_factor(k)

        logger.warn(f"Estimated num output blocks {estimated_num_blocks}")

        # Set the number of expected output blocks in the read input, so that
        # we can set the progress bar.
        assert len(op.input_dependencies) == 1
        up_op = op.input_dependencies[0]
        assert isinstance(up_op, InputDataBuffer)
        up_op._set_num_output_blocks(estimated_num_blocks)
