import math
from typing import Optional, Tuple

from ray import available_resources as ray_available_resources
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.interfaces import PhysicalPlan, Rule
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.util import _autodetect_parallelism
from ray.data.context import WARN_PREFIX, DataContext
from ray.data.datasource.datasource import Reader

logger = DatasetLogger(__name__)


def compute_additional_split_factor(
    reader: Reader,
    parallelism: int,
    mem_size: int,
    target_max_block_size: int,
    cur_additional_split_factor: Optional[int] = None,
) -> Tuple[int, str, int, Optional[int]]:
    ctx = DataContext.get_current()
    parallelism, reason, _, _ = _autodetect_parallelism(
        parallelism, target_max_block_size, ctx, reader, mem_size
    )
    num_read_tasks = len(reader.get_read_tasks(parallelism))
    expected_block_size = None
    if mem_size:
        expected_block_size = mem_size / num_read_tasks
        logger.get_logger().debug(
            f"Expected in-memory size {mem_size}," f" block size {expected_block_size}"
        )
        size_based_splits = round(max(1, expected_block_size / target_max_block_size))
    else:
        size_based_splits = 1
    if cur_additional_split_factor:
        size_based_splits *= cur_additional_split_factor
    logger.get_logger().debug(f"Size based split factor {size_based_splits}")
    estimated_num_blocks = num_read_tasks * size_based_splits
    logger.get_logger().debug(f"Blocks after size splits {estimated_num_blocks}")

    available_cpu_slots = ray_available_resources().get("CPU", 1)
    if (
        parallelism
        and num_read_tasks >= available_cpu_slots * 4
        and num_read_tasks >= 5000
    ):
        logger.get_logger().warn(
            f"{WARN_PREFIX} The requested parallelism of {parallelism} "
            "is more than 4x the number of available CPU slots in the cluster of "
            f"{available_cpu_slots}. This can "
            "lead to slowdowns during the data reading phase due to excessive "
            "task creation. Reduce the parallelism to match with the available "
            "CPU slots in the cluster, or set parallelism to -1 for Ray Data "
            "to automatically determine the parallelism. "
            "You can ignore this message if the cluster is expected to autoscale."
        )

    # Add more output splitting for each read task if needed.
    # TODO(swang): For parallelism=-1 (user did not explicitly set
    # parallelism), and if the following stage produces much larger blocks,
    # we should scale down the target max block size here instead of using
    # splitting, which can have higher memory usage.
    if estimated_num_blocks < parallelism and estimated_num_blocks > 0:
        k = math.ceil(parallelism / estimated_num_blocks)
        estimated_num_blocks = estimated_num_blocks * k
        return parallelism, reason, estimated_num_blocks, k

    return parallelism, reason, estimated_num_blocks, None


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
        (
            detected_parallelism,
            reason,
            estimated_num_blocks,
            k,
        ) = compute_additional_split_factor(
            logical_op._reader,
            logical_op._parallelism,
            logical_op._mem_size,
            op.actual_target_max_block_size,
            op._additional_split_factor,
        )
        if logical_op._parallelism == -1:
            assert reason != ""
            logger.get_logger().info(
                f"Using autodetected parallelism={detected_parallelism} "
                f"for stage {logical_op.name} to satisfy {reason}."
            )
        if k is not None:
            logger.get_logger().info(
                f"To satisfy the requested parallelism of {detected_parallelism}, "
                f"each read task output is split into {k} smaller blocks."
            )

        if k is not None:
            op.set_additional_split_factor(k)

        logger.get_logger().debug(f"Estimated num output blocks {estimated_num_blocks}")

        # Set the number of expected output blocks in the read input, so that
        # we can set the progress bar.
        assert len(op.input_dependencies) == 1
        up_op = op.input_dependencies[0]
        assert isinstance(up_op, InputDataBuffer)
        up_op._set_num_output_blocks(estimated_num_blocks)
