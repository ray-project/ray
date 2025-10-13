import logging
import math
from typing import Optional, Tuple, Union

from ray import available_resources as ray_available_resources
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.interfaces import PhysicalPlan, Rule
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.util import _autodetect_parallelism
from ray.data.context import WARN_PREFIX, DataContext
from ray.data.datasource.datasource import Datasource, Reader

logger = logging.getLogger(__name__)


def compute_additional_split_factor(
    datasource_or_legacy_reader: Union[Datasource, Reader],
    parallelism: int,
    mem_size: int,
    target_max_block_size: Optional[int],
    cur_additional_split_factor: Optional[int] = None,
) -> Tuple[int, str, int, Optional[int]]:
    """Returns parallelism to use and the min safe parallelism to avoid OOMs."""

    ctx = DataContext.get_current()
    detected_parallelism, reason, _ = _autodetect_parallelism(
        parallelism, target_max_block_size, ctx, datasource_or_legacy_reader, mem_size
    )
    num_read_tasks = len(
        datasource_or_legacy_reader.get_read_tasks(detected_parallelism)
    )
    expected_block_size = None
    if mem_size:
        expected_block_size = mem_size / num_read_tasks
        logger.debug(
            f"Expected in-memory size {mem_size}," f" block size {expected_block_size}"
        )
        if target_max_block_size is None:
            # Unlimited block size -> no extra splits
            size_based_splits = 1
        else:
            size_based_splits = round(
                max(1, expected_block_size / target_max_block_size)
            )
    else:
        size_based_splits = 1
    if cur_additional_split_factor:
        size_based_splits *= cur_additional_split_factor
    logger.debug(f"Size based split factor {size_based_splits}")
    estimated_num_blocks = num_read_tasks * size_based_splits
    logger.debug(f"Blocks after size splits {estimated_num_blocks}")

    available_cpu_slots = ray_available_resources().get("CPU", 1)
    if (
        parallelism != -1
        and num_read_tasks >= available_cpu_slots * 4
        and num_read_tasks >= 5000
    ):
        logger.warning(
            f"{WARN_PREFIX} The requested number of read blocks of {parallelism} "
            "is more than 4x the number of available CPU slots in the cluster of "
            f"{available_cpu_slots}. This can "
            "lead to slowdowns during the data reading phase due to excessive "
            "task creation. Reduce the value to match with the available "
            "CPU slots in the cluster, or set override_num_blocks to -1 for Ray Data "
            "to automatically determine the number of read tasks blocks."
            "You can ignore this message if the cluster is expected to autoscale."
        )

    # Add more output splitting for each read task if needed.
    # TODO(swang): For parallelism=-1 (user did not explicitly set
    # parallelism), and if the following operator produces much larger blocks,
    # we should scale down the target max block size here instead of using
    # splitting, which can have higher memory usage.
    if estimated_num_blocks < detected_parallelism and estimated_num_blocks > 0:
        k = math.ceil(detected_parallelism / estimated_num_blocks)
        estimated_num_blocks = estimated_num_blocks * k
        return detected_parallelism, reason, estimated_num_blocks, k

    return detected_parallelism, reason, estimated_num_blocks, None


class SetReadParallelismRule(Rule):
    """
    This rule sets the read op's task parallelism based on the target block
    size, the requested parallelism, the number of read files, and the
    available resources in the cluster.

    If the parallelism is lower than requested, this rule also sets a split
    factor to split the output blocks of the read task, so that the following
    operator will have the desired parallelism.
    """

    def apply(self, plan: PhysicalPlan) -> PhysicalPlan:
        ops = [plan.dag]

        while len(ops) > 0:
            op = ops.pop(0)
            if isinstance(op, InputDataBuffer):
                continue
            logical_op = plan.op_map[op]
            if isinstance(logical_op, Read):
                self._apply(op, logical_op)
            ops += op.input_dependencies

        return plan

    def _apply(self, op: PhysicalOperator, logical_op: Read):
        estimated_in_mem_bytes = logical_op.infer_metadata().size_bytes

        (
            detected_parallelism,
            reason,
            estimated_num_blocks,
            k,
        ) = compute_additional_split_factor(
            logical_op._datasource_or_legacy_reader,
            logical_op._parallelism,
            estimated_in_mem_bytes,
            op.target_max_block_size_override or op.data_context.target_max_block_size,
            op._additional_split_factor,
        )

        if logical_op._parallelism == -1:
            assert reason != ""
            logger.debug(
                f"Using autodetected parallelism={detected_parallelism} "
                f"for operator {logical_op.name} to satisfy {reason}."
            )
        logical_op.set_detected_parallelism(detected_parallelism)

        if k is not None:
            logger.debug(
                f"To satisfy the requested parallelism of {detected_parallelism}, "
                f"each read task output is split into {k} smaller blocks."
            )

        if k is not None:
            op.set_additional_split_factor(k)

        logger.debug(f"Estimated num output blocks {estimated_num_blocks}")
