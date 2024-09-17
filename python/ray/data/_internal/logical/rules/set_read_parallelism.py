import logging
from typing import Tuple, Union

from ray import available_resources as ray_available_resources
from ray.data._internal.logical.interfaces import Rule, LogicalPlan
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.util import _autodetect_parallelism
from ray.data.context import WARN_PREFIX, DataContext
from ray.data.datasource.datasource import Datasource, Reader

logger = logging.getLogger(__name__)


def compute_additional_split_factor(
    datasource_or_legacy_reader: Union[Datasource, Reader],
    parallelism: int,
    mem_size: int,
    target_max_block_size: int,
) -> Tuple[int, str, int]:
    ctx = DataContext.get_current()
    detected_parallelism, reason, _ = _autodetect_parallelism(
        parallelism,
        target_max_block_size,
        ctx,
        datasource_or_legacy_reader=datasource_or_legacy_reader,
        mem_size=mem_size,
    )
    num_read_tasks = len(
        datasource_or_legacy_reader.get_read_tasks(detected_parallelism)
    )

    if mem_size:
        expected_block_size = mem_size / num_read_tasks
        logger.debug(
            f"Expected in-memory size {mem_size}," f" block size {expected_block_size}"
        )
        size_based_splits = round(max(1, expected_block_size / target_max_block_size))
    else:
        size_based_splits = 1

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

    return detected_parallelism, reason, estimated_num_blocks


class SetReadParallelismRule(Rule):
    """
    This rule sets the read op's task parallelism based on the target block
    size, the requested parallelism, the number of read files, and the
    available resources in the cluster.

    If the parallelism is lower than requested, this rule also sets a split
    factor to split the output blocks of the read task, so that the following
    operator will have the desired parallelism.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        for op in plan.dag.post_order_iter():
            if isinstance(op, Read):
                # TODO avoid modifying the plan in place, instead return new plan
                self._apply(op)

            # TODO replace original Read-op plan with Repartition

        return plan

    def _apply(self, logical_op: Read):
        (
            detected_parallelism,
            reason,
            estimated_num_blocks,
        ) = compute_additional_split_factor(
            logical_op._datasource_or_legacy_reader,
            logical_op._parallelism,
            logical_op._mem_size,
            DataContext.target_max_block_size,
        )

        if logical_op._parallelism == -1:
            assert reason != ""
            logger.debug(
                f"Using autodetected parallelism={detected_parallelism} "
                f"for operator {logical_op.name} to satisfy {reason}."
            )

        # TODO avoid propagating parallelism t/h logical op
        logical_op.set_detected_parallelism(detected_parallelism)

        if estimated_num_blocks > 0 and estimated_num_blocks != detected_parallelism:
            logical_op.set_target_num_blocks(estimated_num_blocks)

            logger.debug(
                f"Deduced parallelism of {detected_parallelism} for {logical_op.name}, "
                f"producing {estimated_num_blocks}."
            )
