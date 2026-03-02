from typing import List, Optional, Tuple

from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.transform_fn import (
    AllToAllTransformFnResult,
)
from ray.data._internal.planner.exchange.shuffle_task_spec import ShuffleTaskSpec
from ray.data._internal.planner.exchange.split_repartition_task_scheduler import (
    SplitRepartitionTaskScheduler,
)
from ray.data._internal.planner.random_shuffle import _execute_sort_shuffle
from ray.data._internal.stats import StatsDict
from ray.data.context import DataContext


def generate_repartition_fn(
    num_outputs: int,
    shuffle: bool,
    data_context: DataContext,
    _debug_limit_shuffle_execution_to_num_blocks: Optional[int] = None,
) -> AllToAllTransformFn:
    """Generate function to partition each records of blocks."""

    def shuffle_repartition_fn(
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> Tuple[List[RefBundle], StatsDict]:
        shuffle_spec = ShuffleTaskSpec(
            target_shuffle_max_block_size=(
                ctx.target_max_block_size_override or data_context.target_max_block_size
            ),
            random_shuffle=False,
        )
        return _execute_sort_shuffle(
            refs,
            ctx,
            shuffle_spec,
            num_outputs,
            data_context,
            _debug_limit_shuffle_execution_to_num_blocks=(
                _debug_limit_shuffle_execution_to_num_blocks
            ),
        )

    def split_repartition_fn(
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> AllToAllTransformFnResult:
        shuffle_spec = ShuffleTaskSpec(
            target_shuffle_max_block_size=(
                ctx.target_max_block_size_override or data_context.target_max_block_size
            ),
            random_shuffle=False,
        )
        scheduler = SplitRepartitionTaskScheduler(shuffle_spec)
        return scheduler.execute(refs, num_outputs, ctx)

    if shuffle:
        return shuffle_repartition_fn
    return split_repartition_fn
