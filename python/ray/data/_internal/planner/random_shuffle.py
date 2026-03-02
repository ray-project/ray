import time
from typing import Any, Dict, List, Optional

from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.transform_fn import (
    AllToAllTransformFnResult,
)
from ray.data._internal.planner.exchange.interfaces import ExchangeTaskSpec
from ray.data._internal.planner.exchange.pull_based_shuffle_task_scheduler import (
    PullBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.push_based_shuffle_task_scheduler import (
    PushBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.shuffle_task_spec import ShuffleTaskSpec
from ray.data.context import DataContext, ShuffleStrategy
from ray.util.common import INT32_MAX


def _execute_sort_shuffle(
    refs: List[RefBundle],
    ctx: TaskContext,
    shuffle_spec: ExchangeTaskSpec,
    num_outputs: int,
    data_context: DataContext,
    map_ray_remote_args: Optional[Dict[str, Any]] = None,
    reduce_ray_remote_args: Optional[Dict[str, Any]] = None,
    _debug_limit_shuffle_execution_to_num_blocks: Optional[int] = None,
) -> AllToAllTransformFnResult:
    """Execute sort-based shuffle with the given spec. Used by planner and fusion."""
    num_input_blocks = sum(len(r.blocks) for r in refs)
    use_push_based = (
        data_context.shuffle_strategy == ShuffleStrategy.SORT_SHUFFLE_PUSH_BASED
        and num_outputs == num_input_blocks
    )
    if use_push_based:
        scheduler = PushBasedShuffleTaskScheduler(shuffle_spec)
    else:
        # Fall back to pull-based when push-based doesn't support the case
        # (e.g., repartition with custom num_blocks)
        scheduler = PullBasedShuffleTaskScheduler(shuffle_spec)
    return scheduler.execute(
        refs,
        num_outputs,
        task_ctx=ctx,
        map_ray_remote_args=map_ray_remote_args,
        reduce_ray_remote_args=reduce_ray_remote_args,
        _debug_limit_execution_to_num_blocks=_debug_limit_shuffle_execution_to_num_blocks,
    )


def generate_random_shuffle_fn(
    data_context: DataContext,
    seed: Optional[int],
    num_outputs: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    _debug_limit_shuffle_execution_to_num_blocks: Optional[int] = None,
) -> AllToAllTransformFn:
    """Generate function to randomly shuffle each records of blocks."""

    # If no seed has been specified, pin timestamp based one
    # so that task could be safely retried (w/o changing their output)
    seed = seed if seed is not None else (time.time_ns() % INT32_MAX)

    def fn(
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> AllToAllTransformFnResult:
        num_input_blocks = sum(len(r.blocks) for r in refs)
        shuffle_spec = ShuffleTaskSpec(
            target_shuffle_max_block_size=(
                ctx.target_max_block_size_override or data_context.target_max_block_size
            ),
            random_shuffle=True,
            random_seed=seed,
        )
        return _execute_sort_shuffle(
            refs,
            ctx,
            shuffle_spec,
            num_outputs or num_input_blocks,
            data_context,
            map_ray_remote_args=ray_remote_args,
            reduce_ray_remote_args=ray_remote_args,
            _debug_limit_shuffle_execution_to_num_blocks=(
                _debug_limit_shuffle_execution_to_num_blocks
            ),
        )

    return fn
