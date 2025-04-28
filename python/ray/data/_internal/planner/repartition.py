from typing import List, Optional, Tuple

from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.operators.map_transformer import MapTransformer
from ray.data._internal.planner.exchange.pull_based_shuffle_task_scheduler import (
    PullBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.push_based_shuffle_task_scheduler import (
    PushBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.shuffle_task_spec import ShuffleTaskSpec
from ray.data._internal.planner.exchange.split_repartition_task_scheduler import (
    SplitRepartitionTaskScheduler,
)
from ray.data._internal.stats import StatsDict
from ray.data.context import DataContext, ShuffleStrategy


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
        # If map_transformer is specified (e.g. from fusing
        # MapOperator->AllToAllOperator), we pass a map function which
        # is applied to each block before shuffling.
        map_transformer: Optional["MapTransformer"] = ctx.upstream_map_transformer
        upstream_map_fn = None
        if map_transformer:
            # NOTE(swang): We override the target block size with infinity, to
            # prevent the upstream map from slicing its output into smaller
            # blocks. Since the shuffle task will just fuse these back
            # together, the extra slicing and re-fusing can add high memory
            # overhead. This can be removed once dynamic block splitting is
            # supported for all-to-all ops.
            # See https://github.com/ray-project/ray/issues/40518.
            map_transformer.set_target_max_block_size(float("inf"))

            def upstream_map_fn(blocks):
                return map_transformer.apply_transform(blocks, ctx)

        shuffle_spec = ShuffleTaskSpec(
            ctx.target_max_block_size,
            random_shuffle=False,
            upstream_map_fn=upstream_map_fn,
        )

        if data_context.shuffle_strategy == ShuffleStrategy.SORT_SHUFFLE_PUSH_BASED:
            scheduler = PushBasedShuffleTaskScheduler(shuffle_spec)
        else:
            scheduler = PullBasedShuffleTaskScheduler(shuffle_spec)

        return scheduler.execute(
            refs,
            num_outputs,
            ctx,
            _debug_limit_execution_to_num_blocks=(
                _debug_limit_shuffle_execution_to_num_blocks
            ),
        )

    def split_repartition_fn(
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> Tuple[List[RefBundle], StatsDict]:
        shuffle_spec = ShuffleTaskSpec(ctx.target_max_block_size, random_shuffle=False)
        scheduler = SplitRepartitionTaskScheduler(shuffle_spec)
        return scheduler.execute(refs, num_outputs, ctx)

    if shuffle:
        return shuffle_repartition_fn
    return split_repartition_fn
