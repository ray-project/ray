from typing import Any, Dict, List, Optional, Tuple

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
from ray.data._internal.stats import StatsDict
from ray.data.context import DataContext


def generate_random_shuffle_fn(
    seed: Optional[int],
    num_outputs: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    _debug_limit_shuffle_execution_to_num_blocks: Optional[int] = None,
) -> AllToAllTransformFn:
    """Generate function to randomly shuffle each records of blocks."""

    def fn(
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> Tuple[List[RefBundle], StatsDict]:
        num_input_blocks = sum(len(r.blocks) for r in refs)

        # If map_transformer is specified (e.g. from fusing
        # MapOperator->AllToAllOperator), we pass a map function which
        # is applied to each block before shuffling.
        map_transformer: Optional[MapTransformer] = ctx.upstream_map_transformer
        upstream_map_fn = None
        nonlocal ray_remote_args
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

            # If there is a fused upstream operator,
            # also use the ray_remote_args from the fused upstream operator.
            ray_remote_args = ctx.upstream_map_ray_remote_args

        shuffle_spec = ShuffleTaskSpec(
            ctx.target_max_block_size,
            random_shuffle=True,
            random_seed=seed,
            upstream_map_fn=upstream_map_fn,
        )

        if DataContext.get_current().use_push_based_shuffle:
            if num_outputs is not None:
                raise NotImplementedError(
                    "Push-based shuffle doesn't support setting num_blocks yet."
                )
            scheduler = PushBasedShuffleTaskScheduler(shuffle_spec)
        else:
            scheduler = PullBasedShuffleTaskScheduler(shuffle_spec)

        return scheduler.execute(
            refs,
            num_outputs or num_input_blocks,
            task_ctx=ctx,
            map_ray_remote_args=ray_remote_args,
            reduce_ray_remote_args=ray_remote_args,
            _debug_limit_execution_to_num_blocks=(
                _debug_limit_shuffle_execution_to_num_blocks
            ),
        )

    return fn
