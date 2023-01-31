from typing import Any, Dict, List, Optional, Tuple

from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    RefBundle,
    TaskContext,
)
from ray.data._internal.planner.exchange.push_based_shuffle_task_scheduler import (
    PushBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.shuffle_task_spec import ShuffleTaskSpec
from ray.data._internal.planner.exchange.pull_based_shuffle_task_scheduler import (
    PullBasedShuffleTaskScheduler,
)
from ray.data._internal.stats import StatsDict
from ray.data.context import DatasetContext


def generate_random_shuffle_fn(
    seed: Optional[int],
    num_outputs: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> AllToAllTransformFn:
    """Generate function to randomly shuffle each records of blocks."""

    def fn(
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> Tuple[List[RefBundle], StatsDict]:
        num_input_blocks = sum(len(r.blocks) for r in refs)
        shuffle_spec = ShuffleTaskSpec(random_shuffle=True, random_seed=seed)

        if DatasetContext.get_current().use_push_based_shuffle:
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
            map_ray_remote_args=ray_remote_args,
            reduce_ray_remote_args=ray_remote_args,
        )

    return fn
