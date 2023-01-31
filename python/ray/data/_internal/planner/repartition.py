from typing import List, Tuple

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


def generate_repartition_fn(
    num_outputs: int,
    shuffle: bool,
) -> AllToAllTransformFn:
    """Generate function to randomly shuffle each records of blocks."""
    # TODO: support non-shuffle repartition as _internal/fast_repartition.py.
    assert shuffle, "Execution optimizer does not support non-shuffle repartition yet."

    def fn(
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> Tuple[List[RefBundle], StatsDict]:
        shuffle_spec = ShuffleTaskSpec(random_shuffle=False)

        if DatasetContext.get_current().use_push_based_shuffle:
            scheduler = PushBasedShuffleTaskScheduler(shuffle_spec)
        else:
            scheduler = PullBasedShuffleTaskScheduler(shuffle_spec)

        return scheduler.execute(refs, num_outputs)

    return fn
