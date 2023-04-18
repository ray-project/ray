from typing import List, Tuple

from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    RefBundle,
    TaskContext,
)

from ray.data._internal.planner.exchange.fast_repartition_task_scheduler import (
    FastRepartitionTaskScheduler,
)
from ray.data._internal.planner.exchange.push_based_shuffle_task_scheduler import (
    PushBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.shuffle_task_spec import ShuffleTaskSpec
from ray.data._internal.planner.exchange.pull_based_shuffle_task_scheduler import (
    PullBasedShuffleTaskScheduler,
)
from ray.data._internal.stats import StatsDict
from ray.data.context import DataContext


def generate_repartition_fn(
    num_outputs: int,
    shuffle: bool,
) -> AllToAllTransformFn:
    """Generate function to partition each records of blocks."""

    def fn_shuffle_repartition(
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> Tuple[List[RefBundle], StatsDict]:
        shuffle_spec = ShuffleTaskSpec(random_shuffle=False)

        if DataContext.get_current().use_push_based_shuffle:
            scheduler = PushBasedShuffleTaskScheduler(shuffle_spec)
        else:
            scheduler = PullBasedShuffleTaskScheduler(shuffle_spec)

        return scheduler.execute(refs, num_outputs)

    def fn_fast_repartition(
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> Tuple[List[RefBundle], StatsDict]:
        shuffle_spec = ShuffleTaskSpec(random_shuffle=False)
        scheduler = FastRepartitionTaskScheduler(shuffle_spec)
        return scheduler.execute(refs, num_outputs)

    if shuffle:
        return fn_shuffle_repartition
    return fn_fast_repartition
