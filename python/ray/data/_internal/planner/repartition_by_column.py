import logging
from functools import partial
from typing import Any, Dict, List, Optional, Tuple, Union

from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    RefBundle,
    TaskContext,
)
from ray.data._internal.planner.exchange.repartition_task_scheduler import (
    RepartitionByColumnTaskScheduler,
)
from ray.data._internal.planner.exchange.repartition_task_spec import (
    RepartitionByColumnTaskSpec,
)
from ray.data._internal.stats import StatsDict

logger = logging.getLogger(__name__)


def generate_repartition_by_column_fn(
    keys: Union[str, List[str]],
    concurrency: Optional[int],
    ray_remote_args: Optional[Dict[str, Any]],
) -> AllToAllTransformFn:
    """Generate function to split blocks by the specified key column"""

    def fn(
        refs: List[RefBundle],
        ctx: TaskContext,
        keys: Union[str, List[str]],
        concurrency: Optional[int],
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[RefBundle], StatsDict]:
        repartition_task_spec = RepartitionByColumnTaskSpec(
            keys=keys,
            concurrency=concurrency,
        )
        scheduler = RepartitionByColumnTaskScheduler(repartition_task_spec)

        return scheduler.execute(
            refs=refs,
            output_num_blocks=-1,
            ctx=ctx,
            map_ray_remote_args=ray_remote_args,
            reduce_ray_remote_args=ray_remote_args,
        )

    return partial(
        fn,
        keys=keys,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )
