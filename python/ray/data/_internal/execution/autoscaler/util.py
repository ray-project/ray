import math
from typing import Optional

from .autoscaling_actor_pool import AutoscalingActorPool
from ray.data._internal.execution.interfaces import ExecutionResources


def get_max_scale_up(
    actor_pool: AutoscalingActorPool,
    budget: Optional[ExecutionResources],
) -> Optional[int]:
    """Get the maximum number of actors that can be scaled up.

    Args:
        actor_pool: The actor pool to scale up.
        budget: The budget to scale up.

    Returns:
        The maximum number of actors that can be scaled up, or `None` if you can
        scale up infinitely.
    """
    if budget is None:
        return None

    assert budget.cpu >= 0 and budget.gpu >= 0

    num_cpus_per_actor = actor_pool.per_actor_resource_usage().cpu
    num_gpus_per_actor = actor_pool.per_actor_resource_usage().gpu
    assert num_cpus_per_actor >= 0 and num_gpus_per_actor >= 0

    max_cpu_scale_up: float = float("inf")
    if num_cpus_per_actor > 0 and not math.isinf(budget.cpu):
        max_cpu_scale_up = budget.cpu // num_cpus_per_actor

    max_gpu_scale_up: float = float("inf")
    if num_gpus_per_actor > 0 and not math.isinf(budget.gpu):
        max_gpu_scale_up = budget.gpu // num_gpus_per_actor

    max_scale_up = min(max_cpu_scale_up, max_gpu_scale_up)
    if math.isinf(max_scale_up):
        return None
    else:
        assert not math.isnan(max_scale_up), (
            budget,
            num_cpus_per_actor,
            num_gpus_per_actor,
        )
        return int(max_scale_up)
