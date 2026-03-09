import math
from typing import Any, Callable, Dict, Tuple

from ray.serve.config import AutoscalingContext


class GangSchedulingAutoscalingPolicy:
    """Autoscaling policy that aligns replica counts to gang size multiples.

    When gang scheduling is enabled, the number of replicas must always be a
    multiple of gang_size so that complete gangs can be scheduled or released
    atomically. This policy wraps a base scaling policy (e.g.
    replica_queue_length_autoscaling_policy or user's custom policy) and snaps
    its decision to the nearest gang-aligned value:

    - Scaling up (desired >= current): Rounds up to the next multiple
      so the deployment has enough capacity.
    - Scaling down (desired < current): Rounds down to release only
      complete gangs.

    This class is not intended to be configured directly by users. It is
    automatically injected with a gang-scheduled deployment with autoscaling
    enabled.
    """

    def __init__(self, base_scaling_policy: Callable, gang_size: int):
        self._base_scaling_policy = base_scaling_policy
        self._gang_size = gang_size

    def __call__(self, ctx: AutoscalingContext) -> Tuple[int, Dict[str, Any]]:
        num_replicas, policy_state = self._base_scaling_policy(ctx)

        if self._gang_size > 1 and num_replicas > 0:
            current = ctx.current_num_replicas
            if num_replicas >= current:
                # Scaling up: round up so we have enough capacity
                num_replicas = (
                    math.ceil(num_replicas / self._gang_size) * self._gang_size
                )
            else:
                # Scaling down: round down to release complete gangs
                num_replicas = (num_replicas // self._gang_size) * self._gang_size  

        return num_replicas, policy_state
