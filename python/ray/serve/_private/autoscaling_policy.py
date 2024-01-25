import logging
from typing import Optional

from ray.serve._private.common import TargetCapacityDirection
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.utils import get_capacity_adjusted_num_replicas
from ray.serve.config import AutoscalingConfig

logger = logging.getLogger(SERVE_LOGGER_NAME)


class AutoscalingPolicyManager:
    """Managing autoscaling policies and the lifecycle of the scaling function calls."""

    def __init__(self, config: Optional[AutoscalingConfig]):
        self.config = config
        self.policy = None
        self.policy_state = {}
        self._create_policy()

    def _create_policy(self):
        """Creates an autoscaling policy based on the given config."""
        if self.config:
            self.policy = self.config.get_policy()

    def should_autoscale(self) -> bool:
        """Returns whether autoscaling should be performed."""
        return self.policy is not None

    def get_decision_num_replicas(
        self,
        curr_target_num_replicas: int,
        total_num_requests: int,
        num_running_replicas: int,
        target_capacity: Optional[float] = None,
        target_capacity_direction: Optional[TargetCapacityDirection] = None,
        _skip_bound_check: bool = False,
    ) -> Optional[int]:
        """Interface with the autoscaling policy to get a decision to scale replicas.

        After the decision number of replicas is returned by the policy, it is then
        bounded by the bounds min and max adjusted by the target capacity and returned.
        If `_skip_bound_check` is True, then the bounds are not applied.
        """
        capacity_adjusted_min_replicas = self.get_current_lower_bound(
            target_capacity,
            target_capacity_direction,
        )
        capacity_adjusted_max_replicas = get_capacity_adjusted_num_replicas(
            self.config.max_replicas,
            target_capacity,
        )
        decision_num_replicas = self.policy(
            curr_target_num_replicas=curr_target_num_replicas,
            total_num_requests=total_num_requests,
            num_running_replicas=num_running_replicas,
            config=self.config,
            capacity_adjusted_min_replicas=capacity_adjusted_min_replicas,
            capacity_adjusted_max_replicas=capacity_adjusted_max_replicas,
            policy_state=self.policy_state,
        )

        if _skip_bound_check:
            return decision_num_replicas

        return self.apply_bounds(
            curr_target_num_replicas=decision_num_replicas,
            target_capacity=target_capacity,
            target_capacity_direction=target_capacity_direction,
        )

    def get_current_lower_bound(
        self,
        target_capacity: Optional[float] = None,
        target_capacity_direction: Optional[TargetCapacityDirection] = None,
    ) -> int:
        """Get the autoscaling lower bound, including target_capacity changes.

        The autoscaler uses initial_replicas scaled by target_capacity only
        if the target capacity direction is UP.
        """

        if self.config.initial_replicas is not None and (
            target_capacity_direction == TargetCapacityDirection.UP
        ):
            return get_capacity_adjusted_num_replicas(
                self.config.initial_replicas,
                target_capacity,
            )
        else:
            return get_capacity_adjusted_num_replicas(
                self.config.min_replicas,
                target_capacity,
            )

    def apply_bounds(
        self,
        curr_target_num_replicas: int,
        target_capacity: Optional[float] = None,
        target_capacity_direction: Optional[TargetCapacityDirection] = None,
    ) -> int:
        """Clips curr_target_num_replicas using the current bounds."""

        upper_bound = get_capacity_adjusted_num_replicas(
            self.config.max_replicas,
            target_capacity,
        )
        lower_bound = self.get_current_lower_bound(
            target_capacity,
            target_capacity_direction,
        )
        return max(lower_bound, min(upper_bound, curr_target_num_replicas))
