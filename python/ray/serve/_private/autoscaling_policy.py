import logging
import time
from threading import Thread
from typing import Callable, List, Optional

from ray.serve._private.common import TargetCapacityDirection
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.utils import get_capacity_adjusted_num_replicas
from ray.serve.autoscaling_policy import AutoscalingContext
from ray.serve.config import AutoscalingConfig

logger = logging.getLogger(SERVE_LOGGER_NAME)


class ThreadManager:
    def __init__(self, func: Callable):
        self.func = func
        self.decision_num_replicas = None
        self.thread = None
        self.start_time = time.time()
        self.exponential_backoff = 0

    def __call__(self, context: AutoscalingContext):
        self.start_time = time.time()
        try:
            self.decision_num_replicas = self.func(context)
            self.exponential_backoff = 0
        except Exception as e:
            self.exponential_backoff = self.exponential_backoff * 2 + 1
            logger.warning(
                f"Autoscaling call throw exception: {e}. \n"
                f"exponential_backoff {self.exponential_backoff}s before retry."
            )

    def done(self):
        return self.thread is not None and not self.thread.is_alive()

    def _should_start_thread(self):
        return (
            self.thread is None
            and time.time() - self.start_time >= self.exponential_backoff
        )

    def get_decision_num_replicas(self, context: AutoscalingContext) -> Optional[int]:
        if self._should_start_thread():
            self.thread = Thread(target=self, kwargs={"context": context})
            self.thread.start()

        if self.done():
            self.thread = None
            return self.decision_num_replicas


class AutoscalingPolicyManager:
    """Managing autoscaling policies and the lifecycle of the scaling function calls."""

    def __init__(self, config: Optional[AutoscalingConfig]):
        self.config = config
        self.context = AutoscalingContext(config=config)
        self.policy = None
        self.thread_manager = None
        self._create_policy()

    def _create_policy(self):
        """Creates an autoscaling policy based on the given config."""
        if self.config:
            self.policy = self.config.get_policy()
            self.thread_manager = ThreadManager(self.policy)

    def should_autoscale(self) -> bool:
        """Returns whether autoscaling should be performed."""
        return self.policy is not None

    def get_decision_num_replicas(
        self,
        curr_target_num_replicas: int,
        current_num_ongoing_requests: List[float],
        current_handle_queued_queries: float,
        target_capacity: Optional[float] = None,
        target_capacity_direction: Optional[TargetCapacityDirection] = None,
    ) -> Optional[int]:
        """Interface with the autoscaling policy to get a decision to scale replicas.
        If the autoscaling policy is not ready or returning the same number as the
        current replica number, return None to not execute autoscaling.
        """
        capacity_adjusted_min_replicas = self.get_current_lower_bound(
            target_capacity,
            target_capacity_direction,
        )
        capacity_adjusted_max_replicas = get_capacity_adjusted_num_replicas(
            self.config.max_replicas,
            target_capacity,
        )
        self.context.update(
            curr_target_num_replicas=curr_target_num_replicas,
            current_num_ongoing_requests=current_num_ongoing_requests,
            current_handle_queued_queries=current_handle_queued_queries,
            capacity_adjusted_min_replicas=capacity_adjusted_min_replicas,
            capacity_adjusted_max_replicas=capacity_adjusted_max_replicas,
        )
        # Note: The thread manager will continuously return None until a decision is
        # made. We do not currently force kill and restart the scaling call due to
        # complexities. We will add docs to warn users not to make long-running calls
        # here, and they can set timeout in their policies if needed.
        decision_num_replicas = self.thread_manager.get_decision_num_replicas(
            context=self.context,
        )
        if decision_num_replicas is not None:
            self.context.last_scale_time = time.time()
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
            target_capacity, target_capacity_direction
        )
        return max(lower_bound, min(upper_bound, curr_target_num_replicas))

    def is_within_bounds(
        self,
        num_replicas_running_at_target_version: int,
        target_capacity: float,
        target_capacity_direction: TargetCapacityDirection,
    ) -> bool:
        assert self.config is not None

        lower_bound = self.get_current_lower_bound(
            target_capacity,
            target_capacity_direction,
        )
        upper_bound = get_capacity_adjusted_num_replicas(
            self.config.max_replicas,
            target_capacity,
        )

        return lower_bound <= num_replicas_running_at_target_version <= upper_bound
