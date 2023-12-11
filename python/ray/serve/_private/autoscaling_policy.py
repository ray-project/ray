import logging
from threading import Thread
from typing import Callable, List, Optional

from ray.serve._private.common import TargetCapacityDirection
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.utils import get_capacity_adjusted_num_replicas
from ray.serve.autoscaling_policy import AutoscalingContext
from ray.serve.config import AutoscalingConfig

logger = logging.getLogger(SERVE_LOGGER_NAME)


class CustomScalingPolicy:
    """A custom autoscaling policy to handle user specified scaling logic."""

    def __init__(self, config: AutoscalingConfig):
        self.config = config
        self.custom_scaling_actor_handle = None
        self.custom_scaling_remote_func = None
        self.custom_scaling_ref = None

    #     self._setup_remote_callable()
    #
    # def _setup_remote_callable(self):
    #     """Set up the remote callable for the custom scaling policy.
    #     If the custom scaling policy is a class, then create an actor handle for it.
    #     Else, create a remote function for it.
    #     """
    #     autoscaling_policy_callable = self.config.get_autoscaling_policy()
    #     if isclass(autoscaling_policy_callable):
    #         assert issubclass(autoscaling_policy_callable, AutoscalingPolicy)
    #
    #         self.custom_scaling_actor_handle = ray.remote(
    #             autoscaling_policy_callable
    #         ).remote(self.config)
    #     else:
    #         self.custom_scaling_remote_func = ray.remote(autoscaling_policy_callable)

    # def get_custom_scaling_ref(self, autoscaling_context: AutoscalingContext):
    #     """Get the custom scaling reference.
    #     If the custom scaling policy is a class, then call get_decision_num_replicas()
    #     Else, call the remote function.
    #     """
    #     if self.custom_scaling_actor_handle:
    #         return self.custom_scaling_actor_handle.get_decision_num_replicas.remote(
    #             autoscaling_context
    #         )
    #
    #     return self.custom_scaling_remote_func.remote(autoscaling_context)

    # def get_decision_num_replicas(
    #     self, autoscaling_context: AutoscalingContext
    # ) -> Optional[int]:
    #     """Make a decision to scale replicas.
    #     Returns the new number of replicas to scale to. Or None if the custom scaling
    #     function call is not finished yet, finished but not returning an integer or
    #     None, or throw exception.
    #     """
    #     # TODO (genesu): reimplement this using threading.Event
    #
    #     if self.custom_scaling_ref is None:
    #         # TODO (genesu): add a timeout for this
    #         self.custom_scaling_ref = self.get_custom_scaling_ref(autoscaling_context)
    #
    #     finished, _ = ray.wait([self.custom_scaling_ref], timeout=0)
    #     try:
    #         if self.custom_scaling_ref in finished:
    #             decision_num_replicas = ray.get(self.custom_scaling_ref)
    #             self.custom_scaling_ref = None
    #             if (
    #                 isinstance(decision_num_replicas, int)
    #                 and decision_num_replicas
    #                 != autoscaling_context.curr_target_num_replicas
    #             ):
    #                 return decision_num_replicas
    #             elif not isinstance(decision_num_replicas, (int, type(None))):
    #                 logger.error(
    #                     "Custom scaling policy must return an integer or None. "
    #                     f"Received type {type(decision_num_replicas)}, "
    #                     f"for {decision_num_replicas}."
    #                 )
    #     except Exception as e:
    #         # TODO (genesu): add exponential backoff for this
    #         logger.error(f"Error in custom scaling policy:\n{e}")
    #         self.custom_scaling_ref = None
    #
    #     return None


class ThreadManager:
    def __init__(self, func: Callable):
        self.func = func
        self.get_decision_num_replicas = None
        self.thread = None

    def __call__(self, context: AutoscalingContext):
        self.get_decision_num_replicas = self.func(context)

    def done(self):
        return self.thread is not None and not self.thread.is_alive()

    def get_decision_num_replicas(self, context: AutoscalingContext) -> Optional[int]:
        if self.thread is None:
            self.thread = Thread(target=self, kwargs={"context": context})
            self.thread.start()

        if self.done():
            return self.get_decision_num_replicas


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
            # if self.config.get_policy() != BasicAutoscalingPolicy:
            #     self.policy = CustomScalingPolicy(self.config)
            # else:
            #     self.policy = BasicAutoscalingPolicy(self.config)

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
        override_min_replicas = self.get_current_lower_bound(
            target_capacity,
            target_capacity_direction,
        )
        self.context.update(
            curr_target_num_replicas=curr_target_num_replicas,
            current_num_ongoing_requests=current_num_ongoing_requests,
            current_handle_queued_queries=current_handle_queued_queries,
            override_min_replicas=override_min_replicas,
            target_capacity=target_capacity,
        )
        # self.ThreadManager.get_decision_num_replicas()

        decision_num_replicas = self.policy(self.context)

        return decision_num_replicas

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
