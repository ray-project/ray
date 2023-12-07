import logging
import math
from inspect import isclass
from typing import List, Optional

import ray
from ray.serve._private.constants import CONTROL_LOOP_PERIOD_S, SERVE_LOGGER_NAME
from ray.serve._private.utils import get_capacity_adjusted_num_replicas
from ray.serve.autoscaling_policy import (
    AutoscalingContext,
    AutoscalingPolicy,
    TargetCapacityDirection,
)
from ray.serve.config import AutoscalingConfig

logger = logging.getLogger(SERVE_LOGGER_NAME)


def calculate_desired_num_replicas(
    autoscaling_config: AutoscalingConfig,
    current_num_ongoing_requests: List[float],
    override_min_replicas: Optional[float] = None,
    override_max_replicas: Optional[float] = None,
) -> int:  # (desired replicas):
    """Returns the number of replicas to scale to based on the given metrics.

    Args:
        autoscaling_config: The autoscaling parameters to use for this
            calculation.
        current_num_ongoing_requests (List[float]): A list of the number of
            ongoing requests for each replica.  Assumes each entry has already
            been time-averaged over the desired lookback window.
        override_min_replicas: Overrides min_replicas from the config
            when calculating the final number of replicas.
        override_max_replicas: Overrides max_replicas from the config
            when calculating the final number of replicas.

    Returns:
        desired_num_replicas: The desired number of replicas to scale to, based
            on the input metrics and the current number of replicas.

    """
    current_num_replicas = len(current_num_ongoing_requests)
    if current_num_replicas == 0:
        raise ValueError("Number of replicas cannot be zero")

    # The number of ongoing requests per replica, averaged over all replicas.
    num_ongoing_requests_per_replica: float = sum(current_num_ongoing_requests) / len(
        current_num_ongoing_requests
    )

    # Example: if error_ratio == 2.0, we have two times too many ongoing
    # requests per replica, so we desire twice as many replicas.
    error_ratio: float = (
        num_ongoing_requests_per_replica
        / autoscaling_config.target_num_ongoing_requests_per_replica
    )

    # If error ratio >= 1, then the number of ongoing requests per
    # replica exceeds the target and we will make an upscale decision,
    # so we apply the upscale smoothing factor. Otherwise, the number of
    # ongoing requests per replica is lower than the target and we will
    # make a downscale decision, so we apply the downscale smoothing
    # factor.
    if error_ratio >= 1:
        smoothing_factor = autoscaling_config.get_upscale_smoothing_factor()
    else:
        smoothing_factor = autoscaling_config.get_downscale_smoothing_factor()

    # Multiply the distance to 1 by the smoothing ("gain") factor (default=1).
    smoothed_error_ratio = 1 + ((error_ratio - 1) * smoothing_factor)
    desired_num_replicas = math.ceil(current_num_replicas * smoothed_error_ratio)

    # If error_ratio = 0, meaning there is no more traffic, and desired
    # num replicas is stuck at a positive number due to the math.ceil
    # above, decrease desired_num_replicas by one so that the deployment
    # can eventually scale to 0.
    if (
        error_ratio == 0
        and desired_num_replicas == current_num_replicas
        and desired_num_replicas >= 1
    ):
        desired_num_replicas -= 1

    min_replicas = autoscaling_config.min_replicas
    max_replicas = autoscaling_config.max_replicas
    if override_min_replicas is not None:
        min_replicas = override_min_replicas
    if override_max_replicas is not None:
        max_replicas = override_max_replicas

    # Ensure scaled_min_replicas <= desired_num_replicas <= scaled_max_replicas.
    desired_num_replicas = max(min_replicas, min(max_replicas, desired_num_replicas))

    return desired_num_replicas


class BasicAutoscalingPolicy(AutoscalingPolicy):
    """The default autoscaling policy based on basic thresholds for scaling.
    There is a minimum threshold for the average queue length in the cluster
    to scale up and a maximum threshold to scale down. Each period, a 'scale
    up' or 'scale down' decision is made. This decision must be made for a
    specified number of periods in a row before the number of replicas is
    actually scaled. See config options for more details.  Assumes
    `get_decision_num_replicas` is called once every CONTROL_LOOP_PERIOD_S
    seconds.
    """

    def __init__(self, config: AutoscalingConfig):
        self.config = config
        # TODO(architkulkarni): Make configurable via AutoscalingConfig
        self.loop_period_s = CONTROL_LOOP_PERIOD_S
        self.scale_up_consecutive_periods = int(
            config.upscale_delay_s / self.loop_period_s
        )
        self.scale_down_consecutive_periods = int(
            config.downscale_delay_s / self.loop_period_s
        )

        # Keeps track of previous decisions. Each time the load is above
        # 'scale_up_threshold', the counter is incremented and each time it is
        # below 'scale_down_threshold', the counter is decremented. When the
        # load is between the thresholds or a scaling decision is made, the
        # counter is reset to 0.
        # TODO(architkulkarni): It may be too noisy to reset the counter each
        # time the direction changes, especially if we calculate frequently
        # (like every 0.1s).  A potentially less noisy option is to not reset
        # the counter, and instead only increment/decrement until we reach
        # scale_up_periods or scale_down_periods.
        self.decision_counter = 0

    def get_decision_num_replicas(self, autoscaling_context: AutoscalingContext) -> int:
        if len(autoscaling_context.current_num_ongoing_requests) == 0:
            # When 0 replicas and queries are queued, scale up the replicas
            if autoscaling_context.current_handle_queued_queries > 0:
                return max(
                    math.ceil(1 * self.config.get_upscale_smoothing_factor()),
                    autoscaling_context.curr_target_num_replicas,
                )
            return autoscaling_context.curr_target_num_replicas

        decision_num_replicas = autoscaling_context.curr_target_num_replicas

        desired_num_replicas = calculate_desired_num_replicas(
            self.config,
            autoscaling_context.current_num_ongoing_requests,
            override_min_replicas=self.get_current_lower_bound(
                autoscaling_context.target_capacity,
                autoscaling_context.target_capacity_direction,
            ),
            override_max_replicas=get_capacity_adjusted_num_replicas(
                self.config.max_replicas,
                autoscaling_context.target_capacity,
            ),
        )
        # Scale up.
        if desired_num_replicas > autoscaling_context.curr_target_num_replicas:
            # If the previous decision was to scale down (the counter was
            # negative), we reset it and then increment it (set to 1).
            # Otherwise, just increment.
            if self.decision_counter < 0:
                self.decision_counter = 0
            self.decision_counter += 1

            # Only actually scale the replicas if we've made this decision for
            # 'scale_up_consecutive_periods' in a row.
            if self.decision_counter > self.scale_up_consecutive_periods:
                self.decision_counter = 0
                decision_num_replicas = desired_num_replicas

        # Scale down.
        elif desired_num_replicas < autoscaling_context.curr_target_num_replicas:
            # If the previous decision was to scale up (the counter was
            # positive), reset it to zero before decrementing.
            if self.decision_counter > 0:
                self.decision_counter = 0
            self.decision_counter -= 1

            # Only actually scale the replicas if we've made this decision for
            # 'scale_down_consecutive_periods' in a row.
            if self.decision_counter < -self.scale_down_consecutive_periods:
                self.decision_counter = 0
                decision_num_replicas = desired_num_replicas

        # Do nothing.
        else:
            self.decision_counter = 0

        return decision_num_replicas

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


class CustomScalingPolicy(AutoscalingPolicy):
    """A custom autoscaling policy to handle user specified scaling logic."""

    def __init__(self, config: AutoscalingConfig):
        self.config = config
        self.custom_scaling_actor_handle = None
        self.custom_scaling_remote_func = None
        self.custom_scaling_ref = None
        self._setup_remote_callable()

    def _setup_remote_callable(self):
        """Set up the remote callable for the custom scaling policy.

        If the custom scaling policy is a class, then create an actor handle for it.
        Else, create a remote function for it.
        """
        autoscaling_policy_callable = self.config.get_autoscaling_policy()
        if isclass(autoscaling_policy_callable):
            assert issubclass(autoscaling_policy_callable, AutoscalingPolicy)

            self.custom_scaling_actor_handle = ray.remote(
                autoscaling_policy_callable
            ).remote(self.config)
        else:
            self.custom_scaling_remote_func = ray.remote(autoscaling_policy_callable)

    def get_custom_scaling_ref(self, autoscaling_context: AutoscalingContext):
        """Get the custom scaling reference.

        If the custom scaling policy is a class, then call get_decision_num_replicas().
        Else, call the remote function.
        """
        if self.custom_scaling_actor_handle:
            return self.custom_scaling_actor_handle.get_decision_num_replicas.remote(
                autoscaling_context
            )

        return self.custom_scaling_remote_func.remote(autoscaling_context)

    def get_decision_num_replicas(
        self, autoscaling_context: AutoscalingContext
    ) -> Optional[int]:
        """Make a decision to scale replicas.

        Returns the new number of replicas to scale to. Or None if the custom scaling
        function call is not finished yet, finished but not returning an integer or
        None, or throw exception.
        """

        if self.custom_scaling_ref is None:
            # TODO (genesu): add a timeout for this
            self.custom_scaling_ref = self.get_custom_scaling_ref(autoscaling_context)

        finished, _ = ray.wait([self.custom_scaling_ref], timeout=0)
        try:
            if self.custom_scaling_ref in finished:
                decision_num_replicas = ray.get(self.custom_scaling_ref)
                self.custom_scaling_ref = None
                if (
                    isinstance(decision_num_replicas, int)
                    and decision_num_replicas
                    != autoscaling_context.curr_target_num_replicas
                ):
                    return decision_num_replicas
                elif not isinstance(decision_num_replicas, (int, type(None))):
                    logger.error(
                        "Custom scaling policy must return an integer or None. "
                        f"Received type {type(decision_num_replicas)}, "
                        f"for {decision_num_replicas}."
                    )
        except Exception as e:
            # TODO (genesu): add exponential backoff for this
            logger.error(f"Error in custom scaling policy:\n{e}")
            self.custom_scaling_ref = None

        return None


class AutoscalingPolicyManager:
    """Managing autoscaling policies and the lifecycle of the scaling function calls."""

    def __init__(self, config: Optional[AutoscalingConfig]):
        self.config = config
        self.autoscaling_policy = None
        self._create_policy()

    def _create_policy(self):
        """Creates an autoscaling policy based on the given config."""
        if self.config:
            if self.config.get_autoscaling_policy() != BasicAutoscalingPolicy:
                self.autoscaling_policy = CustomScalingPolicy(self.config)
            else:
                self.autoscaling_policy = BasicAutoscalingPolicy(self.config)

    def should_autoscale(self) -> bool:
        """Returns whether autoscaling should be performed."""
        return self.config is not None

    def get_decision_num_replicas(
        self, autoscaling_context: AutoscalingContext
    ) -> Optional[int]:
        """Interface with the autoscaling policy to get a decision to scale replicas.

        If the autoscaling policy is not ready or returning the same number as the
        current replica number, return None to not execute autoscaling.
        """
        decision_num_replicas = self.autoscaling_policy.get_decision_num_replicas(
            autoscaling_context
        )
        if decision_num_replicas == autoscaling_context.curr_target_num_replicas:
            return None

        return decision_num_replicas
