from abc import ABCMeta, abstractmethod
import math

from ray.serve.config import AutoscalingConfig
from ray.serve._private.constants import CONTROL_LOOP_PERIOD_S

from typing import List


def calculate_desired_num_replicas(
    autoscaling_config: AutoscalingConfig, current_num_ongoing_requests: List[float]
) -> int:  # (desired replicas):
    """Returns the number of replicas to scale to based on the given metrics.

    Args:
        autoscaling_config: The autoscaling parameters to use for this
            calculation.
        current_num_ongoing_requests (List[float]): A list of the number of
            ongoing requests for each replica.  Assumes each entry has already
            been time-averaged over the desired lookback window.

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

    # Multiply the distance to 1 by the smoothing ("gain") factor (default=1).
    smoothed_error_ratio = 1 + ((error_ratio - 1) * autoscaling_config.smoothing_factor)
    desired_num_replicas = math.ceil(current_num_replicas * smoothed_error_ratio)

    # Ensure min_replicas <= desired_num_replicas <= max_replicas.
    desired_num_replicas = min(autoscaling_config.max_replicas, desired_num_replicas)
    desired_num_replicas = max(autoscaling_config.min_replicas, desired_num_replicas)

    return desired_num_replicas


class AutoscalingPolicy:
    """Defines the interface for an autoscaling policy.

    To add a new autoscaling policy, a class should be defined that provides
    this interface. The class may be stateful, in which case it may also want
    to provide a non-default constructor. However, this state will be lost when
    the controller recovers from a failure.
    """

    __metaclass__ = ABCMeta

    def __init__(self, config: AutoscalingConfig):
        """Initialize the policy using the specified config dictionary."""
        self.config = config

    @abstractmethod
    def get_decision_num_replicas(
        self,
        curr_target_num_replicas: int,
        current_num_ongoing_requests: List[float],
        current_handle_queued_queries: float,
    ) -> int:
        """Make a decision to scale replicas.

        Arguments:
            current_num_ongoing_requests: List[float]: List of number of
                ongoing requests for each replica.
            curr_target_num_replicas: The number of replicas that the
                deployment is currently trying to scale to.
            current_handle_queued_queries : The number of handle queued queries,
                if there are multiple handles, the max number of queries at
                a single handle should be passed in

        Returns:
            int: The new number of replicas to scale to.
        """
        return curr_target_num_replicas


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

    def get_decision_num_replicas(
        self,
        curr_target_num_replicas: int,
        current_num_ongoing_requests: List[float],
        current_handle_queued_queries: float,
    ) -> int:

        if len(current_num_ongoing_requests) == 0:
            # When 0 replica and queries queued, scale up the replicas
            if current_handle_queued_queries > 0:
                return max(1, curr_target_num_replicas)
            return curr_target_num_replicas

        decision_num_replicas = curr_target_num_replicas

        desired_num_replicas = calculate_desired_num_replicas(
            self.config, current_num_ongoing_requests
        )
        # Scale up.
        if desired_num_replicas > curr_target_num_replicas:
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
        elif desired_num_replicas < curr_target_num_replicas:
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
