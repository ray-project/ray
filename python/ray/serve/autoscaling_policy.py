from abc import ABCMeta, abstractmethod
import math

from ray.serve.config import AutoscalingConfig
from typing import List


def calculate_desired_num_replicas(autoscaling_config: AutoscalingConfig,
                                   current_num_ongoing_requests: List[float]
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
    num_ongoing_requests_per_replica: float = sum(
        current_num_ongoing_requests) / len(current_num_ongoing_requests)

    # Example: if error_ratio == 2.0, we have two times too many ongoing
    # requests per replica, so we desire twice as many replicas.
    error_ratio: float = (
        num_ongoing_requests_per_replica /
        autoscaling_config.target_num_ongoing_requests_per_replica)

    # Multiply the distance to 1 by the smoothing ("gain") factor (default=1).
    smoothed_error_ratio = 1 + (
        (error_ratio - 1) * autoscaling_config.smoothing_factor)
    desired_num_replicas = math.ceil(
        current_num_replicas * smoothed_error_ratio)

    # Ensure min_replicas <= desired_num_replicas <= max_replicas.
    desired_num_replicas = min(autoscaling_config.max_replicas,
                               desired_num_replicas)
    desired_num_replicas = max(autoscaling_config.min_replicas,
                               desired_num_replicas)

    return desired_num_replicas


class AutoscalingPolicy:
    """Defines the interface for an autoscaling policy.

    To add a new autoscaling policy, a class should be defined that provides
    this interface. The class may be stateful, in which case it may also want
    to provide a non-default constructor. However, this state will be lost when
    the controller recovers from a failure.
    """
    __metaclass__ = ABCMeta

    def __init__(self, config):
        """Initialize the policy using the specified config dictionary."""
        self.config = config

    @abstractmethod
    def scale(self, router_queue_lens, curr_replicas):
        """Make a decision to scale backends.

        Arguments:
            router_queue_lens (Dict[str, int]): map of routers to their most
                recent queue length of unsent queries for this backend.
            curr_replicas (int): The number of replicas that the backend
                currently has.

        Returns:
            int The new number of replicas to scale this backend to.
        """
        return curr_replicas
