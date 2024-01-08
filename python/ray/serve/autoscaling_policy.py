import logging
import math
from typing import List, Optional

from ray.serve._private.constants import CONTROL_LOOP_PERIOD_S, SERVE_LOGGER_NAME
from ray.serve.config import AutoscalingConfig
from ray.util.annotations import PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)


def _calculate_desired_num_replicas(
    autoscaling_config: AutoscalingConfig,
    current_num_ongoing_requests: List[float],
    override_min_replicas: Optional[float] = None,
    override_max_replicas: Optional[float] = None,
) -> int:
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


@PublicAPI(stability="alpha")
class AutoscalingContext:
    """Contains the context for an autoscaling policy.

    This context includes the current number of replicas, the current number
    of ongoing requests, and the current number of queued queries.
    """

    def __init__(
        self,
        config: AutoscalingConfig,
    ):
        self.config = config
        self.curr_target_num_replicas = 0
        self.current_num_ongoing_requests = []
        self.current_handle_queued_queries = 0.0
        self.capacity_adjusted_min_replicas = None
        self.capacity_adjusted_max_replicas = None
        self.decision_counter = 0
        self.last_scale_time = None
        self.app_name = None
        self.deployment_name = None

    def update(
        self,
        curr_target_num_replicas: int,
        current_num_ongoing_requests: List[float],
        current_handle_queued_queries: float,
        capacity_adjusted_min_replicas: int,
        capacity_adjusted_max_replicas: int,
        app_name: Optional[str] = None,
        deployment_name: Optional[str] = None,
    ):
        """
        Arguments:
            curr_target_num_replicas: The number of replicas that the
                deployment is currently trying to scale to.
            current_num_ongoing_requests: List of number of
                ongoing requests for each replica.
            current_handle_queued_queries: The number of handle queued queries,
                if there are multiple handles, the max number of queries at
                a single handle should be passed in
            capacity_adjusted_min_replicas: The `min_replica` of the deployment adjusted
                by the target capacity.
            capacity_adjusted_max_replicas: The `max_replica` of the deployment adjusted
                by the target capacity.
            app_name: The name of the application.
            deployment_name: The name of the deployment.
        """
        self.curr_target_num_replicas = curr_target_num_replicas
        self.current_num_ongoing_requests = current_num_ongoing_requests
        self.current_handle_queued_queries = current_handle_queued_queries
        self.capacity_adjusted_min_replicas = capacity_adjusted_min_replicas
        self.capacity_adjusted_max_replicas = capacity_adjusted_max_replicas
        self.app_name = app_name
        self.deployment_name = deployment_name


@PublicAPI(stability="alpha")
def replica_queue_length_autoscaling_policy(context: AutoscalingContext) -> int:
    """The default autoscaling policy based on basic thresholds for scaling.
    There is a minimum threshold for the average queue length in the cluster
    to scale up and a maximum threshold to scale down. Each period, a 'scale
    up' or 'scale down' decision is made. This decision must be made for a
    specified number of periods in a row before the number of replicas is
    actually scaled. See config options for more details.  Assumes
    `get_decision_num_replicas` is called once every CONTROL_LOOP_PERIOD_S
    seconds.
    """

    if len(context.current_num_ongoing_requests) == 0:
        # When 0 replicas and queries are queued, scale up the replicas
        if context.current_handle_queued_queries > 0:
            return max(
                math.ceil(1 * context.config.get_upscale_smoothing_factor()),
                context.curr_target_num_replicas,
            )
        return context.curr_target_num_replicas

    decision_num_replicas = context.curr_target_num_replicas

    desired_num_replicas = _calculate_desired_num_replicas(
        context.config,
        context.current_num_ongoing_requests,
        override_min_replicas=context.capacity_adjusted_min_replicas,
        override_max_replicas=context.capacity_adjusted_max_replicas,
    )
    # Scale up.
    if desired_num_replicas > context.curr_target_num_replicas:
        # If the previous decision was to scale down (the counter was
        # negative), we reset it and then increment it (set to 1).
        # Otherwise, just increment.
        if context.decision_counter < 0:
            context.decision_counter = 0
        context.decision_counter += 1

        # Only actually scale the replicas if we've made this decision for
        # 'scale_up_consecutive_periods' in a row.
        if context.decision_counter > int(
            context.config.upscale_delay_s / CONTROL_LOOP_PERIOD_S
        ):
            context.decision_counter = 0
            decision_num_replicas = desired_num_replicas

    # Scale down.
    elif desired_num_replicas < context.curr_target_num_replicas:
        # If the previous decision was to scale up (the counter was
        # positive), reset it to zero before decrementing.
        if context.decision_counter > 0:
            context.decision_counter = 0
        context.decision_counter -= 1

        # Only actually scale the replicas if we've made this decision for
        # 'scale_down_consecutive_periods' in a row.
        if context.decision_counter < -int(
            context.config.downscale_delay_s / CONTROL_LOOP_PERIOD_S
        ):
            context.decision_counter = 0
            decision_num_replicas = desired_num_replicas

    # Do nothing.
    else:
        context.decision_counter = 0

    return decision_num_replicas


default_autoscaling_policy = replica_queue_length_autoscaling_policy
