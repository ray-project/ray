import logging
import math
from typing import Any, Dict, Optional

from ray.serve._private.constants import CONTROL_LOOP_INTERVAL_S, SERVE_LOGGER_NAME
from ray.serve.config import AutoscalingConfig
from ray.util.annotations import PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)


def _calculate_desired_num_replicas(
    autoscaling_config: AutoscalingConfig,
    total_num_requests: int,
    num_running_replicas: int,
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
    if num_running_replicas == 0:
        raise ValueError("Number of replicas cannot be zero")

    # Example: if error_ratio == 2.0, we have two times too many ongoing
    # requests per replica, so we desire twice as many replicas.
    target_num_requests = (
        autoscaling_config.get_target_ongoing_requests() * num_running_replicas
    )
    error_ratio: float = total_num_requests / target_num_requests

    # If error ratio >= 1, then the number of ongoing requests per
    # replica exceeds the target and we will make an upscale decision,
    # so we apply the upscale smoothing factor. Otherwise, the number of
    # ongoing requests per replica is lower than the target and we will
    # make a downscale decision, so we apply the downscale smoothing
    # factor.
    if error_ratio >= 1:
        scaling_factor = autoscaling_config.get_upscaling_factor()
    else:
        scaling_factor = autoscaling_config.get_downscaling_factor()

    # Multiply the distance to 1 by the smoothing ("gain") factor (default=1).
    smoothed_error_ratio = 1 + ((error_ratio - 1) * scaling_factor)
    desired_num_replicas = math.ceil(num_running_replicas * smoothed_error_ratio)

    # If desired num replicas is "stuck" because of the smoothing factor
    # (meaning the traffic is low enough for the replicas to downscale
    # without the smoothing factor), decrease desired_num_replicas by 1.
    if (
        math.ceil(num_running_replicas * error_ratio) < num_running_replicas
        and desired_num_replicas == num_running_replicas
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
def replica_queue_length_autoscaling_policy(
    curr_target_num_replicas: int,
    total_num_requests: int,
    num_running_replicas: int,
    config: Optional[AutoscalingConfig],
    capacity_adjusted_min_replicas: int,
    capacity_adjusted_max_replicas: int,
    policy_state: Dict[str, Any],
) -> int:
    """The default autoscaling policy based on basic thresholds for scaling.
    There is a minimum threshold for the average queue length in the cluster
    to scale up and a maximum threshold to scale down. Each period, a 'scale
    up' or 'scale down' decision is made. This decision must be made for a
    specified number of periods in a row before the number of replicas is
    actually scaled. See config options for more details.  Assumes
    `get_decision_num_replicas` is called once every CONTROL_LOOP_PERIOD_S
    seconds.
    """
    decision_counter = policy_state.get("decision_counter", 0)
    if num_running_replicas == 0:
        # When 0 replicas and queries are queued, scale up the replicas
        if total_num_requests > 0:
            return max(
                math.ceil(1 * config.get_upscaling_factor()),
                curr_target_num_replicas,
            )
        return curr_target_num_replicas

    decision_num_replicas = curr_target_num_replicas

    desired_num_replicas = _calculate_desired_num_replicas(
        config,
        total_num_requests,
        num_running_replicas=num_running_replicas,
        override_min_replicas=capacity_adjusted_min_replicas,
        override_max_replicas=capacity_adjusted_max_replicas,
    )
    # Scale up.
    if desired_num_replicas > curr_target_num_replicas:
        # If the previous decision was to scale down (the counter was
        # negative), we reset it and then increment it (set to 1).
        # Otherwise, just increment.
        if decision_counter < 0:
            decision_counter = 0
        decision_counter += 1

        # Only actually scale the replicas if we've made this decision for
        # 'scale_up_consecutive_periods' in a row.
        if decision_counter > int(config.upscale_delay_s / CONTROL_LOOP_INTERVAL_S):
            decision_counter = 0
            decision_num_replicas = desired_num_replicas

    # Scale down.
    elif desired_num_replicas < curr_target_num_replicas:
        # If the previous decision was to scale up (the counter was
        # positive), reset it to zero before decrementing.
        if decision_counter > 0:
            decision_counter = 0
        decision_counter -= 1

        # Only actually scale the replicas if we've made this decision for
        # 'scale_down_consecutive_periods' in a row.
        if decision_counter < -int(config.downscale_delay_s / CONTROL_LOOP_INTERVAL_S):
            decision_counter = 0
            decision_num_replicas = desired_num_replicas

    # Do nothing.
    else:
        decision_counter = 0

    policy_state["decision_counter"] = decision_counter
    return decision_num_replicas


default_autoscaling_policy = replica_queue_length_autoscaling_policy
