import logging
import math
import time  # Added import
from typing import Any, Dict, Optional, TYPE_CHECKING

from ray.serve._private.constants import CONTROL_LOOP_INTERVAL_S, SERVE_LOGGER_NAME
from ray.serve._private.common import DeploymentID  # Added import

# Forward declaration for type hinting
if TYPE_CHECKING:
    from ray.serve.config import AutoscalingConfig
    from ray.serve._private.autoscaling_state import AutoscalingStateManager

from ray.util.annotations import PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)


# Make AutoscalingConfig usable as a type hint without quotes
AutoscalingConfig = "AutoscalingConfig"
AutoscalingStateManager = "AutoscalingStateManager"


def _calculate_desired_num_replicas(
    autoscaling_config: AutoscalingConfig,
    total_num_requests: int,
    num_running_replicas: int,
    override_min_replicas: Optional[float] = None,
    override_max_replicas: Optional[float] = None,
    average_qps: Optional[float] = None,
    target_qps_per_replica: Optional[float] = None,
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
    error_ratio: float = 1.0

    if (
        average_qps is not None
        and target_qps_per_replica is not None
        and target_qps_per_replica > 0
    ):
        # Prioritize QPS-based scaling if data is available
        if num_running_replicas > 0:
            # If average_qps is 0, this implies error_ratio is 0, leading to scale down.
            # If target_qps_per_replica * num_running_replicas is 0 (should not happen if target_qps_per_replica > 0),
            # this would be division by zero. However, target_qps_per_replica > 0 is checked.
            # num_running_replicas > 0 is also checked.
            current_total_target_qps = target_qps_per_replica * num_running_replicas
            if current_total_target_qps > 0:
                error_ratio = average_qps / current_total_target_qps
            elif average_qps > 0:  # current target is 0, but we have QPS
                error_ratio = 2.0  # Indicate upscale is needed
            else:  # current target is 0 and no QPS
                error_ratio = 1.0  # No change
        else:  # num_running_replicas == 0
            if average_qps > 0:
                # If there's QPS, we desire to scale up from zero.
                # Calculate desired replicas directly and then derive an error_ratio
                # that would lead to this.
                # For simplicity, if there's any QPS and we are at 0 replicas,
                # set error_ratio to a value that ensures upscale (e.g. 2.0),
                # actual desired number will be handled by ceiling logic later.
                # The number of replicas will be at least 1 (or min_replicas).
                error_ratio = 2.0  # Default upscale ratio when at 0 replicas with QPS
            else:
                # No QPS and at 0 replicas, so no change desired based on QPS.
                error_ratio = 1.0
    elif num_running_replicas > 0:
        # Fallback to ongoing requests based scaling
        # Example: if error_ratio == 2.0, we have two times too many ongoing
        # requests per replica, so we desire twice as many replicas.
        target_num_requests_per_replica = (
            autoscaling_config.get_target_ongoing_requests()
        )
        if target_num_requests_per_replica > 0:
            target_total_requests = (
                target_num_requests_per_replica * num_running_replicas
            )
            if target_total_requests > 0:
                error_ratio = total_num_requests / target_total_requests
            elif total_num_requests > 0:  # target is 0, but we have requests
                error_ratio = 2.0  # Indicate upscale
            else:  # target is 0 and no requests
                error_ratio = 1.0  # No change
        elif (
            total_num_requests > 0
        ):  # target_ongoing_requests is 0, but we have requests
            error_ratio = 2.0  # Indicate upscale
        else:  # target_ongoing_requests is 0 and no requests
            error_ratio = 1.0  # No change
    elif total_num_requests > 0:  # num_running_replicas is 0, but there are requests
        error_ratio = (
            2.0  # Default upscale ratio when at 0 replicas with pending requests
        )
    else:  # num_running_replicas is 0 and no requests
        error_ratio = 1.0

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

    if num_running_replicas > 0:
        desired_num_replicas = math.ceil(num_running_replicas * smoothed_error_ratio)
        # If desired num replicas is "stuck" because of the smoothing factor
        # (meaning the traffic is low enough for the replicas to downscale
        # without the smoothing factor), decrease desired_num_replicas by 1.
        if (
            math.ceil(num_running_replicas * error_ratio) < num_running_replicas
            and desired_num_replicas == num_running_replicas
            and num_running_replicas
            > 0  # Ensure not to go below 0 if num_running_replicas was 1
        ):
            desired_num_replicas -= 1
    elif error_ratio > 1.0:  # num_running_replicas is 0 and we need to scale up
        # If average_qps and target_qps_per_replica are available, calculate desired replicas based on QPS.
        if (
            average_qps is not None
            and target_qps_per_replica is not None
            and target_qps_per_replica > 0
            and average_qps > 0
        ):
            desired_num_replicas = math.ceil(average_qps / target_qps_per_replica)
        # Else if total_num_requests > 0 (and using ongoing request policy), scale up by at least 1.
        # The error_ratio was set to 2.0, so smoothed_error_ratio will be > 1.
        # A base of 1 replica is a reasonable starting point when scaling from zero.
        # Let's use a default of 1 replica, scaled by the smoothed_error_ratio.
        # Example: if error_ratio = 2.0, smoothed_error_ratio = 1 + (1 * upscaling_factor)
        # desired_num_replicas = ceil(1 * smoothed_error_ratio)
        elif total_num_requests > 0:  # Fallback to requests based scaling from 0
            desired_num_replicas = math.ceil(1 * smoothed_error_ratio)
        else:  # No QPS, no requests, num_running_replicas is 0
            desired_num_replicas = 0  # Should be at least min_replicas later
    else:  # num_running_replicas is 0 and error_ratio is 1.0 (no load)
        desired_num_replicas = 0  # Should be at least min_replicas later

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
    # decision_num_replicas will be updated based on the policy logic.
    # Initialize it to current target in case no decision is made.
    decision_num_replicas = curr_target_num_replicas

    # Placeholder for fetching QPS metrics.
    # In a real scenario, this would come from the AutoscalingStateManager or a similar source.
    # For replica_queue_length_autoscaling_policy, average_qps and target_qps_per_replica
    # are not directly used here but passed to _calculate_desired_num_replicas, which might use them.
    # The primary mechanism for this policy remains total_num_requests (queued + ongoing).
    qps_for_calc: Optional[float] = None
    target_qps_for_calc: Optional[float] = None
    if config and hasattr(
        config, "target_qps_per_replica"
    ):  # Check if config supports QPS
        # This policy *could* use QPS if _calculate_desired_num_replicas is modified to fetch it
        # or if it's passed somehow. For now, we assume it primarily uses total_num_requests.
        # If QPS were to be fetched here, it would be similar to qps_based_autoscaling_policy.
        # qps_for_calc = ... fetch from autoscaling_state_manager ...
        target_qps_for_calc = config.target_qps_per_replica

    # The special handling for num_running_replicas == 0 is now mostly inside
    # _calculate_desired_num_replicas.
    # If num_running_replicas is 0 and there's no load (total_num_requests = 0, average_qps = 0),
    # desired_num_replicas will be 0 (before applying min_replicas).
    # If there is load, it should calculate a positive number of desired_replicas.

    desired_num_replicas = _calculate_desired_num_replicas(
        autoscaling_config=config,
        total_num_requests=total_num_requests,  # Primary metric for this policy
        num_running_replicas=num_running_replicas,
        override_min_replicas=capacity_adjusted_min_replicas,
        override_max_replicas=capacity_adjusted_max_replicas,
        average_qps=qps_for_calc,  # Pass potential QPS data
        target_qps_per_replica=target_qps_for_calc,  # Pass potential QPS target
    )

    # Apply min_replicas bound early if scaling up from 0 and no specific desired number was determined by load
    if (
        num_running_replicas == 0
        and desired_num_replicas == 0
        and capacity_adjusted_min_replicas > 0
    ):
        # If policy decided 0 (e.g. no load), but min_replicas > 0,
        # we should at least start min_replicas.
        # However, _calculate_desired_num_replicas already applies bounds.
        # This explicit check might be redundant if bounds are correctly applied there.
        # The bounds are applied at the end of _calculate_desired_num_replicas.
        pass

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


ALL_AUTOSCALING_POLICIES = {
    "replica_queue_length_autoscaling_policy": replica_queue_length_autoscaling_policy,
    "qps_based_autoscaling_policy": qps_based_autoscaling_policy,
}


@PublicAPI(stability="alpha")
def qps_based_autoscaling_policy(
    curr_target_num_replicas: int,
    deployment_id: DeploymentID,
    num_running_replicas: int,  # Added num_running_replicas
    autoscaling_state_manager: AutoscalingStateManager,
    config: Optional[AutoscalingConfig],
    capacity_adjusted_min_replicas: int,
    capacity_adjusted_max_replicas: int,
    policy_state: Dict[str, Any],
) -> int:
    """
    Autoscaling policy based on queries per second (QPS).
    """
    decision_counter = policy_state.get("decision_counter", 0)
    decision_num_replicas = curr_target_num_replicas

    if config is None:
        logger.warning(
            f"Autoscaling config not provided for deployment {deployment_id}. "
            "Cannot apply QPS-based autoscaling policy."
        )
        policy_state["decision_counter"] = 0
        return curr_target_num_replicas

    target_qps_per_replica = getattr(config, "target_qps_per_replica", None)
    qps_calculation_window_s = getattr(
        config, "qps_autoscaling_window_s", 10.0
    )  # Default to 10s

    if target_qps_per_replica is None or target_qps_per_replica <= 0:
        logger.warning(
            f"Target QPS per replica not configured or invalid for deployment {deployment_id} "
            f"({target_qps_per_replica=}). QPS-based autoscaling disabled."
        )
        policy_state["decision_counter"] = 0
        return curr_target_num_replicas

    window_start_timestamp_s = time.time() - qps_calculation_window_s
    average_qps = autoscaling_state_manager.get_average_qps(
        deployment_id, window_start_timestamp_s
    )

    if average_qps is None:
        logger.warning(
            f"No QPS data available for deployment {deployment_id} in the last "
            f"{qps_calculation_window_s}s. QPS-based autoscaling cannot make a decision."
        )
        # Potentially reset counter or maintain previous trend if desired,
        # for now, reset to avoid sticky decisions on transient data loss.
        policy_state["decision_counter"] = 0
        return curr_target_num_replicas

    # Handle initial scale-up from zero replicas
    if num_running_replicas == 0:
        if average_qps > 0:
            # Directly calculate initial desired replicas based on current QPS
            initial_desired_replicas = math.ceil(average_qps / target_qps_per_replica)

            # Apply an upscaling factor, ensuring at least 1 replica if there's any QPS.
            # config.get_upscaling_factor() is a smoothing factor, might not be ideal for initial scale.
            # Consider a specific initial_upscale_factor if needed. For now, use existing.
            upscaled_initial_replicas = max(
                1, math.ceil(initial_desired_replicas * config.get_upscaling_factor())
            )

            # Ensure it's within bounds and not less than current target (which is likely 0 or initial_replicas)
            final_initial_replicas = max(
                upscaled_initial_replicas,
                curr_target_num_replicas,
                capacity_adjusted_min_replicas,
            )
            final_initial_replicas = min(
                final_initial_replicas, capacity_adjusted_max_replicas
            )

            logger.info(
                f"QPS policy: Scaling up {deployment_id} from 0 replicas to {final_initial_replicas} "
                f"based on {average_qps=}, {target_qps_per_replica=}."
            )
            # For initial scale-up, we directly apply the decision without delay counter.
            policy_state["decision_counter"] = 0
            return final_initial_replicas
        else:
            # No QPS and no running replicas, maintain current target (likely 0 or min_replicas)
            policy_state["decision_counter"] = 0
            return curr_target_num_replicas

    # If we have running replicas, use _calculate_desired_num_replicas
    # total_num_requests effectively becomes average_qps for QPS scaling
    desired_num_replicas = _calculate_desired_num_replicas(
        autoscaling_config=config,
        total_num_requests=average_qps,  # Using average_qps as the load metric
        num_running_replicas=num_running_replicas,
        override_min_replicas=capacity_adjusted_min_replicas,
        override_max_replicas=capacity_adjusted_max_replicas,
        average_qps=average_qps,  # Pass to ensure QPS logic is prioritized
        target_qps_per_replica=target_qps_per_replica,  # Pass to ensure QPS logic is prioritized
    )

    # Apply upscale/downscale delays using decision_counter
    if desired_num_replicas > curr_target_num_replicas:
        if decision_counter < 0:  # Was scaling down
            decision_counter = 0
        decision_counter += 1
        if decision_counter >= int(config.upscale_delay_s / CONTROL_LOOP_INTERVAL_S):
            decision_num_replicas = desired_num_replicas
            decision_counter = 0  # Reset after decision
    elif desired_num_replicas < curr_target_num_replicas:
        if decision_counter > 0:  # Was scaling up
            decision_counter = 0
        decision_counter -= 1
        if decision_counter <= -int(config.downscale_delay_s / CONTROL_LOOP_INTERVAL_S):
            decision_num_replicas = desired_num_replicas
            decision_counter = 0  # Reset after decision
    else:  # desired_num_replicas == curr_target_num_replicas
        decision_counter = 0

    policy_state["decision_counter"] = decision_counter
    return decision_num_replicas
