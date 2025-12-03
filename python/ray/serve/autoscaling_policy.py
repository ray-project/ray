import functools
import logging
import math
from typing import Any, Callable, Dict, Optional, Tuple

from ray.serve._private.constants import CONTROL_LOOP_INTERVAL_S, SERVE_LOGGER_NAME
from ray.serve.config import AutoscalingConfig, AutoscalingContext
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


def _apply_scaling_factors(
    desired_num_replicas: int,
    current_num_replicas: int,
    autoscaling_config: AutoscalingConfig,
) -> int:
    """Apply scaling factors to the desired number of replicas.
    Returns the scaled number of replicas depending on the scaling factor.
    The computation uses the difference between desired and current to scale.

    """
    if current_num_replicas == desired_num_replicas:
        return desired_num_replicas
    replicas_delta = desired_num_replicas - current_num_replicas
    scaling_factor = (
        autoscaling_config.get_upscaling_factor()
        if replicas_delta > 0
        else autoscaling_config.get_downscaling_factor()
    )
    scaled_num_replicas = math.ceil(
        current_num_replicas + scaling_factor * replicas_delta
    )
    # If the scaled_replicas are stuck during downscaling because of scaling factor, decrement by 1.
    if (
        desired_num_replicas < current_num_replicas
        and scaled_num_replicas == current_num_replicas
    ):
        scaled_num_replicas -= 1
    return scaled_num_replicas


def _apply_delay_logic(
    desired_num_replicas: int,
    curr_target_num_replicas: int,
    config: AutoscalingConfig,
    policy_state: Dict[str, Any],
) -> Tuple[int, Dict[str, Any]]:

    """Apply delay logic to the desired number of replicas."""
    decision_num_replicas = curr_target_num_replicas
    decision_counter = policy_state.get("decision_counter", 0)
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
        # Downscaling to zero is only allowed from 1 -> 0
        is_scaling_to_zero = curr_target_num_replicas == 1
        # Determine the delay to use
        if is_scaling_to_zero:
            # Check if the downscale_to_zero_delay_s is set
            if config.downscale_to_zero_delay_s is not None:
                delay_s = config.downscale_to_zero_delay_s
            else:
                delay_s = config.downscale_delay_s
        else:
            delay_s = config.downscale_delay_s
            # The desired_num_replicas>0 for downscaling cases other than 1->0
            desired_num_replicas = max(1, desired_num_replicas)
        # Only actually scale the replicas if we've made this decision for
        # 'scale_down_consecutive_periods' in a row.
        if decision_counter < -int(delay_s / CONTROL_LOOP_INTERVAL_S):
            decision_counter = 0
            decision_num_replicas = desired_num_replicas
    # Do nothing.
    else:
        decision_counter = 0

    policy_state["decision_counter"] = decision_counter
    return decision_num_replicas, policy_state


def _apply_bounds(
    num_replicas: int,
    capacity_adjusted_min_replicas: int,
    capacity_adjusted_max_replicas: int,
) -> int:
    """Clip replica count to be within capacity-adjusted min/max bounds."""
    return max(
        capacity_adjusted_min_replicas,
        min(capacity_adjusted_max_replicas, num_replicas),
    )


@PublicAPI(stability="alpha")
def apply_autoscaling_config(
    policy_func: Callable[[AutoscalingContext], Tuple[int, Dict[str, Any]]]
) -> Callable[[AutoscalingContext], Tuple[int, Dict[str, Any]]]:
    """
    Wraps a custom policy function to automatically apply:
    - upscaling_factor / downscaling_factor
    - min_replicas / max_replicas bounds
    - upscale_delay_s / downscale_delay_s / downscale_to_zero_delay_s
    """

    @functools.wraps(policy_func)
    def wrapped_policy(ctx: AutoscalingContext) -> Tuple[int, Dict[str, Any]]:

        policy_state = ctx.policy_state
        # Get raw desired replicas from custom policy.
        # NOTE: Currently custom policies do not get the default policy's 0 replica cold start fast path.
        # upscale_delay_s applies even at 0 replicas.
        """
        Currently the custom policy can return a dictionary with a decision_counter key.
        and can overwrite the decision_counter key in the policy_state dictionary currenty I am just ignoring that field.
        There are a few ways to handle this:
        1. One method is to create a custom_policy_state inside the policy_state dictionary to avoid overwriting decision_counter key if the custom policy also returns
         a dictionary with decision_counter key.
         ```
            if "custom_policy_state" not in policy_state:
                policy_state["custom_policy_state"] = {}
            user_ctx = dataclasses.replace(ctx, policy_state=policy_state["custom_policy_state"])
            desired_num_replicas, updated_custom_policy_state = policy_func(user_ctx)
            # Update custom policy state with the updated state returned by the custom policy
            if updated_custom_policy_state:
                policy_state["custom_policy_state"].update(updated_custom_policy_state)
        Pros: This can be used for future internal states and can separate the custom and internal states for policies that need to use both.
        ```
        2. We can ignore the decision_counter key from the custom policy and warn the user that the decision_counter key is a protected key and cannot be overwritten.
        so we have created a new key called custom_decision_counter inside the policy_state dictionary to store the decision_counter key from the custom policy.
        ```

        desired_num_replicas, updated_custom_policy_state = policy_func(ctx)
        # Update custom policy state with the updated state returned by the custom policy
        if updated_custom_policy_state:
            if "decision_counter" in updated_custom_policy_state:
                logger.warning("The decision_counter key is a protected key and cannot be overwritten. The decision_counter key will be stored under the `custom_decision_counter` key.")
                policy_state["custom_decision_counter"] = updated_custom_policy_state.pop("decision_counter")
            policy_state.update(updated_custom_policy_state)
        Pros: This is a simple solution and can fit into the existing implementation.
        Cons: This can become an issue if the custom policy and internal policy states have a lot of overlaps in the future.
       """
        desired_num_replicas, updated_custom_policy_state = policy_func(ctx)
        if updated_custom_policy_state:
            if "decision_counter" in updated_custom_policy_state:
                logger.warning(
                    "The 'decision_counter' key is reserved for internal use and will be ignored."
                )
                updated_custom_policy_state.pop("decision_counter")
            policy_state.update(updated_custom_policy_state)
        # Apply scaling factors (if configured)
        if (
            ctx.config.upscaling_factor is not None
            or ctx.config.downscaling_factor is not None
        ):
            desired_num_replicas = _apply_scaling_factors(
                desired_num_replicas, ctx.current_num_replicas, ctx.config
            )

        # Apply delay logic
        decision_num_replicas, updated_state = _apply_delay_logic(
            desired_num_replicas, ctx.target_num_replicas, ctx.config, policy_state
        )

        # Apply bounds
        final_num_replicas = _apply_bounds(
            decision_num_replicas,
            ctx.capacity_adjusted_min_replicas,
            ctx.capacity_adjusted_max_replicas,
        )

        return final_num_replicas, updated_state

    return wrapped_policy


@PublicAPI(stability="alpha")
def replica_queue_length_autoscaling_policy(
    ctx: AutoscalingContext,
) -> Tuple[int, Dict[str, Any]]:
    """The default autoscaling policy based on basic thresholds for scaling.
    There is a minimum threshold for the average queue length in the cluster
    to scale up and a maximum threshold to scale down. Each period, a 'scale
    up' or 'scale down' decision is made. This decision must be made for a
    specified number of periods in a row before the number of replicas is
    actually scaled. See config options for more details.  Assumes
    `get_decision_num_replicas` is called once every CONTROL_LOOP_PERIOD_S
    seconds.
    """

    curr_target_num_replicas: int = ctx.target_num_replicas
    total_num_requests: int = ctx.total_num_requests
    num_running_replicas: int = ctx.current_num_replicas
    config: Optional[AutoscalingConfig] = ctx.config
    capacity_adjusted_min_replicas: int = ctx.capacity_adjusted_min_replicas
    capacity_adjusted_max_replicas: int = ctx.capacity_adjusted_max_replicas
    policy_state: Dict[str, Any] = ctx.policy_state
    if num_running_replicas == 0:
        # When 0 replicas and queries are queued, scale up the replicas
        if total_num_requests > 0:
            return (
                max(
                    math.ceil(1 * config.get_upscaling_factor()),
                    curr_target_num_replicas,
                ),
                policy_state,
            )
        return curr_target_num_replicas, policy_state

    desired_num_replicas = _calculate_desired_num_replicas(
        config,
        total_num_requests,
        num_running_replicas=num_running_replicas,
        override_min_replicas=capacity_adjusted_min_replicas,
        override_max_replicas=capacity_adjusted_max_replicas,
    )
    decision_num_replicas, policy_state = _apply_delay_logic(
        desired_num_replicas, curr_target_num_replicas, config, policy_state
    )
    return decision_num_replicas, policy_state


default_autoscaling_policy = replica_queue_length_autoscaling_policy
