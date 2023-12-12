import math
import os
from typing import List

from ray.serve._private.constants import CONTROL_LOOP_PERIOD_S
from ray.serve._private.utils import calculate_desired_num_replicas
from ray.serve.config import AutoscalingConfig
from ray.util.annotations import DeveloperAPI, PublicAPI

PROMETHEUS_HOST = os.environ.get("RAY_PROMETHEUS_HOST", "http://localhost:9090")


@PublicAPI(stability="beta")
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

    @DeveloperAPI
    def update(
        self,
        curr_target_num_replicas: int,
        current_num_ongoing_requests: List[float],
        current_handle_queued_queries: float,
        capacity_adjusted_min_replicas: int,
        capacity_adjusted_max_replicas: int,
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
            capacity_adjusted_min_replicas: The minimum number of replicas.
            capacity_adjusted_max_replicas: The maximum number of replicas.
        """
        self.curr_target_num_replicas = curr_target_num_replicas
        self.current_num_ongoing_requests = current_num_ongoing_requests
        self.current_handle_queued_queries = current_handle_queued_queries
        self.capacity_adjusted_min_replicas = capacity_adjusted_min_replicas
        self.capacity_adjusted_max_replicas = capacity_adjusted_max_replicas


@PublicAPI(stability="stable")
def basic_autoscaling_policy(context: AutoscalingContext) -> int:
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

    desired_num_replicas = calculate_desired_num_replicas(
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
