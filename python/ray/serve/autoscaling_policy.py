import os
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Any, List, Optional

import requests

from ray.serve._private.common import TargetCapacityDirection
from ray.serve.config import AutoscalingConfig
from ray.util.annotations import PublicAPI

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
        self.override_min_replicas = 0
        self.target_capacity = None
        self.decision_counter = 0

    def update(
        self,
        curr_target_num_replicas: int,
        current_num_ongoing_requests: List[float],
        current_handle_queued_queries: float,
        override_min_replicas: int,
        target_capacity: Optional[float] = None,
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
            override_min_replicas: The minimum number of replicas.
            target_capacity: The target capacity of the deployment.
        """
        self.curr_target_num_replicas = curr_target_num_replicas
        self.current_num_ongoing_requests = current_num_ongoing_requests
        self.current_handle_queued_queries = current_handle_queued_queries
        self.override_min_replicas = override_min_replicas
        self.target_capacity = target_capacity
