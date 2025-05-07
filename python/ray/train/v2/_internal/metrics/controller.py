from typing import Dict

from ray.train.v2._internal.metrics.base import RUN_NAME_TAG_KEY, Metric


class ControllerMetrics:
    """Factory for creating controller-specific metrics.

    This class defines all metrics used to track the state and performance of the
    training controller. Each metric is defined with its name, type, default value,
    description, and required tags.
    """

    # ===== Tag Keys =====
    # Base tags that apply to all controller metrics
    TAG_KEYS = [RUN_NAME_TAG_KEY]

    # Additional tags for specific metrics
    CONTROLLER_STATE_TAG_KEY = "ray_train_controller_state"

    # ===== Metric Names =====
    # Time metrics (in seconds)
    WORKER_GROUP_START_TOTAL_TIME_S = "train_worker_group_start_total_time_s"
    WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S = "train_worker_group_shutdown_total_time_s"

    # State metrics
    CONTROLLER_STATE = "train_controller_state"

    @classmethod
    def _create_time_metric(cls, name: str, description: str) -> Metric:
        """Create a time-based metric."""
        return Metric(
            name=name,
            type=float,
            default=0.0,
            description=description,
            tag_keys=cls.TAG_KEYS,
        )

    @classmethod
    def _create_controller_state_metric(cls) -> Metric:
        """Create the controller state metric."""
        return Metric(
            name=cls.CONTROLLER_STATE,
            type=int,
            default=0,
            description="Number of controllers in each state",
            tag_keys=cls.TAG_KEYS + [cls.CONTROLLER_STATE_TAG_KEY],
        )

    @classmethod
    def get_controller_metrics(cls) -> Dict[str, Metric]:
        """Get all controller metrics."""
        return {
            cls.WORKER_GROUP_START_TOTAL_TIME_S: cls._create_time_metric(
                cls.WORKER_GROUP_START_TOTAL_TIME_S,
                "Total time taken to start worker groups",
            ),
            cls.WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S: cls._create_time_metric(
                cls.WORKER_GROUP_SHUTDOWN_TOTAL_TIME_S,
                "Total time taken to shutdown worker groups",
            ),
            cls.CONTROLLER_STATE: cls._create_controller_state_metric(),
        }
