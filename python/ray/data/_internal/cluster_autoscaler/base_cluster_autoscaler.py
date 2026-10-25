from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.execution_options import (
        ExecutionResources,
    )


@DeveloperAPI
class ClusterAutoscaler(ABC):
    """Abstract interface for Ray Data cluster autoscaler."""

    @abstractmethod
    def try_trigger_scaling(self):
        """Try trigger autoscaling.

        This method will be called each time when StreamingExecutor makes
        a scheduling decision. A subclass should override this method to
        handle the autoscaling of the cluster.
        """
        ...

    @abstractmethod
    def on_executor_shutdown(self):
        """Callback when the StreamingExecutor is shutting down."""
        ...

    @abstractmethod
    def get_total_resources(self) -> "ExecutionResources":
        """Get the total resources that are available to this data execution."""
        ...
