import logging

from ray.data._internal.cluster_autoscaler.base_cluster_autoscaler import (
    ClusterAutoscaler,
)
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources

logger = logging.getLogger(__name__)


class PlacementGroupClusterAutoscaler(ClusterAutoscaler):
    """Autoscaler for datasets using PlacementGroupSchedulingStrategy.

    A placement group defines a fixed resource envelope. Cluster autoscaling
    is disabled because adding nodes cannot expand the PG.
    """

    def __init__(self, pg_resources: ExecutionResources):
        self._pg_resources = pg_resources

    def try_trigger_scaling(self):
        pass

    def on_executor_shutdown(self):
        pass

    def get_total_resources(self) -> ExecutionResources:
        return self._pg_resources
