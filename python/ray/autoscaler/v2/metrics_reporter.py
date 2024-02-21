from collections import defaultdict
from typing import Dict, List
from ray.autoscaler._private.prom_metrics import AutoscalerPrometheusMetrics
from ray.autoscaler.v2.instance_manager.common import InstanceUtil
from ray.autoscaler.v2.instance_manager.config import NodeTypeConfig
from ray.autoscaler.v2.schema import NodeType

from ray.core.generated.instance_manager_pb2 import Instance as IMInstance


class AutoscalerMetricsReporter:
    def __init__(self, prom_metrics: AutoscalerPrometheusMetrics) -> None:
        self._prom_metrics = prom_metrics

    def report_instances(
        self,
        instances: List[IMInstance],
    ):
        """
        Record autoscaler metrics for:
            - pending_nodes: Nodes that are launching/pending ray start
            - active_nodes: Active nodes (nodes running ray)
            - recently_failed_nodes: Nodes that are being terminated.
            - stopped_nodes: Nodes that are terminated.
        """
        # pending instances.
        pending_by_type = defaultdict(int)
        ray_running_by_type = defaultdict(int)
        terminating_by_type = defaultdict(int)
        terminated_by_type = defaultdict(int)

        for instance in instances:
            if InstanceUtil.is_ray_pending(instance.status):
                pending_by_type[instance.instance_type] += 1
            elif InstanceUtil.is_ray_running(instance.status):
                ray_running_by_type[instance.instance_type] += 1
            elif instance.status == IMInstance.TERMINATING:
                terminating_by_type[instance.instance_type] += 1
            elif instance.status == IMInstance.TERMINATED:
                terminated_by_type[instance.instance_type] += 1

        for instance_type, count in pending_by_type.items():
            self._prom_metrics.pending_nodes.labels(
                SessionName=self._prom_metrics.session_name, NodeType=instance_type
            ).set(count)

        for instance_type, count in ray_running_by_type.items():
            self._prom_metrics.active_nodes.labels(
                SessionName=self._prom_metrics.session_name, NodeType=instance_type
            ).set(count)

        for instance_type, count in terminating_by_type.items():
            self._prom_metrics.recently_failed_nodes.labels(
                SessionName=self._prom_metrics.session_name, NodeType=instance_type
            ).set(count)

        for instance_type, count in terminated_by_type.items():
            # TODO: this could be a gauge in v2.
            self._prom_metrics.stopped_nodes.inc(count)

    def report_resources(
        self,
        instances: List[IMInstance],
        node_type_configs: Dict[NodeType, NodeTypeConfig],
    ):
        """
        Record autoscaler metrics for:
            - pending_resources: Pending resources
            - cluster_resources: Cluster resources (resources running on the cluster)
        """
        # pending resources.
        pending_resources = defaultdict(float)
        cluster_resources = defaultdict(float)

        def _add_resources(resource_map, node_type_configs, node_type, count):
            node_resources = node_type_configs[node_type].resources
            for resource_name, resource_value in node_resources.items():
                resource_map[resource_name] += resource_value * count

        for instance in instances:
            if InstanceUtil.is_ray_pending(instance.status):
                _add_resources(
                    pending_resources, node_type_configs, instance.instance_type, 1
                )
            elif InstanceUtil.is_ray_running(instance.status):
                _add_resources(
                    cluster_resources, node_type_configs, instance.instance_type, 1
                )

        for resource_name, resource_value in pending_resources.items():
            self._prom_metrics.pending_resources.labels(
                SessionName=self._prom_metrics.session_name, resource=resource_name
            ).set(resource_value)

        for resource_name, resource_value in cluster_resources.items():
            self._prom_metrics.cluster_resources.labels(
                SessionName=self._prom_metrics.session_name, resource=resource_name
            ).set(resource_value)
