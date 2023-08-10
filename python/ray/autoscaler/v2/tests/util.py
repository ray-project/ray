from typing import Dict, List

from ray.autoscaler.v2.schema import ResourceUsage
from ray.core.generated import autoscaler_pb2
from ray.core.generated.instance_manager_pb2 import Instance


def get_cluster_resource_state(stub) -> autoscaler_pb2.ClusterResourceState:
    request = autoscaler_pb2.GetClusterResourceStateRequest(
        last_seen_cluster_resource_state_version=0
    )
    return stub.GetClusterResourceState(request).cluster_resource_state


class FakeCounter:
    def dec(self, *args, **kwargs):
        pass


def create_instance(
    instance_id,
    status=Instance.UNKNOWN,
    version=0,
    ray_status=Instance.RAY_STATUS_UNKOWN,
    instance_type="worker_nodes1",
):
    return Instance(
        instance_id=instance_id,
        status=status,
        version=version,
        instance_type=instance_type,
        ray_status=ray_status,
        timestamp_since_last_modified=1,
    )


def report_autoscaling_state(stub, autoscaling_state: autoscaler_pb2.AutoscalingState):
    request = autoscaler_pb2.ReportAutoscalingStateRequest(
        autoscaling_state=autoscaling_state
    )
    stub.ReportAutoscalingState(request)


def get_total_resources(usages: List[ResourceUsage]) -> Dict[str, float]:
    """Returns a map of resource name to total resource."""
    return {r.resource_name: r.total for r in usages}


def get_available_resources(usages: List[ResourceUsage]) -> Dict[str, float]:
    """Returns a map of resource name to available resource."""
    return {r.resource_name: r.total - r.used for r in usages}


def get_used_resources(usages: List[ResourceUsage]) -> Dict[str, float]:
    """Returns a map of resource name to used resource."""
    return {r.resource_name: r.used for r in usages}
