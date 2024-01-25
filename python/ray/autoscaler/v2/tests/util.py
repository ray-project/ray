import abc
import operator
from abc import abstractmethod
from typing import Dict, List, Optional

import ray
from ray.autoscaler.v2.schema import AutoscalerInstance, ClusterStatus, ResourceUsage
from ray.autoscaler.v2.sdk import get_cluster_status
from ray.core.generated import autoscaler_pb2
from ray.core.generated.instance_manager_pb2 import Instance


def make_autoscaler_instance(
    im_instance: Optional[Instance] = None,
    ray_node: Optional[autoscaler_pb2.NodeState] = None,
    cloud_instance_id: Optional[str] = None,
) -> AutoscalerInstance:
    return AutoscalerInstance(
        im_instance=im_instance,
        ray_node=ray_node,
        cloud_instance_id=cloud_instance_id,
    )


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
    instance_type="worker_nodes1",
):
    return Instance(
        instance_id=instance_id,
        status=status,
        version=version,
        instance_type=instance_type,
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


"""
Test utils for e2e autoscaling states checking.
"""


class Check(abc.ABC):
    @abstractmethod
    def check(self, status: ClusterStatus):
        pass

    def __repr__(self) -> str:
        return self.__str__()


class CheckFailure(RuntimeError):
    pass


class NodeCountCheck(Check):
    def __init__(self, count: int):
        self.count = count

    def check(self, status: ClusterStatus):
        healthy_nodes = len(status.active_nodes) + len(status.idle_nodes)
        if healthy_nodes != self.count:
            raise CheckFailure(f"Expected {self.count} nodes, got {healthy_nodes}")

    def __str__(self) -> str:
        return f"NodeCountCheck: {self.count}"


class TotalResourceCheck(Check):
    def __init__(
        self, resources: Dict[str, float], op: operator = operator.eq, enforce_all=False
    ):
        self.resources = resources
        self.op = op
        self.enforce_all = enforce_all

    def check(self, status: ClusterStatus):
        actual = status.total_resources()
        if self.enforce_all and len(actual) != len(self.resources):
            raise CheckFailure(
                f"Expected {len(self.resources)} resources, got {len(actual)}"
            )

        for k, v in self.resources.items():
            if k not in actual and v:
                raise CheckFailure(f"Expected resource {k} not found")

            if not self.op(v, actual.get(k, 0)):
                raise CheckFailure(
                    f"Expected resource {k} {self.op} {v}, got {actual.get(k, 0)}"
                )

    def __str__(self) -> str:
        return f"TotalResourceCheck({self.op}): {self.resources}"


def check_cluster(
    targets: List[Check],
) -> bool:
    gcs_address = ray.get_runtime_context().gcs_address
    cluster_status = get_cluster_status(gcs_address)

    for target in targets:
        target.check(cluster_status)

    return True
