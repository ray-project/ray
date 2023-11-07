from collections import defaultdict
from typing import Dict, List, Optional
from ray.autoscaler._private.util import NodeStatus

from ray.autoscaler.v2.schema import ResourceRequestByCount, ResourceUsage
from ray.autoscaler.v2.utils import resource_requests_by_count, to_resource_requests
from ray.core.generated.autoscaler_pb2 import (
    AutoscalingState,
    GetClusterResourceStateRequest,
    NodeState,
    RayNodeKind,
    ClusterResourceState,
    ReportAutoscalingStateRequest,
)
import abc
import operator
from abc import abstractmethod

import ray
from ray.autoscaler.v2.schema import ClusterStatus
from ray.autoscaler.v2.sdk import get_cluster_status
from ray.core.generated import autoscaler_pb2
from ray.core.generated.instance_manager_pb2 import Instance


def get_cluster_resource_state(stub) -> ClusterResourceState:
    request = GetClusterResourceStateRequest(last_seen_cluster_resource_state_version=0)
    return stub.GetClusterResourceState(request).cluster_resource_state


class FakeCounter:
    def dec(self, *args, **kwargs):
        pass


def report_autoscaling_state(stub, autoscaling_state: AutoscalingState):
    request = ReportAutoscalingStateRequest(autoscaling_state=autoscaling_state)
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


def make_resource_requests_by_count(
    resource_requests: List[Dict],
) -> List[ResourceRequestByCount]:
    return resource_requests_by_count(to_resource_requests(resource_requests))


def make_cluster_resource_state(
    resource_requests: Optional[List[Dict]] = None,
    node_states: Optional[List[NodeState]] = None,
) -> ClusterResourceState:
    node_states = node_states or []
    resource_requests = resource_requests or []
    return ClusterResourceState(
        cluster_resource_state_version=0,
        cluster_session_name="fake-test-session",
        pending_resource_requests=make_resource_requests_by_count(resource_requests),
        node_states=node_states,
    )


def make_ray_node(
    instance_type: str,
    id: str,
    total_resources: Dict[str, float],
    available_resources: Dict[str, float],
    status: NodeStatus,
    node_kind: RayNodeKind,
    idle_duration_ms: int = 0,
) -> NodeState:
    return NodeState(
        node_id=id.encode(),
        ray_node_type_name=instance_type,
        instance_id=id,
        total_resources=total_resources,
        available_resources=available_resources,
        node_state_version=0,
        status=status,
        idle_duration_ms=idle_duration_ms,
        node_ip_address="99.99.99.99",
        instance_type_name="fake-instance-type",
        ray_node_kind=node_kind,
    )


def make_instance(
    instance_id,
    status=Instance.UNKNOWN,
    version=0,
    ray_status=Instance.RAY_STATUS_UNKNOWN,
    instance_type="worker_nodes1",
):
    return Instance(
        instance_id=instance_id,
        status=status,
        version=version,
        instance_type=instance_type,
        ray_status=ray_status,
        last_modified_timestamp_at_storage_ms=1,
    )


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
