import abc
import operator
import time
from abc import abstractmethod
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import ray
from ray.autoscaler.v2.schema import AutoscalerInstance, ClusterStatus, ResourceUsage
from ray.autoscaler.v2.sdk import get_cluster_status
from ray.core.generated import autoscaler_pb2
from ray.core.generated.instance_manager_pb2 import Instance, NodeKind


class MockEventLogger:
    def __init__(self, logger) -> None:
        self._logs = defaultdict(list)
        self._logger = logger

    def info(self, s):
        self._logger.info(s)
        self._logs["info"].append(s)

    def warning(self, s):
        self._logger.warning(s)
        self._logs["warning"].append(s)

    def error(self, s):
        self._logger.error(s)
        self._logs["error"].append(s)

    def debug(self, s):
        self._logger.debug(s)
        self._logs["debug"].append(s)

    def get_logs(self, level: str) -> List[str]:
        return self._logs[level]


class MockSubscriber:
    def __init__(self):
        self.events = []

    def notify(self, events):
        self.events.extend(events)

    def clear(self):
        self.events.clear()

    def events_by_id(self, instance_id):
        return [e for e in self.events if e.instance_id == instance_id]


def make_autoscaler_instance(
    im_instance: Optional[Instance] = None,
    ray_node: Optional[autoscaler_pb2.NodeState] = None,
    cloud_instance_id: Optional[str] = None,
) -> AutoscalerInstance:

    if cloud_instance_id:
        if im_instance:
            im_instance.cloud_instance_id = cloud_instance_id
        if ray_node:
            ray_node.instance_id = cloud_instance_id

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
    instance_type="worker_nodes1",
    status_times: List[Tuple["Instance.InstanceStatus", int]] = None,
    launch_request_id="",
    version=0,
    cloud_instance_id="",
    ray_node_id="",
    node_kind=NodeKind.WORKER,
):

    if not status_times:
        status_times = [(status, time.time_ns())]

    return Instance(
        instance_id=instance_id,
        status=status,
        version=version,
        instance_type=instance_type,
        launch_request_id=launch_request_id,
        status_history=[
            Instance.StatusHistory(instance_status=status, timestamp_ns=ts)
            for status, ts in status_times
        ],
        cloud_instance_id=cloud_instance_id,
        node_id=ray_node_id,
        node_kind=node_kind,
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
