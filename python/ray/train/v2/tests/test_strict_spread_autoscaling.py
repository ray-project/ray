"""STRICT_SPREAD must start on an autoscaling cluster (#66766).

Chains the real components end-to-end, without a cluster:

    Train policy -> build_train_resource_request
      -> _AutoscalingCoordinatorActor._merge_and_send_requests
      -> (bundles forwarded to request_resources)
      -> autoscaler v2 ResourceDemandScheduler (cluster resource constraint)
      -> launched nodes become ray.nodes()
      -> coordinator reservation
      -> Train scaling decision

The only fakes are the two coordinator hooks (`send_resources_request` and
`get_cluster_nodes`), which stand in for GCS. GCS stores the
`ClusterResourceConstraint` proto verbatim (`gcs_autoscaler_state_manager.cc`),
so feeding the forwarded bundles straight into the scheduler is faithful.
"""

import sys
from typing import Dict, List
from unittest.mock import MagicMock

import pytest

from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.scheduler import (
    NodeTypeConfig,
    ResourceDemandScheduler,
    SchedulingRequest,
)
from ray.autoscaler.v2.tests.util import MockEventLogger, make_autoscaler_instance
from ray.autoscaler.v2.utils import ResourceRequestUtil
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceConstraint,
    NodeState,
    NodeStatus,
)
from ray.core.generated.instance_manager_pb2 import Instance
from ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator import (
    _AutoscalingCoordinatorActor,
)
from ray.train.v2._internal.execution.scaling_policy import (
    ResizeDecision,
)
from ray.train.v2._internal.execution.scaling_policy.elastic import (
    ElasticScalingPolicy,
)
from ray.train.v2._internal.execution.scaling_policy.fixed import (
    FixedScalingPolicy,
)
from ray.train.v2.api.config import ScalingConfig

REQUESTER_ID = "train-run"
HEAD = {"CPU": 4}
GPU_8X = {"CPU": 64, "GPU": 8}
GPU_1X = {"CPU": 8, "GPU": 1}


class FakeCluster:
    """Stands in for GCS: the autoscaler side and ray.nodes() side."""

    def __init__(self, node_types: Dict[str, Dict[str, float]]):
        self.node_type_configs = {
            name: NodeTypeConfig(
                name=name, resources=res, min_worker_nodes=0, max_worker_nodes=16
            )
            for name, res in node_types.items()
        }
        # (node_type, node_id) of running worker nodes.
        self.running: List[tuple] = []
        # Latest cluster resource constraint, as sent by request_resources.
        self.constraint_bundles: List[Dict[str, float]] = []

    # -- coordinator hooks --------------------------------------------------

    def send_resources_request(self, bundles, label_selectors=None):
        self.constraint_bundles = list(bundles)

    def get_cluster_nodes(self):
        nodes = [{"NodeID": "head", "Alive": True, "Resources": dict(HEAD)}]
        for node_type, node_id in self.running:
            nodes.append(
                {
                    "NodeID": node_id,
                    "Alive": True,
                    "Resources": dict(self.node_type_configs[node_type].resources),
                }
            )
        return nodes

    # -- autoscaler ---------------------------------------------------------

    def autoscale(self) -> Dict[str, int]:
        """Run one real autoscaler v2 scheduling round; launch what it asks."""
        requests = [ResourceRequestUtil.make(b) for b in self.constraint_bundles]
        instances = [
            make_autoscaler_instance(
                im_instance=Instance(
                    instance_type=node_type,
                    status=Instance.RAY_RUNNING,
                    instance_id=node_id,
                    node_id=node_id,
                ),
                ray_node=NodeState(
                    node_id=node_id.encode(),
                    ray_node_type_name=node_type,
                    available_resources=self.node_type_configs[node_type].resources,
                    total_resources=self.node_type_configs[node_type].resources,
                    idle_duration_ms=0,
                    status=NodeStatus.RUNNING,
                ),
                cloud_instance_id=f"c-{node_id}",
            )
            for node_type, node_id in self.running
        ]
        request = SchedulingRequest(
            node_type_configs=self.node_type_configs,
            cluster_resource_constraints=[
                ClusterResourceConstraint(
                    resource_requests=ResourceRequestUtil.group_by_count(requests)
                )
            ]
            if requests
            else [],
            current_instances=instances,
            disable_launch_config_check=True,
        )
        event_logger = AutoscalerEventLogger(MockEventLogger(MagicMock()))
        reply = ResourceDemandScheduler(event_logger).schedule(request)
        launched = {r.instance_type: r.count for r in reply.to_launch}
        for node_type, count in launched.items():
            for _ in range(count):
                self.running.append((node_type, f"{node_type}-{len(self.running)}"))
        return launched


def _make_policy(policy_cls, scaling_config, coordinator):
    policy = policy_cls(scaling_config)
    client = MagicMock()

    def send(resources, label_selectors, strategy):
        coordinator.request_resources(
            requester_id=REQUESTER_ID,
            resources=resources,
            label_selectors=label_selectors,
            expire_after_s=1e9,
            strategy=strategy,
        )

    client.send_resource_request.side_effect = send
    client.maybe_send_resource_request.side_effect = send
    client.get_reserved_resources.side_effect = (
        lambda recompute=False: coordinator.get_reserved_resources(
            REQUESTER_ID, recompute=recompute
        )
    )
    policy._coordinator_client = client
    return policy


def _run(policy_cls, scaling_config, cluster, rounds=5):
    """Drive policy <-> coordinator <-> autoscaler for a few rounds."""
    coordinator = _AutoscalingCoordinatorActor(
        get_current_time=lambda: 0.0,
        send_resources_request=cluster.send_resources_request,
        get_cluster_nodes=cluster.get_cluster_nodes,
    )
    policy = _make_policy(policy_cls, scaling_config, coordinator)
    policy._send_resource_request()
    decision = None
    for _ in range(rounds):
        cluster.autoscale()
        coordinator._tick()
        decision = policy.make_decision_for_non_running_worker_group()
        if isinstance(decision, ResizeDecision):
            break
    return decision, coordinator


SPREAD_8 = dict(num_workers=8, use_gpu=True, placement_strategy="STRICT_SPREAD")


@pytest.mark.parametrize(
    "node_types",
    [{"gpu_8x": GPU_8X}, {"gpu_8x": GPU_8X, "gpu_1x": GPU_1X}],
    ids=["only_8x", "8x_and_1x"],
)
def test_fixed_strict_spread_starts(node_types):
    """The autoscaler launches one node per worker and the reservation covers
    all 8 on distinct nodes, so Train can pin the worker group to it.

    Without the spread constraint, the autoscaler satisfies the 8 bundles with
    a single 8-GPU node (even when a 1-GPU node type is available), the
    STRICT_SPREAD reservation covers 1/8, and Train never starts.
    """
    cluster = FakeCluster(node_types)
    decision, coordinator = _run(FixedScalingPolicy, ScalingConfig(**SPREAD_8), cluster)

    assert len(cluster.running) == 8
    reserved = coordinator.get_reserved_resources(REQUESTER_ID)
    assert len(reserved) == 8
    assert sum(r.get("GPU", 0) for r in reserved.values()) == 8
    assert isinstance(decision, ResizeDecision)
    assert decision.num_workers == 8


def test_elastic_strict_spread_starts():
    """The elastic policy sizes the worker group from the reservation, so it
    stays below min_workers unless the reservation spans distinct nodes."""
    cluster = FakeCluster({"gpu_8x": GPU_8X})
    decision, _ = _run(
        ElasticScalingPolicy,
        ScalingConfig(
            num_workers=(2, 8), use_gpu=True, placement_strategy="STRICT_SPREAD"
        ),
        cluster,
    )
    assert len(cluster.running) == 8
    assert isinstance(decision, ResizeDecision)
    assert decision.num_workers == 8


def test_pack_still_packs():
    """PACK is not spread, so the autoscaler still uses one big node."""
    cluster = FakeCluster({"gpu_8x": GPU_8X})
    decision, _ = _run(
        FixedScalingPolicy,
        ScalingConfig(num_workers=8, use_gpu=True, placement_strategy="PACK"),
        cluster,
    )
    assert [t for t, _ in cluster.running] == ["gpu_8x"]
    assert isinstance(decision, ResizeDecision)
    assert decision.num_workers == 8


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
