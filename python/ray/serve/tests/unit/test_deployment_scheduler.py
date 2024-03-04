import random
import sys
from typing import List
from unittest.mock import Mock

import pytest

import ray
from ray.serve._private.common import DeploymentID
from ray.serve._private.config import ReplicaConfig
from ray.serve._private.deployment_scheduler import (
    DefaultDeploymentScheduler,
    DeploymentDownscaleRequest,
    DeploymentSchedulingInfo,
    ReplicaSchedulingRequest,
    Resources,
    SpreadDeploymentSchedulingPolicy,
)
from ray.serve._private.test_utils import MockClusterNodeInfoCache
from ray.tests.conftest import *  # noqa


def get_random_resources(n: int) -> List[Resources]:
    """Gets n random resources."""

    resources = {
        "CPU": lambda: random.randint(0, 10),
        "GPU": lambda: random.randint(0, 10),
        "memory": lambda: random.randint(0, 10),
        "resources": lambda: {"custom_A": random.randint(0, 10)},
    }

    res = list()
    for _ in range(n):
        resource_dict = dict()
        for resource, callable in resources.items():
            if random.randint(0, 1) == 0:
                resource_dict[resource] = callable()

        res.append(Resources.from_ray_resource_dict(resource_dict))

    return res


class TestResources:
    @pytest.mark.parametrize("resource_type", ["CPU", "GPU", "memory"])
    def test_basic(self, resource_type: str):
        # basic resources
        a = Resources.from_ray_resource_dict({resource_type: 1, "resources": {}})
        b = Resources.from_ray_resource_dict({resource_type: 0})
        assert a.can_fit(b)
        assert not b.can_fit(a)

    def test_neither_bigger(self):
        a = Resources.from_ray_resource_dict({"CPU": 1, "GPU": 0})
        b = Resources.from_ray_resource_dict({"CPU": 0, "GPU": 1})
        assert not a == b
        assert not a.can_fit(b)
        assert not b.can_fit(a)

    combos = [tuple(get_random_resources(20)[i : i + 2]) for i in range(0, 20, 2)]

    @pytest.mark.parametrize("resource_A,resource_B", combos)
    def test_soft_resources_consistent_comparison(self, resource_A, resource_B):
        """Resources should have consistent comparison. Either A==B, A<B, or A>B."""

        assert (
            resource_A == resource_B
            or resource_A > resource_B
            or resource_A < resource_B
        )

    def test_compare_resources(self):
        # Prioritize GPU
        a = Resources.from_ray_resource_dict(
            {"GPU": 1, "CPU": 10, "memory": 10, "resources": {"custom": 10}}
        )
        b = Resources.from_ray_resource_dict(
            {"GPU": 2, "CPU": 0, "memory": 0, "resources": {"custom": 0}}
        )
        assert b > a

        # Then CPU
        a = Resources.from_ray_resource_dict(
            {"GPU": 1, "CPU": 1, "memory": 10, "resources": {"custom": 10}}
        )
        b = Resources.from_ray_resource_dict(
            {"GPU": 1, "CPU": 2, "memory": 0, "resources": {"custom": 0}}
        )
        assert b > a

        # Then memory
        a = Resources.from_ray_resource_dict(
            {"GPU": 1, "CPU": 1, "memory": 1, "resources": {"custom": 10}}
        )
        b = Resources.from_ray_resource_dict(
            {"GPU": 1, "CPU": 1, "memory": 2, "resources": {"custom": 0}}
        )
        assert b > a

        # Then custom resources
        a = Resources.from_ray_resource_dict(
            {"GPU": 1, "CPU": 1, "memory": 1, "resources": {"custom": 1}}
        )
        b = Resources.from_ray_resource_dict(
            {"GPU": 1, "CPU": 1, "memory": 1, "resources": {"custom": 2}}
        )
        assert b > a

    def test_sort_resources(self):
        """Prioritize GPUs, CPUs, memory, then custom resources when sorting."""

        a = Resources.from_ray_resource_dict(
            {"GPU": 0, "CPU": 4, "memory": 99, "resources": {"A": 10}}
        )
        b = Resources.from_ray_resource_dict({"GPU": 0, "CPU": 2, "memory": 100})
        c = Resources.from_ray_resource_dict({"GPU": 1, "CPU": 1, "memory": 50})
        d = Resources.from_ray_resource_dict({"GPU": 2, "CPU": 0, "memory": 0})
        e = Resources.from_ray_resource_dict(
            {"GPU": 3, "CPU": 8, "memory": 10000, "resources": {"A": 6}}
        )
        f = Resources.from_ray_resource_dict(
            {"GPU": 3, "CPU": 8, "memory": 10000, "resources": {"A": 2}}
        )

        for _ in range(10):
            resources = [a, b, c, d, e, f]
            random.shuffle(resources)
            resources.sort(reverse=True)
            assert resources == [e, f, d, c, a, b]

    def test_custom_resources(self):
        a = Resources.from_ray_resource_dict({"resources": {"alice": 2}})
        b = Resources.from_ray_resource_dict({"resources": {"alice": 3}})
        assert a < b
        assert b.can_fit(a)
        assert a + b == Resources(**{"alice": 5})

        a = Resources.from_ray_resource_dict({"resources": {"bob": 2}})
        b = Resources.from_ray_resource_dict({"CPU": 4})
        assert a + b == Resources(**{"CPU": 4, "bob": 2})

    def test_implicit_resources(self):
        r = Resources()
        # Implicit resources
        assert r.get(f"{ray._raylet.IMPLICIT_RESOURCE_PREFIX}random") == 1
        # Everything else
        assert r.get("CPU") == 0
        assert r.get("GPU") == 0
        assert r.get("memory") == 0
        assert r.get("random_custom") == 0

        # Arithmetric with implicit resources
        implicit_resource = f"{ray._raylet.IMPLICIT_RESOURCE_PREFIX}whatever"
        a = Resources()
        b = Resources.from_ray_resource_dict({"resources": {implicit_resource: 0.5}})
        assert a.get(implicit_resource) == 1
        assert b.get(implicit_resource) == 0.5
        assert a.can_fit(b)

        a -= b
        assert a.get(implicit_resource) == 0.5
        assert a.can_fit(b)

        a -= b
        assert a.get(implicit_resource) == 0
        assert not a.can_fit(b)


def test_deployment_scheduling_info():
    info = DeploymentSchedulingInfo(
        scheduling_policy=SpreadDeploymentSchedulingPolicy,
        actor_resources=Resources.from_ray_resource_dict({"CPU": 2, "GPU": 1}),
    )
    assert info.required_resources == Resources.from_ray_resource_dict(
        {"CPU": 2, "GPU": 1}
    )
    assert not info.is_non_strict_pack_pg()

    info = DeploymentSchedulingInfo(
        scheduling_policy=SpreadDeploymentSchedulingPolicy,
        actor_resources=Resources.from_ray_resource_dict({"CPU": 2, "GPU": 1}),
        placement_group_bundles=[
            Resources.from_ray_resource_dict({"CPU": 100}),
            Resources.from_ray_resource_dict({"GPU": 100}),
        ],
        placement_group_strategy="STRICT_PACK",
    )
    assert info.required_resources == Resources.from_ray_resource_dict(
        {"CPU": 100, "GPU": 100}
    )
    assert not info.is_non_strict_pack_pg()

    info = DeploymentSchedulingInfo(
        scheduling_policy=SpreadDeploymentSchedulingPolicy,
        actor_resources=Resources.from_ray_resource_dict({"CPU": 2, "GPU": 1}),
        placement_group_bundles=[
            Resources.from_ray_resource_dict({"CPU": 100}),
            Resources.from_ray_resource_dict({"GPU": 100}),
        ],
        placement_group_strategy="PACK",
    )
    assert info.required_resources == Resources.from_ray_resource_dict(
        {"CPU": 2, "GPU": 1}
    )
    assert info.is_non_strict_pack_pg()


def test_downscale_multiple_deployments():
    """Test to make sure downscale prefers replicas without node id
    and then replicas on a node with fewest replicas of all deployments.
    """

    cluster_node_info_cache = MockClusterNodeInfoCache()
    scheduler = DefaultDeploymentScheduler(
        cluster_node_info_cache, head_node_id="fake-head-node-id"
    )

    d1_id = DeploymentID(name="deployment1")
    d2_id = DeploymentID(name="deployment2")
    scheduler.on_deployment_created(d1_id, SpreadDeploymentSchedulingPolicy())
    scheduler.on_deployment_created(d2_id, SpreadDeploymentSchedulingPolicy())
    scheduler.on_replica_running(d1_id, "replica1", "node1")
    scheduler.on_replica_running(d1_id, "replica2", "node2")
    scheduler.on_replica_running(d1_id, "replica3", "node2")
    scheduler.on_replica_running(d2_id, "replica1", "node1")
    scheduler.on_replica_running(d2_id, "replica2", "node2")
    scheduler.on_replica_running(d2_id, "replica3", "node1")
    scheduler.on_replica_running(d2_id, "replica4", "node1")
    deployment_to_replicas_to_stop = scheduler.schedule(
        upscales={},
        downscales={
            d1_id: DeploymentDownscaleRequest(deployment_id=d1_id, num_to_stop=1)
        },
    )
    assert len(deployment_to_replicas_to_stop) == 1
    # Even though node1 has fewest replicas of deployment1
    # but it has more replicas of all deployments so
    # we should stop replicas from node2.
    assert len(deployment_to_replicas_to_stop[d1_id]) == 1
    assert deployment_to_replicas_to_stop[d1_id] < {"replica2", "replica3"}

    scheduler.on_replica_stopping(d1_id, "replica3")
    scheduler.on_replica_stopping(d2_id, "replica3")
    scheduler.on_replica_stopping(d2_id, "replica4")

    deployment_to_replicas_to_stop = scheduler.schedule(
        upscales={},
        downscales={
            d1_id: DeploymentDownscaleRequest(deployment_id=d1_id, num_to_stop=1),
            d2_id: DeploymentDownscaleRequest(deployment_id=d2_id, num_to_stop=1),
        },
    )
    assert len(deployment_to_replicas_to_stop) == 2
    # We should stop replicas from the same node.
    assert len(deployment_to_replicas_to_stop[d1_id]) == 1
    assert (
        deployment_to_replicas_to_stop[d1_id] == deployment_to_replicas_to_stop[d2_id]
    )

    scheduler.on_replica_stopping(d1_id, "replica1")
    scheduler.on_replica_stopping(d1_id, "replica2")
    scheduler.on_replica_stopping(d2_id, "replica1")
    scheduler.on_replica_stopping(d2_id, "replica2")
    scheduler.on_deployment_deleted(d1_id)
    scheduler.on_deployment_deleted(d2_id)


def test_downscale_head_node():
    """Test to make sure downscale deprioritizes replicas on the head node."""

    head_node_id = "fake-head-node-id"
    dep_id = DeploymentID(name="deployment1")
    cluster_node_info_cache = MockClusterNodeInfoCache()
    scheduler = DefaultDeploymentScheduler(
        cluster_node_info_cache, head_node_id=head_node_id
    )

    scheduler.on_deployment_created(dep_id, SpreadDeploymentSchedulingPolicy())
    scheduler.on_replica_running(dep_id, "replica1", head_node_id)
    scheduler.on_replica_running(dep_id, "replica2", "node2")
    scheduler.on_replica_running(dep_id, "replica3", "node2")
    deployment_to_replicas_to_stop = scheduler.schedule(
        upscales={},
        downscales={
            dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
        },
    )
    assert len(deployment_to_replicas_to_stop) == 1
    assert deployment_to_replicas_to_stop[dep_id] < {"replica2", "replica3"}
    scheduler.on_replica_stopping(dep_id, deployment_to_replicas_to_stop[dep_id].pop())

    deployment_to_replicas_to_stop = scheduler.schedule(
        upscales={},
        downscales={
            dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
        },
    )
    assert len(deployment_to_replicas_to_stop) == 1
    assert deployment_to_replicas_to_stop[dep_id] < {"replica2", "replica3"}
    scheduler.on_replica_stopping(dep_id, deployment_to_replicas_to_stop[dep_id].pop())

    deployment_to_replicas_to_stop = scheduler.schedule(
        upscales={},
        downscales={
            dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
        },
    )
    assert len(deployment_to_replicas_to_stop) == 1
    assert deployment_to_replicas_to_stop[dep_id] == {"replica1"}
    scheduler.on_replica_stopping(dep_id, "replica1")
    scheduler.on_deployment_deleted(dep_id)


def test_downscale_single_deployment():
    """Test to make sure downscale prefers replicas without node id
    and then replicas on a node with fewest replicas of all deployments.
    """

    dep_id = DeploymentID(name="deployment1")
    cluster_node_info_cache = MockClusterNodeInfoCache()
    cluster_node_info_cache.add_node("node1")
    cluster_node_info_cache.add_node("node2")
    scheduler = DefaultDeploymentScheduler(
        cluster_node_info_cache, head_node_id="fake-head-node-id"
    )

    scheduler.on_deployment_created(dep_id, SpreadDeploymentSchedulingPolicy())
    scheduler.on_deployment_deployed(
        dep_id, ReplicaConfig.create(lambda x: x, ray_actor_options={"num_cpus": 0})
    )
    scheduler.on_replica_running(dep_id, "replica1", "node1")
    scheduler.on_replica_running(dep_id, "replica2", "node1")
    scheduler.on_replica_running(dep_id, "replica3", "node2")
    scheduler.on_replica_recovering(dep_id, "replica4")
    deployment_to_replicas_to_stop = scheduler.schedule(
        upscales={},
        downscales={
            dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
        },
    )
    assert len(deployment_to_replicas_to_stop) == 1
    # Prefer replica without node id
    assert deployment_to_replicas_to_stop[dep_id] == {"replica4"}
    scheduler.on_replica_stopping(dep_id, "replica4")

    deployment_to_replicas_to_stop = scheduler.schedule(
        upscales={
            dep_id: [
                ReplicaSchedulingRequest(
                    deployment_id=dep_id,
                    replica_name="replica5",
                    actor_def=Mock(),
                    actor_resources={"CPU": 1},
                    actor_options={},
                    actor_init_args=(),
                    on_scheduled=lambda actor_handle, placement_group: actor_handle,
                ),
            ]
        },
        downscales={},
    )
    assert not deployment_to_replicas_to_stop
    deployment_to_replicas_to_stop = scheduler.schedule(
        upscales={},
        downscales={
            dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
        },
    )
    assert len(deployment_to_replicas_to_stop) == 1
    # Prefer replica without node id
    assert deployment_to_replicas_to_stop[dep_id] == {"replica5"}
    scheduler.on_replica_stopping(dep_id, "replica5")

    deployment_to_replicas_to_stop = scheduler.schedule(
        upscales={},
        downscales={
            dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
        },
    )
    assert len(deployment_to_replicas_to_stop) == 1
    # Prefer replica on a node with fewest replicas of all deployments.
    assert deployment_to_replicas_to_stop[dep_id] == {"replica3"}
    scheduler.on_replica_stopping(dep_id, "replica3")

    deployment_to_replicas_to_stop = scheduler.schedule(
        upscales={},
        downscales={
            dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=2)
        },
    )
    assert len(deployment_to_replicas_to_stop) == 1
    assert deployment_to_replicas_to_stop[dep_id] == {"replica1", "replica2"}
    scheduler.on_replica_stopping(dep_id, "replica1")
    scheduler.on_replica_stopping(dep_id, "replica2")
    scheduler.on_deployment_deleted(dep_id)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
