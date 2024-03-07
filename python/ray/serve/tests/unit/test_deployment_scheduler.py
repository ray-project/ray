import random
import sys
from copy import copy
from typing import Dict, List, Optional
from unittest.mock import Mock

import pytest

import ray
from ray.serve._private import default_impl
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.config import ReplicaConfig
from ray.serve._private.constants import RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY
from ray.serve._private.deployment_scheduler import (
    DeploymentDownscaleRequest,
    DeploymentSchedulingInfo,
    ReplicaSchedulingRequest,
    Resources,
    SpreadDeploymentSchedulingPolicy,
)
from ray.serve._private.test_utils import MockClusterNodeInfoCache
from ray.tests.conftest import *  # noqa
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)


def dummy():
    pass


class MockActorHandle:
    def __init__(self, **kwargs):
        self._options = kwargs


class MockActorClass:
    def __init__(self):
        self._init_args = ()
        self._options = dict()

    def options(self, **kwargs):
        res = copy(self)

        for k, v in kwargs.items():
            res._options[k] = v

        return res

    def remote(self, *args) -> MockActorHandle:
        return MockActorHandle(init_args=args, **self._options)


class MockPlacementGroup:
    def __init__(
        self,
        bundles: List[Dict[str, float]],
        strategy: str = "PACK",
        name: str = "",
        lifetime: Optional[str] = None,
        _soft_target_node_id: Optional[str] = None,
    ):
        self._bundles = bundles
        self._strategy = strategy
        self._name = name
        self._lifetime = lifetime
        self._soft_target_node_id = _soft_target_node_id


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


def test_get_available_resources_per_node():
    d_id = DeploymentID("a", "b")

    cluster_node_info_cache = MockClusterNodeInfoCache()
    cluster_node_info_cache.add_node("node1", {"GPU": 10, "CPU": 32, "memory": 1024})

    scheduler = default_impl.create_deployment_scheduler(
        cluster_node_info_cache,
        head_node_id_override="fake-head-node-id",
        create_placement_group_fn_override=None,
    )
    scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
    scheduler.on_deployment_deployed(
        d_id,
        ReplicaConfig.create(
            dummy,
            ray_actor_options={"num_gpus": 1, "num_cpus": 3},
            max_replicas_per_node=4,
        ),
    )

    # Without updating cluster node info cache, when a replica is marked
    # as launching, the resources it uses should decrease the scheduler's
    # view of current available resources per node in the cluster
    scheduler._on_replica_launching(
        ReplicaID(unique_id="replica0", deployment_id=d_id), target_node_id="node1"
    )
    assert scheduler._get_available_resources_per_node().get("node1") == Resources(
        **{"GPU": 9, "CPU": 29, "memory": 1024}
    )

    # Similarly when a replica is marked as running, the resources it
    # uses should decrease current available resources per node
    scheduler.on_replica_running(
        ReplicaID(unique_id="replica1", deployment_id=d_id), node_id="node1"
    )
    assert scheduler._get_available_resources_per_node().get("node1") == Resources(
        **{"GPU": 8, "CPU": 26, "memory": 1024}
    )

    # Get updated info from GCS that available memory has dropped,
    # the decreased memory should reflect in current available resources
    # per node, while also keeping track of the CPU and GPU resources
    # used by launching and running replicas
    cluster_node_info_cache.set_available_resources_per_node(
        "node1", {"GPU": 10, "CPU": 32, "memory": 256}
    )
    assert scheduler._get_available_resources_per_node().get("node1") == Resources(
        **{"GPU": 8, "CPU": 26, "memory": 256}
    )


def test_downscale_multiple_deployments():
    """Test to make sure downscale prefers replicas without node id
    and then replicas on a node with fewest replicas of all deployments.
    """

    cluster_node_info_cache = MockClusterNodeInfoCache()
    scheduler = default_impl.create_deployment_scheduler(
        cluster_node_info_cache,
        head_node_id_override="fake-head-node-id",
        create_placement_group_fn_override=None,
    )

    d1_id = DeploymentID(name="deployment1")
    d2_id = DeploymentID(name="deployment2")
    d1_r1_id = ReplicaID(
        unique_id="replica1",
        deployment_id=d1_id,
    )
    d1_r2_id = ReplicaID(
        unique_id="replica2",
        deployment_id=d1_id,
    )
    d1_r3_id = ReplicaID(
        unique_id="replica3",
        deployment_id=d1_id,
    )
    d2_r1_id = ReplicaID(
        unique_id="replica1",
        deployment_id=d2_id,
    )
    d2_r2_id = ReplicaID(
        unique_id="replica2",
        deployment_id=d2_id,
    )
    d2_r3_id = ReplicaID(
        unique_id="replica3",
        deployment_id=d2_id,
    )
    d2_r4_id = ReplicaID(
        unique_id="replica4",
        deployment_id=d2_id,
    )
    scheduler.on_deployment_created(d1_id, SpreadDeploymentSchedulingPolicy())
    scheduler.on_deployment_created(d2_id, SpreadDeploymentSchedulingPolicy())
    scheduler.on_replica_running(d1_r1_id, "node1")
    scheduler.on_replica_running(d1_r2_id, "node2")
    scheduler.on_replica_running(d1_r3_id, "node2")
    scheduler.on_replica_running(d2_r1_id, "node1")
    scheduler.on_replica_running(d2_r2_id, "node2")
    scheduler.on_replica_running(d2_r3_id, "node1")
    scheduler.on_replica_running(d2_r4_id, "node1")
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
    assert deployment_to_replicas_to_stop[d1_id] < {d1_r2_id, d1_r3_id}

    scheduler.on_replica_stopping(d1_r3_id)
    scheduler.on_replica_stopping(d2_r3_id)
    scheduler.on_replica_stopping(d2_r4_id)

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
    assert {r.unique_id for r in deployment_to_replicas_to_stop[d1_id]} == {
        r.unique_id for r in deployment_to_replicas_to_stop[d2_id]
    }

    scheduler.on_replica_stopping(d1_r1_id)
    scheduler.on_replica_stopping(d1_r2_id)
    scheduler.on_replica_stopping(d2_r1_id)
    scheduler.on_replica_stopping(d2_r2_id)
    scheduler.on_deployment_deleted(d1_id)
    scheduler.on_deployment_deleted(d2_id)


def test_downscale_head_node():
    """Test to make sure downscale deprioritizes replicas on the head node."""

    head_node_id = "fake-head-node-id"
    dep_id = DeploymentID(name="deployment1")
    cluster_node_info_cache = MockClusterNodeInfoCache()
    scheduler = default_impl.create_deployment_scheduler(
        cluster_node_info_cache,
        head_node_id_override=head_node_id,
        create_placement_group_fn_override=None,
    )

    r1_id = ReplicaID(
        unique_id="replica1",
        deployment_id=dep_id,
    )
    r2_id = ReplicaID(
        unique_id="replica2",
        deployment_id=dep_id,
    )
    r3_id = ReplicaID(
        unique_id="replica3",
        deployment_id=dep_id,
    )
    scheduler.on_deployment_created(dep_id, SpreadDeploymentSchedulingPolicy())
    scheduler.on_replica_running(r1_id, head_node_id)
    scheduler.on_replica_running(r2_id, "node2")
    scheduler.on_replica_running(r3_id, "node2")
    deployment_to_replicas_to_stop = scheduler.schedule(
        upscales={},
        downscales={
            dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
        },
    )
    assert len(deployment_to_replicas_to_stop) == 1
    assert deployment_to_replicas_to_stop[dep_id] < {r2_id, r3_id}
    scheduler.on_replica_stopping(deployment_to_replicas_to_stop[dep_id].pop())

    deployment_to_replicas_to_stop = scheduler.schedule(
        upscales={},
        downscales={
            dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
        },
    )
    assert len(deployment_to_replicas_to_stop) == 1
    assert deployment_to_replicas_to_stop[dep_id] < {r2_id, r3_id}
    scheduler.on_replica_stopping(deployment_to_replicas_to_stop[dep_id].pop())

    deployment_to_replicas_to_stop = scheduler.schedule(
        upscales={},
        downscales={
            dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
        },
    )
    assert len(deployment_to_replicas_to_stop) == 1
    assert deployment_to_replicas_to_stop[dep_id] == {r1_id}
    scheduler.on_replica_stopping(r1_id)
    scheduler.on_deployment_deleted(dep_id)


def test_downscale_single_deployment():
    """Test to make sure downscale prefers replicas without node id
    and then replicas on a node with fewest replicas of all deployments.
    """

    dep_id = DeploymentID(name="deployment1")
    cluster_node_info_cache = MockClusterNodeInfoCache()
    cluster_node_info_cache.add_node("node1")
    cluster_node_info_cache.add_node("node2")
    scheduler = default_impl.create_deployment_scheduler(
        cluster_node_info_cache,
        head_node_id_override="fake-head-node-id",
        create_placement_group_fn_override=None,
    )

    scheduler.on_deployment_created(dep_id, SpreadDeploymentSchedulingPolicy())
    scheduler.on_deployment_deployed(
        dep_id, ReplicaConfig.create(lambda x: x, ray_actor_options={"num_cpus": 0})
    )
    r1_id = ReplicaID(
        unique_id="replica1",
        deployment_id=dep_id,
    )
    r2_id = ReplicaID(
        unique_id="replica2",
        deployment_id=dep_id,
    )
    r3_id = ReplicaID(
        unique_id="replica3",
        deployment_id=dep_id,
    )
    r4_id = ReplicaID(
        unique_id="replica4",
        deployment_id=dep_id,
    )
    scheduler.on_replica_running(r1_id, "node1")
    scheduler.on_replica_running(r2_id, "node1")
    scheduler.on_replica_running(r3_id, "node2")
    scheduler.on_replica_recovering(r4_id)
    deployment_to_replicas_to_stop = scheduler.schedule(
        upscales={},
        downscales={
            dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
        },
    )
    assert len(deployment_to_replicas_to_stop) == 1
    # Prefer replica without node id
    assert deployment_to_replicas_to_stop[dep_id] == {r4_id}
    scheduler.on_replica_stopping(r4_id)

    r5_id = ReplicaID(
        unique_id="replica5",
        deployment_id=dep_id,
    )
    deployment_to_replicas_to_stop = scheduler.schedule(
        upscales={
            dep_id: [
                ReplicaSchedulingRequest(
                    replica_id=r5_id,
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
    assert deployment_to_replicas_to_stop[dep_id] == {r5_id}
    scheduler.on_replica_stopping(r5_id)

    deployment_to_replicas_to_stop = scheduler.schedule(
        upscales={},
        downscales={
            dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
        },
    )
    assert len(deployment_to_replicas_to_stop) == 1
    # Prefer replica on a node with fewest replicas of all deployments.
    assert deployment_to_replicas_to_stop[dep_id] == {r3_id}
    scheduler.on_replica_stopping(r3_id)

    deployment_to_replicas_to_stop = scheduler.schedule(
        upscales={},
        downscales={
            dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=2)
        },
    )
    assert len(deployment_to_replicas_to_stop) == 1
    assert deployment_to_replicas_to_stop[dep_id] == {r1_id, r2_id}
    scheduler.on_replica_stopping(r1_id)
    scheduler.on_replica_stopping(r2_id)
    scheduler.on_deployment_deleted(dep_id)


@pytest.mark.skipif(
    not RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY, reason="Needs compact strategy."
)
class TestCompactScheduling:
    def test_basic(self):
        d_id1 = DeploymentID(name="deployment1")
        d_id2 = DeploymentID(name="deployment2")

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node("node1", {"CPU": 3})
        cluster_node_info_cache.add_node("node2", {"CPU": 2})
        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="fake-head-node-id",
            create_placement_group_fn_override=None,
        )

        scheduler.on_deployment_created(d_id1, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_created(d_id2, SpreadDeploymentSchedulingPolicy())

        scheduler.on_deployment_deployed(
            d_id1,
            ReplicaConfig.create(dummy, ray_actor_options={"num_cpus": 1}),
        )
        scheduler.on_deployment_deployed(
            d_id2,
            ReplicaConfig.create(dummy, ray_actor_options={"num_cpus": 3}),
        )

        on_scheduled_mock = Mock()
        on_scheduled_mock2 = Mock()
        scheduler.schedule(
            upscales={
                d_id1: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(unique_id=f"r{i}", deployment_id=d_id1),
                        actor_def=MockActorClass(),
                        actor_resources={"CPU": 1},
                        actor_options={},
                        actor_init_args=(),
                        on_scheduled=on_scheduled_mock,
                    )
                    for i in range(2)
                ],
                d_id2: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(unique_id="r2", deployment_id=d_id2),
                        actor_def=MockActorClass(),
                        actor_resources={"CPU": 3},
                        actor_options={},
                        actor_init_args=(),
                        on_scheduled=on_scheduled_mock2,
                    )
                ],
            },
            downscales={},
        )

        assert len(on_scheduled_mock.call_args_list) == 2
        for call in on_scheduled_mock.call_args_list:
            assert call.kwargs == {"placement_group": None}
            assert len(call.args) == 1
            scheduling_strategy = call.args[0]._options["scheduling_strategy"]
            assert isinstance(scheduling_strategy, NodeAffinitySchedulingStrategy)
            assert scheduling_strategy.node_id == "node2"

        assert len(on_scheduled_mock2.call_args_list) == 1
        call = on_scheduled_mock2.call_args_list[0]
        assert call.kwargs == {"placement_group": None}
        assert len(call.args) == 1
        scheduling_strategy = call.args[0]._options["scheduling_strategy"]
        assert isinstance(scheduling_strategy, NodeAffinitySchedulingStrategy)
        assert scheduling_strategy.node_id == "node1"

    def test_placement_groups(self):
        d_id1 = DeploymentID(name="deployment1")
        d_id2 = DeploymentID(name="deployment2")

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node("node1", {"CPU": 3})
        cluster_node_info_cache.add_node("node2", {"CPU": 2})
        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="fake-head-node-id",
            create_placement_group_fn_override=lambda *args, **kwargs: MockPlacementGroup(  # noqa
                *args, **kwargs
            ),
        )

        ray.util.placement_group
        scheduler.on_deployment_created(d_id1, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_created(d_id2, SpreadDeploymentSchedulingPolicy())

        scheduler.on_deployment_deployed(
            d_id1,
            ReplicaConfig.create(
                dummy,
                ray_actor_options={"num_cpus": 0},
                placement_group_bundles=[{"CPU": 0.5}, {"CPU": 0.5}],
                placement_group_strategy="STRICT_PACK",
            ),
        )
        scheduler.on_deployment_deployed(
            d_id2,
            ReplicaConfig.create(
                dummy,
                ray_actor_options={"num_cpus": 0},
                placement_group_bundles=[{"CPU": 0.5}, {"CPU": 2.5}],
                placement_group_strategy="STRICT_PACK",
            ),
        )

        on_scheduled_mock = Mock()
        on_scheduled_mock2 = Mock()
        scheduler.schedule(
            upscales={
                d_id1: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(unique_id=f"r{i}", deployment_id=d_id1),
                        actor_def=MockActorClass(),
                        actor_resources={"CPU": 0},
                        placement_group_bundles=[{"CPU": 0.5}, {"CPU": 0.5}],
                        placement_group_strategy="STRICT_PACK",
                        actor_options={"name": "random_replica"},
                        actor_init_args=(),
                        on_scheduled=on_scheduled_mock,
                    )
                    for i in range(2)
                ],
                d_id2: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(unique_id="r2", deployment_id=d_id2),
                        actor_def=MockActorClass(),
                        actor_resources={"CPU": 0},
                        placement_group_bundles=[{"CPU": 0.5}, {"CPU": 2.5}],
                        placement_group_strategy="STRICT_PACK",
                        actor_options={"name": "some_replica"},
                        actor_init_args=(),
                        on_scheduled=on_scheduled_mock2,
                    )
                ],
            },
            downscales={},
        )

        assert len(on_scheduled_mock.call_args_list) == 2
        for call in on_scheduled_mock.call_args_list:
            assert len(call.args) == 1
            scheduling_strategy = call.args[0]._options["scheduling_strategy"]
            assert isinstance(scheduling_strategy, PlacementGroupSchedulingStrategy)
            assert call.kwargs.get("placement_group")._soft_target_node_id == "node2"

        assert len(on_scheduled_mock2.call_args_list) == 1
        call = on_scheduled_mock2.call_args_list[0]
        assert len(call.args) == 1
        scheduling_strategy = call.args[0]._options["scheduling_strategy"]
        assert isinstance(scheduling_strategy, PlacementGroupSchedulingStrategy)
        assert call.kwargs.get("placement_group")._soft_target_node_id == "node1"

    def test_heterogeneous_resources(self):
        d_id1 = DeploymentID(name="deployment1")
        d_id2 = DeploymentID(name="deployment2")

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node("node1", {"GPU": 4, "CPU": 6})
        cluster_node_info_cache.add_node("node2", {"GPU": 10, "CPU": 2})
        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="fake-head-node-id",
            create_placement_group_fn_override=None,
        )

        scheduler.on_deployment_created(d_id1, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_created(d_id2, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_deployed(
            d_id1,
            ReplicaConfig.create(
                dummy, ray_actor_options={"num_gpus": 2, "num_cpus": 2}
            ),
        )
        scheduler.on_deployment_deployed(
            d_id2,
            ReplicaConfig.create(
                dummy, ray_actor_options={"num_gpus": 1, "num_cpus": 1}
            ),
        )

        on_scheduled_mock = Mock()
        scheduler.schedule(
            upscales={
                d_id1: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(unique_id="r0", deployment_id=d_id1),
                        actor_def=MockActorClass(),
                        actor_resources={"GPU": 2, "CPU": 2},
                        actor_options={},
                        actor_init_args=(),
                        on_scheduled=on_scheduled_mock,
                    )
                ],
                d_id2: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(unique_id=f"r{i+1}", deployment_id=d_id2),
                        actor_def=MockActorClass(),
                        actor_resources={"GPU": 1, "CPU": 1},
                        actor_options={},
                        actor_init_args=(),
                        on_scheduled=on_scheduled_mock,
                    )
                    for i in range(2)
                ],
            },
            downscales={},
        )

        # Even though scheduling on node 2 would minimize fragmentation
        # of CPU resources, we should prioritize minimizing fragmentation
        # of GPU resources first, so all 3 replicas should be scheduled
        # to node 1
        assert len(on_scheduled_mock.call_args_list) == 3
        for call in on_scheduled_mock.call_args_list:
            assert len(call.args) == 1
            scheduling_strategy = call.args[0]._options["scheduling_strategy"]
            assert isinstance(scheduling_strategy, NodeAffinitySchedulingStrategy)
            assert scheduling_strategy.node_id == "node1"
            assert call.kwargs == {"placement_group": None}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
