import random
import sys
from collections import defaultdict
from typing import List
from unittest import mock
from unittest.mock import Mock

import pytest

import ray
from ray._raylet import NodeID
from ray.serve._private import default_impl
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.config import ReplicaConfig
from ray.serve._private.constants import (
    RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY,
)
from ray.serve._private.deployment_scheduler import (
    DeploymentDownscaleRequest,
    DeploymentSchedulingInfo,
    ReplicaSchedulingRequest,
    Resources,
    SpreadDeploymentSchedulingPolicy,
)
from ray.serve._private.test_utils import (
    MockActorClass,
    MockClusterNodeInfoCache,
    MockPlacementGroup,
)
from ray.tests.conftest import *  # noqa
from ray.util.scheduling_strategies import (
    In,
    NodeAffinitySchedulingStrategy,
    NodeLabelSchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)


def dummy():
    pass


def rconfig(**config_opts):
    return ReplicaConfig.create(dummy, **config_opts)


def get_random_resources(n: int) -> List[Resources]:
    """Gets n random resources."""

    resources = {
        "CPU": lambda: random.randint(0, 10),
        "GPU": lambda: random.randint(0, 10),
        "memory": lambda: random.randint(0, 10),
        "custom_A": lambda: random.randint(0, 10),
    }

    res = list()
    for _ in range(n):
        resource_dict = dict()
        for resource, callable in resources.items():
            if random.randint(0, 1) == 0:
                resource_dict[resource] = callable()

        res.append(Resources(resource_dict))

    return res


class TestResources:
    @pytest.mark.parametrize("resource_type", ["CPU", "GPU", "memory"])
    def test_basic(self, resource_type: str):
        # basic resources
        a = Resources({resource_type: 1})
        b = Resources({resource_type: 0})
        assert a.can_fit(b)
        assert not b.can_fit(a)

    def test_neither_bigger(self):
        a = Resources({"CPU": 1, "GPU": 0})
        b = Resources({"CPU": 0, "GPU": 1})
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
        a = Resources({"GPU": 1, "CPU": 10, "memory": 10, "custom": 10})
        b = Resources({"GPU": 2, "CPU": 0, "memory": 0, "custom": 0})
        assert b > a

        # Then CPU
        a = Resources({"GPU": 1, "CPU": 1, "memory": 10, "custom": 10})
        b = Resources({"GPU": 1, "CPU": 2, "memory": 0, "custom": 0})
        assert b > a

        # Then memory
        a = Resources({"GPU": 1, "CPU": 1, "memory": 1, "custom": 10})
        b = Resources({"GPU": 1, "CPU": 1, "memory": 2, "custom": 0})
        assert b > a

        # Then custom resources
        a = Resources({"GPU": 1, "CPU": 1, "memory": 1, "custom": 1})
        b = Resources({"GPU": 1, "CPU": 1, "memory": 1, "custom": 2})
        assert b > a

    def test_sort_resources(self):
        """Prioritize GPUs, CPUs, memory, then custom resources when sorting."""

        a = Resources({"GPU": 0, "CPU": 4, "memory": 99, "A": 10})
        b = Resources({"GPU": 0, "CPU": 2, "memory": 100})
        c = Resources({"GPU": 1, "CPU": 1, "memory": 50})
        d = Resources({"GPU": 2, "CPU": 0, "memory": 0})
        e = Resources({"GPU": 3, "CPU": 8, "memory": 10000, "A": 6})
        f = Resources({"GPU": 3, "CPU": 8, "memory": 10000, "A": 2})

        for _ in range(10):
            resources = [a, b, c, d, e, f]
            random.shuffle(resources)
            resources.sort(reverse=True)
            assert resources == [e, f, d, c, a, b]

    def test_custom_resources(self):
        a = Resources({"alice": 2})
        b = Resources({"alice": 3})
        assert a < b
        assert b.can_fit(a)
        assert a + b == Resources(**{"alice": 5})

        a = Resources({"bob": 2})
        b = Resources({"CPU": 4})
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
        b = Resources({implicit_resource: 0.5})
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
        deployment_id=DeploymentID("a", "b"),
        scheduling_policy=SpreadDeploymentSchedulingPolicy,
        actor_resources=Resources({"CPU": 2, "GPU": 1}),
    )
    assert info.required_resources == Resources({"CPU": 2, "GPU": 1})
    assert not info.is_non_strict_pack_pg()

    info = DeploymentSchedulingInfo(
        deployment_id=DeploymentID("a", "b"),
        scheduling_policy=SpreadDeploymentSchedulingPolicy,
        actor_resources=Resources({"CPU": 2, "GPU": 1}),
        placement_group_bundles=[
            Resources({"CPU": 100}),
            Resources({"GPU": 100}),
        ],
        placement_group_strategy="STRICT_PACK",
    )
    assert info.required_resources == Resources({"CPU": 100, "GPU": 100})
    assert not info.is_non_strict_pack_pg()

    info = DeploymentSchedulingInfo(
        deployment_id=DeploymentID("a", "b"),
        scheduling_policy=SpreadDeploymentSchedulingPolicy,
        actor_resources=Resources({"CPU": 2, "GPU": 1}),
        placement_group_bundles=[
            Resources({"CPU": 100}),
            Resources({"GPU": 100}),
        ],
        placement_group_strategy="PACK",
    )
    assert info.required_resources == Resources({"CPU": 2, "GPU": 1})
    assert info.is_non_strict_pack_pg()


def test_get_available_resources_per_node():
    d_id = DeploymentID("a", "b")

    cluster_node_info_cache = MockClusterNodeInfoCache()
    cluster_node_info_cache.add_node(
        "node1", {"GPU": 10, "CPU": 32, "memory": 1024, "customx": 1}
    )

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
            ray_actor_options={
                "num_gpus": 1,
                "num_cpus": 3,
                "resources": {"customx": 0.1},
            },
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
        **{
            "GPU": 9,
            "CPU": 29,
            "memory": 1024,
            "customx": 0.9,
            f"{ray._raylet.IMPLICIT_RESOURCE_PREFIX}b:a": 0.75,
        }
    )

    # Similarly when a replica is marked as running, the resources it
    # uses should decrease current available resources per node
    scheduler.on_replica_running(
        ReplicaID(unique_id="replica1", deployment_id=d_id), node_id="node1"
    )
    assert scheduler._get_available_resources_per_node().get("node1") == Resources(
        **{
            "GPU": 8,
            "CPU": 26,
            "memory": 1024,
            "customx": 0.8,
            f"{ray._raylet.IMPLICIT_RESOURCE_PREFIX}b:a": 0.5,
        }
    )

    # Get updated info from GCS that available MEMORY has dropped,
    # the decreased memory should reflect in current available resources
    # per node, while also keeping track of the CPU, GPU, custom resources
    # used by launching and running replicas
    cluster_node_info_cache.set_available_resources_per_node(
        "node1", {"GPU": 10, "CPU": 32, "memory": 256, "customx": 1}
    )
    assert scheduler._get_available_resources_per_node().get("node1") == Resources(
        **{
            "GPU": 8,
            "CPU": 26,
            "memory": 256,
            "customx": 0.8,
            f"{ray._raylet.IMPLICIT_RESOURCE_PREFIX}b:a": 0.5,
        }
    )


def test_get_node_to_running_replicas():
    """Test DeploymentScheduler._get_node_to_running_replicas()."""

    d_id = DeploymentID("a", "b")
    scheduler = default_impl.create_deployment_scheduler(
        MockClusterNodeInfoCache(),
        head_node_id_override="fake-head-node-id",
        create_placement_group_fn_override=None,
    )
    scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
    scheduler.on_deployment_deployed(d_id, rconfig())

    # Test simple fixed case
    scheduler.on_replica_running(ReplicaID("r1", d_id), "node1")
    scheduler.on_replica_running(ReplicaID("r2", d_id), "node1")
    scheduler.on_replica_running(ReplicaID("r3", d_id), "node2")
    assert scheduler._get_node_to_running_replicas() == {
        "node1": {ReplicaID("r1", d_id), ReplicaID("r2", d_id)},
        "node2": {ReplicaID("r3", d_id)},
    }
    scheduler.on_replica_stopping(ReplicaID("r1", d_id))
    scheduler.on_replica_stopping(ReplicaID("r2", d_id))
    scheduler.on_replica_stopping(ReplicaID("r3", d_id))

    # Test random case
    node_to_running_replicas = defaultdict(set)
    for i in range(40):
        node_id = f"node{random.randint(0,5)}"
        r_id = ReplicaID(f"r{i}", d_id)
        node_to_running_replicas[node_id].add(r_id)
        scheduler.on_replica_running(r_id, node_id)
    assert scheduler._get_node_to_running_replicas() == node_to_running_replicas


def test_get_available_resources_per_node_pg():
    """Test DeploymentScheduler._get_available_resources_per_node()."""

    d_id = DeploymentID("a", "b")

    cluster_node_info_cache = MockClusterNodeInfoCache()
    cluster_node_info_cache.add_node(
        "node1", {"GPU": 10, "CPU": 32, "memory": 1024, "customx": 1}
    )

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
            ray_actor_options={"num_cpus": 0},
            placement_group_bundles=[{"GPU": 1}, {"CPU": 3}, {"customx": 0.1}],
            placement_group_strategy="STRICT_PACK",
        ),
    )

    # Without updating cluster node info cache, when a replica is marked
    # as launching, the resources it uses should decrease the scheduler's
    # view of current available resources per node in the cluster
    scheduler._on_replica_launching(
        ReplicaID(unique_id="replica0", deployment_id=d_id), target_node_id="node1"
    )
    assert scheduler._get_available_resources_per_node().get("node1") == Resources(
        **{
            "GPU": 9,
            "CPU": 29,
            "memory": 1024,
            "customx": 0.9,
        }
    )

    # Similarly when a replica is marked as running, the resources it
    # uses should decrease current available resources per node
    scheduler.on_replica_running(
        ReplicaID(unique_id="replica1", deployment_id=d_id), node_id="node1"
    )
    assert scheduler._get_available_resources_per_node().get("node1") == Resources(
        **{
            "GPU": 8,
            "CPU": 26,
            "memory": 1024,
            "customx": 0.8,
        }
    )

    # Get updated info from GCS that available MEMORY has dropped,
    # the decreased memory should reflect in current available resources
    # per node, while also keeping track of the CPU, GPU, custom resources
    # used by launching and running replicas
    cluster_node_info_cache.set_available_resources_per_node(
        "node1", {"GPU": 10, "CPU": 32, "memory": 256, "customx": 1}
    )
    assert scheduler._get_available_resources_per_node().get("node1") == Resources(
        **{
            "GPU": 8,
            "CPU": 26,
            "memory": 256,
            "customx": 0.8,
        }
    )


def test_best_fit_node():
    """Test DeploymentScheduler._best_fit_node()."""

    scheduler = default_impl.create_deployment_scheduler(
        MockClusterNodeInfoCache(),
        head_node_id_override="fake-head-node-id",
        create_placement_group_fn_override=None,
    )

    # None of the nodes can schedule the replica
    assert (
        scheduler._best_fit_node(
            required_resources=Resources(GPU=1, CPU=1, customx=0.1),
            available_resources={
                "node1": Resources(GPU=3, CPU=3),
                "node2": Resources(CPU=3, customx=1),
            },
        )
        is None
    )

    # Only node2 can fit the replica
    assert "node2" == scheduler._best_fit_node(
        required_resources=Resources(GPU=1, CPU=1, customx=0.1),
        available_resources={
            "node1": Resources(CPU=3),
            "node2": Resources(GPU=1, CPU=3, customx=1),
            "node3": Resources(CPU=3, customx=1),
        },
    )

    # We should prioritize minimizing fragementation of GPUs over CPUs
    assert "node1" == scheduler._best_fit_node(
        required_resources=Resources(GPU=1, CPU=1, customx=0.1),
        available_resources={
            "node1": Resources(GPU=2, CPU=10, customx=1),
            "node2": Resources(GPU=10, CPU=2, customx=1),
        },
    )

    # When GPU is the same, should prioritize minimizing fragmentation
    # of CPUs over customer resources
    assert "node2" == scheduler._best_fit_node(
        required_resources=Resources(GPU=1, CPU=1, customx=0.1),
        available_resources={
            "node1": Resources(GPU=10, CPU=5, customx=0.1),
            "node2": Resources(GPU=10, CPU=2, customx=10),
        },
    )

    # Custom resource prioritization: customx is more important than customy
    with mock.patch(
        "ray.serve._private.deployment_scheduler.RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES",
        "customx,customy",
    ):
        original = Resources.CUSTOM_PRIORITY
        Resources.CUSTOM_PRIORITY = ["customx", "customy"]

        assert "node2" == scheduler._best_fit_node(
            required_resources=Resources(customx=1, customy=1),
            available_resources={
                "node1": Resources(customx=2, customy=5),
                "node2": Resources(customx=2, customy=1),
            },
        )

        # If customx and customy are equal, GPU should determine best fit
        assert "node2" == scheduler._best_fit_node(
            required_resources=Resources(customx=1, customy=1, GPU=1),
            available_resources={
                "node1": Resources(customx=2, customy=2, GPU=10),
                "node2": Resources(customx=2, customy=2, GPU=2),
            },
        )

        # restore
        Resources.CUSTOM_PRIORITY = original


def test_schedule_replica():
    """Test DeploymentScheduler._schedule_replica()"""

    d_id = DeploymentID("deployment1", "app1")
    cluster_node_info_cache = MockClusterNodeInfoCache()
    scheduler = default_impl.create_deployment_scheduler(
        cluster_node_info_cache,
        head_node_id_override="fake-head-node-id",
        create_placement_group_fn_override=lambda request: MockPlacementGroup(request),
    )

    scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
    scheduler.on_deployment_deployed(d_id, rconfig(ray_actor_options={"num_cpus": 1}))

    scheduling_strategy = None

    def set_scheduling_strategy(actor_handle, placement_group):
        nonlocal scheduling_strategy
        scheduling_strategy = actor_handle._options["scheduling_strategy"]

    # Placement group without target node id
    r0_id = ReplicaID(unique_id="r0", deployment_id=d_id)
    scheduling_request = ReplicaSchedulingRequest(
        replica_id=r0_id,
        actor_def=MockActorClass(),
        actor_resources={"CPU": 1},
        placement_group_bundles=[{"CPU": 1}, {"CPU": 1}],
        placement_group_strategy="STRICT_PACK",
        actor_options={"name": "r0"},
        actor_init_args=(),
        on_scheduled=set_scheduling_strategy,
    )
    scheduler._pending_replicas[d_id][r0_id] = scheduling_request
    scheduler._schedule_replica(
        scheduling_request=scheduling_request,
        default_scheduling_strategy="some_default",
        target_node_id=None,
        target_labels={"abc": In("xyz")},
    )
    assert isinstance(scheduling_strategy, PlacementGroupSchedulingStrategy)
    assert len(scheduler._launching_replicas[d_id]) == 1
    assert not scheduler._launching_replicas[d_id][r0_id].target_labels
    assert not scheduler._launching_replicas[d_id][r0_id].target_node_id

    # Placement group with target node id
    r1_id = ReplicaID(unique_id="r1", deployment_id=d_id)
    scheduling_request = ReplicaSchedulingRequest(
        replica_id=r1_id,
        actor_def=MockActorClass(),
        actor_resources={"CPU": 1},
        placement_group_bundles=[{"CPU": 1}, {"CPU": 1}],
        placement_group_strategy="STRICT_PACK",
        actor_options={"name": "r1"},
        actor_init_args=(),
        on_scheduled=set_scheduling_strategy,
    )
    scheduler._pending_replicas[d_id][r1_id] = scheduling_request
    node_id_1 = NodeID.from_random().hex()
    scheduler._schedule_replica(
        scheduling_request=scheduling_request,
        default_scheduling_strategy="some_default",
        target_node_id=node_id_1,
        target_labels={"abc": In("xyz")},  # this should get ignored
    )
    assert isinstance(scheduling_strategy, PlacementGroupSchedulingStrategy)
    assert len(scheduler._launching_replicas[d_id]) == 2
    assert not scheduler._launching_replicas[d_id][r1_id].target_labels
    assert scheduler._launching_replicas[d_id][r1_id].target_node_id == node_id_1

    # Target node id without placement group
    r2_id = ReplicaID(unique_id="r2", deployment_id=d_id)
    scheduling_request = ReplicaSchedulingRequest(
        replica_id=r2_id,
        actor_def=MockActorClass(),
        actor_resources={"CPU": 1},
        actor_options={"name": "r2"},
        actor_init_args=(),
        on_scheduled=set_scheduling_strategy,
    )
    scheduler._pending_replicas[d_id][r2_id] = scheduling_request
    scheduler._schedule_replica(
        scheduling_request=scheduling_request,
        default_scheduling_strategy="some_default",
        target_node_id=node_id_1,
        target_labels={"abc": In("xyz")},  # this should get ignored
    )
    assert isinstance(scheduling_strategy, NodeAffinitySchedulingStrategy)
    assert scheduling_strategy.node_id == node_id_1
    assert len(scheduler._launching_replicas[d_id]) == 3
    assert not scheduler._launching_replicas[d_id][r2_id].target_labels
    assert scheduler._launching_replicas[d_id][r2_id].target_node_id == node_id_1

    # Target labels
    r3_id = ReplicaID(unique_id="r3", deployment_id=d_id)
    scheduling_request = ReplicaSchedulingRequest(
        replica_id=r3_id,
        actor_def=MockActorClass(),
        actor_resources={"CPU": 1},
        actor_options={"name": "r3"},
        actor_init_args=(),
        on_scheduled=set_scheduling_strategy,
    )
    scheduler._pending_replicas[d_id][r3_id] = scheduling_request
    scheduler._schedule_replica(
        scheduling_request=scheduling_request,
        default_scheduling_strategy="some_default",
        target_node_id=None,
        target_labels={"abc": In("xyz")},
    )
    assert isinstance(scheduling_strategy, NodeLabelSchedulingStrategy)
    assert scheduling_strategy.soft
    assert len(scheduler._launching_replicas[d_id]) == 4
    assert not scheduler._launching_replicas[d_id][r3_id].target_node_id
    assert len(scheduler._launching_replicas[d_id][r3_id].target_labels.keys()) == 1
    operator = scheduler._launching_replicas[d_id][r3_id].target_labels["abc"]
    assert isinstance(operator, In) and operator.values == ["xyz"]

    # internal implicit resource with max_replicas_per_node
    r4_id = ReplicaID(unique_id="r4", deployment_id=d_id)
    scheduling_request = ReplicaSchedulingRequest(
        replica_id=r4_id,
        actor_def=MockActorClass(),
        actor_resources={"my_rs": 1, "CPU": 1},
        placement_group_bundles=None,
        placement_group_strategy=None,
        actor_options={"name": "r4", "num_cpus": 1, "resources": {"my_rs": 1}},
        actor_init_args=(),
        on_scheduled=set_scheduling_strategy,
        max_replicas_per_node=10,
    )
    scheduler._pending_replicas[d_id][r4_id] = scheduling_request
    scheduler._schedule_replica(
        scheduling_request=scheduling_request,
        default_scheduling_strategy="some_default",
        target_node_id=None,
        target_labels=None,
    )
    assert scheduling_strategy == "some_default"
    assert len(scheduler._launching_replicas[d_id]) == 5
    assert scheduling_request.actor_options == {
        "name": "r4",
        "num_cpus": 1,
        "resources": {"my_rs": 1},
    }


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
        node_id_1 = NodeID.from_random().hex()
        node_id_2 = NodeID.from_random().hex()

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node(node_id_1, {"CPU": 3})
        cluster_node_info_cache.add_node(node_id_2, {"CPU": 2})
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
            assert scheduling_strategy.node_id == node_id_2

        assert len(on_scheduled_mock2.call_args_list) == 1
        call = on_scheduled_mock2.call_args_list[0]
        assert call.kwargs == {"placement_group": None}
        assert len(call.args) == 1
        scheduling_strategy = call.args[0]._options["scheduling_strategy"]
        assert isinstance(scheduling_strategy, NodeAffinitySchedulingStrategy)
        assert scheduling_strategy.node_id == node_id_1

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

        _ = ray.util.placement_group
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
        node_id_1 = NodeID.from_random().hex()
        node_id_2 = NodeID.from_random().hex()

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node(node_id_1, {"GPU": 4, "CPU": 6})
        cluster_node_info_cache.add_node(node_id_2, {"GPU": 10, "CPU": 2})
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
            assert scheduling_strategy.node_id == node_id_1
            assert call.kwargs == {"placement_group": None}

    def test_max_replicas_per_node(self):
        """Test that at most `max_replicas_per_node` number of replicas
        are scheduled onto a node even if that node has more resources.
        """

        d_id1 = DeploymentID(name="deployment1")
        node_id_1 = NodeID.from_random().hex()
        node_id_2 = NodeID.from_random().hex()
        cluster_node_info_cache = MockClusterNodeInfoCache()
        # Should try to schedule on node1 to minimize fragmentation
        cluster_node_info_cache.add_node(node_id_1, {"CPU": 20})
        cluster_node_info_cache.add_node(node_id_2, {"CPU": 21})

        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="fake-head-node-id",
            create_placement_group_fn_override=lambda *args, **kwargs: MockPlacementGroup(  # noqa
                *args, **kwargs
            ),
        )
        scheduler.on_deployment_created(d_id1, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_deployed(
            d_id1,
            ReplicaConfig.create(
                dummy, max_replicas_per_node=4, ray_actor_options={"num_cpus": 2}
            ),
        )

        state = defaultdict(int)

        def on_scheduled(actor_handle, placement_group):
            scheduling_strategy = actor_handle._options["scheduling_strategy"]
            if isinstance(scheduling_strategy, NodeAffinitySchedulingStrategy):
                state[scheduling_strategy.node_id] += 1
            elif isinstance(scheduling_strategy, PlacementGroupSchedulingStrategy):
                state[placement_group._soft_target_node_id] += 1

        scheduler.schedule(
            upscales={
                d_id1: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(
                            unique_id=f"replica{i}", deployment_id=d_id1
                        ),
                        actor_def=MockActorClass(),
                        actor_resources={"CPU": 2},
                        max_replicas_per_node=4,
                        actor_options={"name": "random"},
                        actor_init_args=(),
                        on_scheduled=on_scheduled,
                    )
                    for i in range(5)
                ]
            },
            downscales={},
        )
        assert state[node_id_1] == 4
        assert state[node_id_2] == 1

    def test_custom_resources(self):
        d_id = DeploymentID(name="deployment1")
        node_id_1 = NodeID.from_random().hex()
        node_id_2 = NodeID.from_random().hex()
        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node(node_id_1, {"CPU": 3})
        cluster_node_info_cache.add_node(node_id_2, {"CPU": 100, "customA": 1})

        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="fake-head-node-id",
            create_placement_group_fn_override=lambda *args, **kwargs: MockPlacementGroup(  # noqa
                *args, **kwargs
            ),
        )
        scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_deployed(
            d_id,
            ReplicaConfig.create(
                dummy, ray_actor_options={"num_cpus": 2, "resources": {"customA": 0.1}}
            ),
        )

        # Despite trying to schedule on node that minimizes fragmentation,
        # should respect custom resources and schedule onto node2
        def on_scheduled(actor_handle, placement_group):
            scheduling_strategy = actor_handle._options["scheduling_strategy"]
            assert isinstance(scheduling_strategy, NodeAffinitySchedulingStrategy)
            assert scheduling_strategy.node_id == node_id_2

        scheduler.schedule(
            upscales={
                d_id: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(unique_id="r0", deployment_id=d_id),
                        actor_def=MockActorClass(),
                        actor_resources={"CPU": 2, "customA": 0.1},
                        actor_options={"name": "random"},
                        actor_init_args=(),
                        on_scheduled=on_scheduled,
                    )
                ]
            },
            downscales={},
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
