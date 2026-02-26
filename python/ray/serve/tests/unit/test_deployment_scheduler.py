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
from ray.serve._private.common import (
    CreatePlacementGroupRequest,
    DeploymentID,
    DeploymentStatus,
    GangPlacementGroupRequest,
    GangReservationResult,
    ReplicaID,
    ReplicaState,
)
from ray.serve._private.config import ReplicaConfig
from ray.serve._private.constants import (
    RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY,
)
from ray.serve._private.deployment_scheduler import (
    DeploymentDownscaleRequest,
    DeploymentSchedulingInfo,
    ReplicaSchedulingRequest,
    ReplicaSchedulingRequestStatus,
    Resources,
    SpreadDeploymentSchedulingPolicy,
)
from ray.serve._private.deployment_state import DeploymentStateManager
from ray.serve._private.test_utils import (
    MockActorClass,
    MockClusterNodeInfoCache,
    MockPlacementGroup,
)
from ray.serve.config import GangSchedulingConfig
from ray.serve.tests.unit.test_deployment_state import (
    check_counts,
    deployment_info,
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


def test_schedule_gang_replica():
    """Test DeploymentScheduler._schedule_replica() gang placement group path."""
    d_id = DeploymentID("deployment1", "app1")
    cluster_node_info_cache = MockClusterNodeInfoCache()
    mock_create_pg = Mock(
        side_effect=RuntimeError("Should not create placement group for gang request")
    )
    scheduler = default_impl.create_deployment_scheduler(
        cluster_node_info_cache,
        head_node_id_override="fake-head-node-id",
        create_placement_group_fn_override=mock_create_pg,
    )

    scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
    scheduler.on_deployment_deployed(d_id, rconfig(ray_actor_options={"num_cpus": 1}))

    scheduling_strategy = None

    def set_scheduling_strategy(actor_handle, placement_group):
        nonlocal scheduling_strategy
        scheduling_strategy = actor_handle._options["scheduling_strategy"]

    replica_id = ReplicaID(unique_id="r_gang", deployment_id=d_id)
    reserved_gang_pg = Mock()
    scheduling_request = ReplicaSchedulingRequest(
        replica_id=replica_id,
        actor_def=MockActorClass(),
        actor_resources={"CPU": 1},
        actor_options={"name": "r_gang"},
        actor_init_args=(),
        on_scheduled=set_scheduling_strategy,
        gang_placement_group=reserved_gang_pg,
        gang_pg_index=3,
    )
    scheduler._pending_replicas[d_id][replica_id] = scheduling_request

    scheduler._schedule_replica(
        scheduling_request=scheduling_request,
        default_scheduling_strategy="some_default",
        target_node_id=NodeID.from_random().hex(),  # should get ignored
        target_labels={"abc": In("xyz")},  # should get ignored
    )

    assert isinstance(scheduling_strategy, PlacementGroupSchedulingStrategy)
    assert scheduling_strategy.placement_group == reserved_gang_pg
    assert scheduling_strategy.placement_group_bundle_index == 3
    assert len(scheduler._launching_replicas[d_id]) == 1
    assert not scheduler._launching_replicas[d_id][replica_id].target_labels
    assert not scheduler._launching_replicas[d_id][replica_id].target_node_id
    mock_create_pg.assert_not_called()


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
    assert deployment_to_replicas_to_stop[d1_id].issubset({d1_r2_id, d1_r3_id})

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
    assert deployment_to_replicas_to_stop[dep_id].issubset({r2_id, r3_id})
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
    assert deployment_to_replicas_to_stop[dep_id] <= {r1_id, r2_id}
    scheduler.on_replica_stopping(r1_id)
    scheduler.on_replica_stopping(r2_id)
    scheduler.on_deployment_deleted(dep_id)


def test_schedule_passes_placement_group_options():
    """Test that bundle_label_selector is passed to CreatePlacementGroupRequest."""
    cluster_node_info_cache = MockClusterNodeInfoCache()
    captured_requests = []

    def mock_create_pg(request):
        captured_requests.append(request)

        class MockPG:
            def wait(self, *args):
                return True

        return MockPG()

    scheduler = default_impl.create_deployment_scheduler(
        cluster_node_info_cache,
        head_node_id_override="fake-head-node-id",
        create_placement_group_fn_override=mock_create_pg,
    )

    dep_id = DeploymentID(name="pg_options_test")
    # Use Spread policy here, but the logic is shared across policies.
    scheduler.on_deployment_created(dep_id, SpreadDeploymentSchedulingPolicy())

    test_labels = [{"region": "us-west"}]
    # Create a request with the new options
    req = ReplicaSchedulingRequest(
        replica_id=ReplicaID("r1", dep_id),
        actor_def=MockActorClass(),
        actor_resources={"CPU": 1},
        actor_options={"name": "r1"},
        actor_init_args=(),
        on_scheduled=lambda *args, **kwargs: None,
        placement_group_bundles=[{"CPU": 1}],
        placement_group_bundle_label_selector=test_labels,
        placement_group_strategy="STRICT_PACK",
    )

    scheduler.schedule(upscales={dep_id: [req]}, downscales={})

    # Verify the PlacementGroupSchedulingRequest is created.
    assert len(captured_requests) == 1
    pg_request = captured_requests[0]

    # bundle_label_selector should be passed to request.
    assert pg_request.bundle_label_selector == test_labels


def test_filter_nodes_by_label_selector():
    """Test _filter_nodes_by_label_selector logic used by _find_best_fit_node_for_pack
    when bin-packing, such that label constraints are enforced for the preferred node."""

    class MockScheduler(default_impl.DefaultDeploymentScheduler):
        def __init__(self):
            pass

    scheduler = MockScheduler()

    nodes = {
        "n1": Resources(),
        "n2": Resources(),
        "n3": Resources(),
    }
    node_labels = {
        "n1": {"region": "us-west", "gpu": "T4", "env": "prod"},
        "n2": {"region": "us-east", "gpu": "A100", "env": "dev"},
        "n3": {"region": "me-central", "env": "staging"},  # No GPU label
    }

    # equals operator
    filtered = scheduler._filter_nodes_by_label_selector(
        nodes, {"region": "us-west"}, node_labels
    )
    assert set(filtered.keys()) == {"n1"}

    # not equals operator
    filtered = scheduler._filter_nodes_by_label_selector(
        nodes, {"region": "!us-west"}, node_labels
    )
    assert set(filtered.keys()) == {"n2", "n3"}

    # in operator
    filtered = scheduler._filter_nodes_by_label_selector(
        nodes, {"region": "in(us-west,us-east)"}, node_labels
    )
    assert set(filtered.keys()) == {"n1", "n2"}

    # !in operator
    filtered = scheduler._filter_nodes_by_label_selector(
        nodes, {"env": "!in(dev,staging)"}, node_labels
    )
    assert set(filtered.keys()) == {"n1"}

    # Missing labels treated as not a match for equality.
    filtered = scheduler._filter_nodes_by_label_selector(
        nodes, {"gpu": "A100"}, node_labels
    )
    assert set(filtered.keys()) == {"n2"}

    # Not equal should match node with missing labels.
    filtered = scheduler._filter_nodes_by_label_selector(
        nodes, {"gpu": "!T4"}, node_labels
    )
    assert set(filtered.keys()) == {"n2", "n3"}


def test_build_pack_placement_candidates():
    """Test strategy generation logic in DefaultDeploymentScheduler._build_pack_placement_candidates,
    verifying that the scheduler correctly generates a list of (resources, labels) tuples to
    attempt for scheduling."""

    # Setup scheduler with mocks
    cluster_node_info_cache = MockClusterNodeInfoCache()
    scheduler = default_impl.create_deployment_scheduler(
        cluster_node_info_cache,
        head_node_id_override="head_node",
        create_placement_group_fn_override=None,
    )

    # Basic Ray Actor
    req_basic = ReplicaSchedulingRequest(
        replica_id=ReplicaID("r1", DeploymentID(name="d1")),
        actor_def=MockActorClass(),
        actor_resources={"CPU": 1},
        actor_options={},
        actor_init_args=(),
        on_scheduled=Mock(),
    )
    strategies = scheduler._build_pack_placement_candidates(req_basic)
    assert len(strategies) == 1
    assert strategies[0][0] == {"CPU": 1}
    assert strategies[0][1] == []

    # Actor with label_selector and fallback_strategy
    req_fallback = ReplicaSchedulingRequest(
        replica_id=ReplicaID("r2", DeploymentID(name="d1")),
        actor_def=MockActorClass(),
        actor_resources={"CPU": 1},
        actor_options={
            "label_selector": {"region": "us-west"},
            "fallback_strategy": [{"label_selector": {"region": "us-east"}}],
        },
        actor_init_args=(),
        on_scheduled=Mock(),
    )
    strategies = scheduler._build_pack_placement_candidates(req_fallback)
    assert len(strategies) == 2

    assert strategies[0][0] == {"CPU": 1}
    assert strategies[0][1] == [{"region": "us-west"}]
    assert strategies[1][0] == {"CPU": 1}
    assert strategies[1][1] == [{"region": "us-east"}]

    # Scheduling replica with placement group PACK strategy and bundle_label_selector
    req_pack = ReplicaSchedulingRequest(
        replica_id=ReplicaID("r4", DeploymentID(name="d1")),
        actor_def=MockActorClass(),
        actor_resources={"CPU": 0.1},
        actor_options={},
        actor_init_args=(),
        on_scheduled=Mock(),
        placement_group_bundles=[{"CPU": 5}],
        placement_group_strategy="PACK",
        placement_group_bundle_label_selector=[
            {"accelerator-type": "H100"},
            {"accelerator-type": "H100"},
        ],
    )

    with pytest.raises(NotImplementedError):
        scheduler._build_pack_placement_candidates(req_pack)

    # Scheduling replica with placement group STRICT_PACK strategy and bundle_label_selector
    req_pg = ReplicaSchedulingRequest(
        replica_id=ReplicaID("r3", DeploymentID(name="d1")),
        actor_def=MockActorClass(),
        actor_resources={},
        actor_options={},
        actor_init_args=(),
        on_scheduled=Mock(),
        placement_group_bundles=[{"CPU": 2}],
        placement_group_strategy="STRICT_PACK",
        placement_group_bundle_label_selector=[{"accelerator-type": "A100"}],
    )
    strategies = scheduler._build_pack_placement_candidates(req_pg)
    assert len(strategies) == 1

    assert strategies[0][0] == {"CPU": 2}
    assert strategies[0][1] == [{"accelerator-type": "A100"}]


def test_build_pack_placement_candidates_pg_fallback_error():
    """
    Test that providing placement_group_fallback_strategy raises NotImplementedError.
    """
    cluster_node_info_cache = MockClusterNodeInfoCache()
    scheduler = default_impl.create_deployment_scheduler(
        cluster_node_info_cache,
        head_node_id_override="head_node",
        create_placement_group_fn_override=None,
    )

    # Create a request with placement_group_fallback_strategy defined.
    req = ReplicaSchedulingRequest(
        replica_id=ReplicaID("r1", DeploymentID(name="d1")),
        actor_def=MockActorClass(),
        actor_resources={},
        actor_options={},
        actor_init_args=(),
        on_scheduled=Mock(),
        placement_group_bundles=[{"CPU": 1}],
        placement_group_strategy="STRICT_PACK",
        # Raises NotImplementedError since not added to placement group options yet.
        placement_group_fallback_strategy=[{"label_selector": {"zone": "us-east-1a"}}],
    )

    # Verify the scheduler raises the expected error
    with pytest.raises(NotImplementedError, match="not yet supported"):
        scheduler._build_pack_placement_candidates(req)


@pytest.mark.skipif(
    not RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY, reason="Needs pack strategy."
)
class TestPackScheduling:
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

    def test_actor_creation_failure_does_not_decrement_resources(self):
        """When actor creation fails for a replica, available resources
        should not be decremented so subsequent replicas in the same
        scheduling batch can still use that node.
        """

        d_id = DeploymentID(name="deployment1")
        node_id = NodeID.from_random().hex()

        cluster_node_info_cache = MockClusterNodeInfoCache()
        # Node has exactly 1 CPU — enough for one 1-CPU replica.
        cluster_node_info_cache.add_node(node_id, {"CPU": 1})

        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="fake-head-node-id",
            create_placement_group_fn_override=None,
        )
        scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_deployed(
            d_id,
            ReplicaConfig.create(dummy, ray_actor_options={"num_cpus": 1}),
        )

        # Create a mock actor class whose .options().remote() raises on the
        # first call (simulating actor creation failure) but succeeds after.
        call_count = 0

        class FailOnceMockActorClass(MockActorClass):
            def remote(self, *args):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise RuntimeError("Simulated actor creation failure")
                return super().remote(*args)

        on_scheduled_mock = Mock()
        r0_id = ReplicaID(unique_id="r0", deployment_id=d_id)
        r1_id = ReplicaID(unique_id="r1", deployment_id=d_id)

        req0 = ReplicaSchedulingRequest(
            replica_id=r0_id,
            actor_def=FailOnceMockActorClass(),
            actor_resources={"CPU": 1},
            actor_options={},
            actor_init_args=(),
            on_scheduled=on_scheduled_mock,
        )
        req1 = ReplicaSchedulingRequest(
            replica_id=r1_id,
            actor_def=MockActorClass(),
            actor_resources={"CPU": 1},
            actor_options={},
            actor_init_args=(),
            on_scheduled=on_scheduled_mock,
        )

        scheduler.schedule(
            upscales={d_id: [req0, req1]},
            downscales={},
        )

        # The first replica should have failed.
        assert req0.status == ReplicaSchedulingRequestStatus.ACTOR_CREATION_FAILED

        # The second replica should have succeeded and been scheduled to the
        # node.
        assert req1.status == ReplicaSchedulingRequestStatus.SUCCEEDED
        assert on_scheduled_mock.call_count == 1
        call = on_scheduled_mock.call_args_list[0]
        scheduling_strategy = call.args[0]._options["scheduling_strategy"]
        assert isinstance(scheduling_strategy, NodeAffinitySchedulingStrategy)
        assert scheduling_strategy.node_id == node_id

    def test_pg_creation_failure_does_not_decrement_resources(self):
        """When placement group creation fails for a replica, available
        resources should not be decremented so subsequent replicas in the
        same scheduling batch can still use that node.
        """

        d_id = DeploymentID(name="deployment1")
        node_id = NodeID.from_random().hex()

        cluster_node_info_cache = MockClusterNodeInfoCache()
        # Node has exactly 1 CPU — enough for one replica with 1-CPU PG.
        cluster_node_info_cache.add_node(node_id, {"CPU": 1})

        call_count = 0

        def fail_once_create_pg(request):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Simulated PG creation failure")
            return MockPlacementGroup(request)

        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="fake-head-node-id",
            create_placement_group_fn_override=fail_once_create_pg,
        )
        scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_deployed(
            d_id,
            ReplicaConfig.create(
                dummy,
                ray_actor_options={"num_cpus": 0},
                placement_group_bundles=[{"CPU": 1}],
                placement_group_strategy="STRICT_PACK",
            ),
        )

        on_scheduled_mock = Mock()
        r0_id = ReplicaID(unique_id="r0", deployment_id=d_id)
        r1_id = ReplicaID(unique_id="r1", deployment_id=d_id)

        req0 = ReplicaSchedulingRequest(
            replica_id=r0_id,
            actor_def=MockActorClass(),
            actor_resources={"CPU": 0},
            placement_group_bundles=[{"CPU": 1}],
            placement_group_strategy="STRICT_PACK",
            actor_options={"name": "r0"},
            actor_init_args=(),
            on_scheduled=on_scheduled_mock,
        )
        req1 = ReplicaSchedulingRequest(
            replica_id=r1_id,
            actor_def=MockActorClass(),
            actor_resources={"CPU": 0},
            placement_group_bundles=[{"CPU": 1}],
            placement_group_strategy="STRICT_PACK",
            actor_options={"name": "r1"},
            actor_init_args=(),
            on_scheduled=on_scheduled_mock,
        )

        scheduler.schedule(
            upscales={d_id: [req0, req1]},
            downscales={},
        )

        # The first replica should have failed at PG creation.
        assert (
            req0.status
            == ReplicaSchedulingRequestStatus.PLACEMENT_GROUP_CREATION_FAILED
        )

        # The second replica should still succeed.
        assert req1.status == ReplicaSchedulingRequestStatus.SUCCEEDED
        assert on_scheduled_mock.call_count == 1
        call = on_scheduled_mock.call_args_list[0]
        scheduling_strategy = call.args[0]._options["scheduling_strategy"]
        assert isinstance(scheduling_strategy, PlacementGroupSchedulingStrategy)

    def test_pack_prefers_newly_non_idle_node(self):
        """After scheduling a replica to a previously idle node, subsequent
        replicas in the same batch should prefer that node (now non-idle)
        over other idle nodes, even if the idle node is a tighter fit.

        Regression test: without updating node_to_running_replicas after
        each scheduling, the PACK scheduler would treat all initially-idle
        nodes as idle for the entire batch, falling through to pure
        best-fit and potentially spreading replicas across nodes.
        """

        d_id1 = DeploymentID(name="deployment1")
        d_id2 = DeploymentID(name="deployment2")
        node_id_1 = NodeID.from_random().hex()
        node_id_2 = NodeID.from_random().hex()

        cluster_node_info_cache = MockClusterNodeInfoCache()
        # Node 1 has GPU + CPU; node 2 has only CPU.
        # After the GPU replica is placed on node 1, node 2 would be
        # a tighter best-fit for a CPU-only replica (2 CPU remaining
        # vs 4 CPU on node 1). But PACK should prefer node 1 because
        # it is now non-idle.
        cluster_node_info_cache.add_node(node_id_1, {"GPU": 1, "CPU": 4})
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
            ReplicaConfig.create(
                dummy, ray_actor_options={"num_gpus": 1, "num_cpus": 0}
            ),
        )
        scheduler.on_deployment_deployed(
            d_id2,
            ReplicaConfig.create(dummy, ray_actor_options={"num_cpus": 1}),
        )

        on_scheduled_mock1 = Mock()
        on_scheduled_mock2 = Mock()
        scheduler.schedule(
            upscales={
                d_id1: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(unique_id="r0", deployment_id=d_id1),
                        actor_def=MockActorClass(),
                        actor_resources={"GPU": 1},
                        actor_options={},
                        actor_init_args=(),
                        on_scheduled=on_scheduled_mock1,
                    )
                ],
                d_id2: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(unique_id="r1", deployment_id=d_id2),
                        actor_def=MockActorClass(),
                        actor_resources={"CPU": 1},
                        actor_options={},
                        actor_init_args=(),
                        on_scheduled=on_scheduled_mock2,
                    )
                ],
            },
            downscales={},
        )

        # The GPU replica must go to node 1 (only node with GPU).
        assert len(on_scheduled_mock1.call_args_list) == 1
        call1 = on_scheduled_mock1.call_args_list[0]
        strategy1 = call1.args[0]._options["scheduling_strategy"]
        assert isinstance(strategy1, NodeAffinitySchedulingStrategy)
        assert strategy1.node_id == node_id_1
        assert call1.kwargs == {"placement_group": None}

        # The CPU replica should also go to node 1 (now non-idle) rather
        # than node 2 (idle but tighter fit). The PACK scheduler prefers
        # non-idle nodes to consolidate replicas onto fewer nodes.
        assert len(on_scheduled_mock2.call_args_list) == 1
        call2 = on_scheduled_mock2.call_args_list[0]
        strategy2 = call2.args[0]._options["scheduling_strategy"]
        assert isinstance(strategy2, NodeAffinitySchedulingStrategy)
        assert strategy2.node_id == node_id_1
        assert call2.kwargs == {"placement_group": None}


class TestGangDeploymentStates:
    def test_schedule_gang_placement_groups(self, mock_deployment_state_manager):
        """Creates gangs successfully and verifies placement requests include expected bundles and strategy."""
        captured_requests = []
        gang_size = 2
        num_gangs = 2
        num_replicas_to_add = gang_size * num_gangs
        replica_resource_dict = {"CPU": 2.0, "GPU": 1.0}
        gang_strategy = "SPREAD"

        def create_pg_fn(request: CreatePlacementGroupRequest, *args, **kwargs):
            captured_requests.append(request)
            return Mock()

        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm(
            create_placement_group_fn_override=create_pg_fn,
        )
        scheduler = dsm._deployment_scheduler
        deployment_id = DeploymentID(name="d1", app_name="app1")
        gang_request = GangPlacementGroupRequest(
            deployment_id,
            gang_size,
            gang_strategy,
            num_replicas_to_add,
            replica_resource_dict=replica_resource_dict,
        )

        result = scheduler.schedule_gang_placement_groups({deployment_id: gang_request})

        assert deployment_id in result
        assert result[deployment_id].success
        assert len(result[deployment_id].gang_pgs) == num_gangs
        assert len(captured_requests) == num_gangs
        for req in captured_requests:
            assert isinstance(req, CreatePlacementGroupRequest)
            assert req.bundles == [replica_resource_dict] * gang_size
            assert req.strategy == gang_strategy

    def test_schedule_gang_placement_groups_invalid_gang_size(
        self, mock_deployment_state_manager
    ):
        """Returns failure when desired replicas cannot be evenly divided by gang size."""
        gang_size = 3
        num_replicas_to_add = 4
        create_pg_fn = Mock()
        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm(
            create_placement_group_fn_override=create_pg_fn,
        )
        scheduler = dsm._deployment_scheduler
        deployment_id = DeploymentID(name="d2", app_name="app2")
        gang_request = GangPlacementGroupRequest(
            deployment_id,
            gang_size,
            "STRICT_PACK",
            num_replicas_to_add,
            {"CPU": 1.0},
        )

        result = scheduler.schedule_gang_placement_groups({deployment_id: gang_request})

        assert not result[deployment_id].success
        assert "not divisible by gang_size" in result[deployment_id].error_message
        create_pg_fn.assert_not_called()

    def test_schedule_gang_placement_groups_all_pg_creation_failures(
        self, mock_deployment_state_manager
    ):
        """Reports failure when every gang placement group creation attempt raises exceptions."""
        gang_size = 2
        num_gangs = 2
        num_replicas_to_add = gang_size * num_gangs

        def create_pg_fn(request: CreatePlacementGroupRequest, *args, **kwargs):
            raise RuntimeError("simulated placement group creation failure")

        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm(
            create_placement_group_fn_override=create_pg_fn,
        )
        scheduler = dsm._deployment_scheduler
        deployment_id = DeploymentID(name="d3", app_name="app3")
        gang_request = GangPlacementGroupRequest(
            deployment_id,
            gang_size,
            "STRICT_PACK",
            num_replicas_to_add,
            {"CPU": 1.0},
        )

        result = scheduler.schedule_gang_placement_groups({deployment_id: gang_request})

        assert not result[deployment_id].success
        assert (
            "Failed to create any gang placement groups"
            in result[deployment_id].error_message
        )

    def test_schedule_gang_placement_groups_partial_pg_creation_failures(
        self, mock_deployment_state_manager
    ):
        """Keeps successful gang reservations when only a subset of placement groups fail."""
        gang_size = 2
        num_gangs = 2
        num_replicas_to_add = gang_size * num_gangs
        failed_gangs = 1
        num_calls = 0

        def create_pg_fn(request: CreatePlacementGroupRequest, *args, **kwargs):
            nonlocal num_calls
            num_calls += 1
            if num_calls == 1:
                raise RuntimeError("fail first gang only")
            return Mock()

        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm(
            create_placement_group_fn_override=create_pg_fn,
        )
        scheduler = dsm._deployment_scheduler
        deployment_id = DeploymentID(name="d4", app_name="app4")
        gang_request = GangPlacementGroupRequest(
            deployment_id,
            gang_size,
            "STRICT_PACK",
            num_replicas_to_add,
            {"CPU": 1.0},
        )

        result = scheduler.schedule_gang_placement_groups({deployment_id: gang_request})

        assert result[deployment_id].success
        assert len(result[deployment_id].gang_pgs) == num_gangs - failed_gangs

    def test_schedule_gang_placement_groups_with_per_replica_bundles(
        self, mock_deployment_state_manager
    ):
        """Flattens per-replica bundles and propagates label selectors and fallback strategies correctly."""
        captured_requests = []
        gang_size = 2
        num_gangs = 4
        num_replicas_to_add = num_gangs * gang_size

        def create_pg_fn(request: CreatePlacementGroupRequest, *args, **kwargs):
            captured_requests.append(request)
            return Mock()

        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm(
            create_placement_group_fn_override=create_pg_fn,
        )
        scheduler = dsm._deployment_scheduler
        deployment_id = DeploymentID(name="d5", app_name="app5")
        per_replica_bundles = [{"GPU": 1.0, "CPU": 1.0}, {"CPU": 1.0}]
        per_replica_label_selector = [{"gpu": "a100"}, {"zone": "z1"}]
        per_replica_fallback = [{"allow_soft": True}, {"allow_soft": False}]
        gang_request = GangPlacementGroupRequest(
            deployment_id,
            gang_size,
            "STRICT_PACK",
            num_replicas_to_add,
            replica_resource_dict={"CPU": 1.0},
            replica_placement_group_bundles=per_replica_bundles,
            replica_pg_bundle_label_selector=per_replica_label_selector,
            replica_pg_fallback_strategy=per_replica_fallback,
        )

        result = scheduler.schedule_gang_placement_groups({deployment_id: gang_request})

        assert result[deployment_id].success
        assert len(captured_requests) == num_gangs
        expected_bundles = per_replica_bundles * gang_size
        expected_label_selector = per_replica_label_selector * gang_size
        expected_fallback = per_replica_fallback * gang_size
        for req in captured_requests:
            assert req.bundles == expected_bundles
            assert req.bundle_label_selector == expected_label_selector
            assert req.fallback_strategy == expected_fallback

    def test_schedule_gang_placement_groups_without_per_replica_bundles_uses_resource_dict(
        self, mock_deployment_state_manager
    ):
        """Uses replica resource dict for each gang bundle without optional selectors."""
        captured_requests = []
        gang_size = 3
        num_gangs = 2
        num_replicas_to_add = gang_size * num_gangs
        replica_resource_dict = {"CPU": 2.0, "GPU": 0.5}

        def create_pg_fn(request: CreatePlacementGroupRequest, *args, **kwargs):
            captured_requests.append(request)
            return Mock()

        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm(
            create_placement_group_fn_override=create_pg_fn,
        )
        scheduler = dsm._deployment_scheduler
        deployment_id = DeploymentID(name="d6", app_name="app6")
        gang_request = GangPlacementGroupRequest(
            deployment_id,
            gang_size,
            "STRICT_PACK",
            num_replicas_to_add,
            replica_resource_dict=replica_resource_dict,
        )

        result = scheduler.schedule_gang_placement_groups({deployment_id: gang_request})

        assert result[deployment_id].success
        assert len(captured_requests) == num_gangs
        for req in captured_requests:
            assert req.bundles == [replica_resource_dict] * gang_size
            assert req.bundle_label_selector is None
            assert req.fallback_strategy is None

    def test_schedule_gang_placement_groups_multiple_deployments(
        self, mock_deployment_state_manager
    ):
        """Schedules gang placement groups for multiple deployments and returns independent results."""
        create_pg_fn = Mock(return_value=Mock())
        gang_size_1 = 2
        num_gangs_1 = 2
        num_replicas_to_add_1 = gang_size_1 * num_gangs_1
        gang_size_2 = 3
        num_gangs_2 = 2
        num_replicas_to_add_2 = gang_size_2 * num_gangs_2
        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm(
            create_placement_group_fn_override=create_pg_fn,
        )
        scheduler = dsm._deployment_scheduler
        deployment_id_1 = DeploymentID(name="d7", app_name="app7")
        deployment_id_2 = DeploymentID(name="d8", app_name="app8")
        gang_requests = {
            deployment_id_1: GangPlacementGroupRequest(
                deployment_id_1,
                gang_size_1,
                "STRICT_PACK",
                num_replicas_to_add_1,
                {"CPU": 1.0},
            ),
            deployment_id_2: GangPlacementGroupRequest(
                deployment_id_2,
                gang_size_2,
                "STRICT_PACK",
                num_replicas_to_add_2,
                {"CPU": 1.0},
            ),
        }

        result = scheduler.schedule_gang_placement_groups(gang_requests)

        assert set(result.keys()) == {deployment_id_1, deployment_id_2}
        assert result[deployment_id_1].success
        assert result[deployment_id_2].success
        assert len(result[deployment_id_1].gang_pgs) == num_gangs_1
        assert len(result[deployment_id_2].gang_pgs) == num_gangs_2
        assert create_pg_fn.call_count == num_gangs_1 + num_gangs_2


class TestScaleDeploymentGangReplicas:
    def test_stopping_replicas_skip_upscale(self, mock_deployment_state_manager):
        """Skips upscale while gang replicas are stopping after startup failures."""
        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm(
            create_placement_group_fn_override=lambda *args, **kwargs: Mock(),
        )
        gang_size = 2
        target_replicas = 2
        deployment_id = DeploymentID(name="gang_stopping_skip", app_name="app")

        info, version = deployment_info(
            num_replicas=target_replicas,
            version="v1",
            gang_scheduling_config=GangSchedulingConfig(gang_size=gang_size),
        )
        dsm.deploy(deployment_id, info)
        ds = dsm._deployment_states[deployment_id]

        dsm.update()
        check_counts(
            ds, total=target_replicas, by_state=[(ReplicaState.STARTING, 2, version)]
        )

        for replica in ds._replicas.get([ReplicaState.STARTING]):
            replica._actor.set_failed_to_start()

        dsm._deployment_scheduler.schedule_gang_placement_groups = Mock(return_value={})
        captured_upscales = {}
        original_schedule = dsm._deployment_scheduler.schedule

        def schedule_with_capture(upscales, downscales):
            captured_upscales.update(upscales)
            return original_schedule(upscales, downscales)

        dsm._deployment_scheduler.schedule = Mock(side_effect=schedule_with_capture)
        dsm.update()

        assert captured_upscales == {}
        dsm._deployment_scheduler.schedule_gang_placement_groups.assert_not_called()
        check_counts(
            ds, total=target_replicas, by_state=[(ReplicaState.STOPPING, 2, version)]
        )
        assert ds.curr_status_info.status == DeploymentStatus.UPDATING

    def test_gang_reservation_failure_records_startup_failure(
        self, mock_deployment_state_manager
    ):
        """Keeps upscale empty and records reservation failure details in deployment status."""
        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()
        deployment_id = DeploymentID(name="gang_reservation_fail", app_name="app")
        error_msg = "simulated gang placement reservation failure"

        info, _ = deployment_info(
            num_replicas=4,
            gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        )
        dsm.deploy(deployment_id, info)
        ds = dsm._deployment_states[deployment_id]

        dsm._deployment_scheduler.schedule_gang_placement_groups = Mock(
            return_value={
                deployment_id: GangReservationResult(
                    success=False, error_message=error_msg
                )
            }
        )
        captured_upscales = {}
        original_schedule = dsm._deployment_scheduler.schedule

        def schedule_with_capture(upscales, downscales):
            captured_upscales.update(upscales)
            return original_schedule(upscales, downscales)

        dsm._deployment_scheduler.schedule = Mock(side_effect=schedule_with_capture)
        dsm.update()

        assert captured_upscales == {}
        check_counts(ds, total=0)
        assert ds.curr_status_info.status == DeploymentStatus.UPDATING
        assert "Gang scheduling failed" in ds.curr_status_info.message
        assert error_msg in ds.curr_status_info.message

    def test_successful_gang_reservation(self, mock_deployment_state_manager):
        """Creates expected gang scheduling requests and marks all replicas as starting."""
        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()
        gang_size = 2
        num_gangs = 2
        target_replicas = gang_size * num_gangs
        deployment_id = DeploymentID(name="gang_success_sched", app_name="app")
        gang_pgs = [Mock(name="pg-0"), Mock(name="pg-1")]

        info, version = deployment_info(
            num_replicas=target_replicas,
            version="v1",
            gang_scheduling_config=GangSchedulingConfig(gang_size=gang_size),
        )
        dsm.deploy(deployment_id, info)
        ds = dsm._deployment_states[deployment_id]

        dsm._deployment_scheduler.schedule_gang_placement_groups = Mock(
            return_value={
                deployment_id: GangReservationResult(success=True, gang_pgs=gang_pgs)
            }
        )

        captured_upscales = {}
        original_schedule = dsm._deployment_scheduler.schedule

        def schedule_with_capture(upscales, downscales):
            captured_upscales.update(upscales)
            return original_schedule(upscales, downscales)

        dsm._deployment_scheduler.schedule = Mock(side_effect=schedule_with_capture)
        dsm.update()

        assert deployment_id in captured_upscales
        scheduling_requests = captured_upscales[deployment_id]
        assert len(scheduling_requests) == target_replicas
        assert {r.gang_placement_group for r in scheduling_requests} == set(gang_pgs)
        assert sorted(r.gang_pg_index for r in scheduling_requests) == [0, 0, 1, 1]
        check_counts(
            ds,
            total=target_replicas,
            by_state=[(ReplicaState.STARTING, target_replicas, version)],
        )
        starting_replicas = ds._replicas.get([ReplicaState.STARTING])
        assert len(starting_replicas) == target_replicas
        gang_to_replicas = {}
        for replica in starting_replicas:
            gang_to_replicas.setdefault(replica.gang_context.gang_id, []).append(
                replica
            )

        assert len(gang_to_replicas) == num_gangs
        for gang_id, replicas in gang_to_replicas.items():
            assert len(replicas) == gang_size
            member_ids = {r.replica_id.unique_id for r in replicas}
            assert sorted(r.gang_context.rank for r in replicas) == list(
                range(gang_size)
            )
            for replica in replicas:
                gang_context = replica.gang_context
                assert gang_context.gang_id == gang_id
                assert gang_context.world_size == gang_size
                assert set(gang_context.member_replica_ids) == member_ids

    def test_gang_sibling_cleanup_on_startup_failure(
        self, mock_deployment_state_manager
    ):
        """Stops gang siblings when one member fails startup to avoid partial gangs."""
        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm(
            create_placement_group_fn_override=lambda *args, **kwargs: Mock(),
        )
        gang_size = 2
        target_replicas = 4
        deployment_id = DeploymentID(name="gang_sibling_cleanup", app_name="app")

        info, version = deployment_info(
            num_replicas=target_replicas,
            version="v1",
            gang_scheduling_config=GangSchedulingConfig(gang_size=gang_size),
        )
        dsm.deploy(deployment_id, info)
        ds = dsm._deployment_states[deployment_id]

        dsm.update()
        starting_replicas = ds._replicas.get([ReplicaState.STARTING])
        initial_context_by_replica = {
            r.replica_id.unique_id: (
                r.gang_context.gang_id,
                r.gang_context.rank,
                r.gang_context.world_size,
                tuple(r.gang_context.member_replica_ids),
            )
            for r in starting_replicas
        }
        gang_to_replicas = {}
        for replica in starting_replicas:
            gang_to_replicas.setdefault(replica.gang_context.gang_id, []).append(
                replica
            )
        failed_gang_id, failed_gang_members = next(iter(gang_to_replicas.items()))

        failed_gang_members[0]._actor.set_failed_to_start()
        failed_gang_members[1]._actor.set_ready()
        dsm.update()

        stopping_replicas = ds._replicas.get([ReplicaState.STOPPING])
        starting_replicas = ds._replicas.get([ReplicaState.STARTING])
        assert len(stopping_replicas) == gang_size
        assert all(r.gang_context.gang_id == failed_gang_id for r in stopping_replicas)
        assert all(r.gang_context.gang_id != failed_gang_id for r in starting_replicas)
        surviving_gang_ids = {r.gang_context.gang_id for r in starting_replicas}
        assert len(surviving_gang_ids) == 1
        for replica in starting_replicas:
            context_snapshot = initial_context_by_replica[replica.replica_id.unique_id]
            assert context_snapshot == (
                replica.gang_context.gang_id,
                replica.gang_context.rank,
                replica.gang_context.world_size,
                tuple(replica.gang_context.member_replica_ids),
            )
        check_counts(
            ds,
            total=target_replicas,
            by_state=[
                (ReplicaState.STOPPING, 2, version),
                (ReplicaState.STARTING, 2, version),
            ],
        )

    def test_terminally_failed_deployment_skips_gang_reservation(
        self, mock_deployment_state_manager
    ):
        """Does not reserve gang placement groups after deployment reaches terminal startup failure."""
        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()
        deployment_id = DeploymentID(name="gang_terminal_failure", app_name="app")
        info, _ = deployment_info(
            num_replicas=2,
            gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        )
        dsm.deploy(deployment_id, info)
        ds = dsm._deployment_states[deployment_id]

        dsm._deployment_scheduler.schedule_gang_placement_groups = Mock(
            return_value={
                deployment_id: GangReservationResult(
                    success=False, error_message="simulated gang reservation failure"
                )
            }
        )

        for _ in range(20):
            dsm.update()
            if ds.curr_status_info.status == DeploymentStatus.DEPLOY_FAILED:
                break
        assert ds.curr_status_info.status == DeploymentStatus.DEPLOY_FAILED

        dsm._deployment_scheduler.schedule_gang_placement_groups.reset_mock()
        dsm.update()
        dsm._deployment_scheduler.schedule_gang_placement_groups.assert_not_called()

    def test_healthy_after_starting_replicas_ready(self, mock_deployment_state_manager):
        """Transitions gang deployment to healthy once all starting replicas become ready."""
        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm(
            create_placement_group_fn_override=lambda *args, **kwargs: Mock(),
        )
        gang_size = 2
        target_replicas = 4
        deployment_id = DeploymentID(name="gang_healthy", app_name="app")
        info, version = deployment_info(
            num_replicas=target_replicas,
            version="v1",
            gang_scheduling_config=GangSchedulingConfig(gang_size=gang_size),
        )
        dsm.deploy(deployment_id, info)
        ds = dsm._deployment_states[deployment_id]

        dsm.update()
        check_counts(
            ds,
            total=target_replicas,
            by_state=[(ReplicaState.STARTING, target_replicas, version)],
        )

        for replica in ds._replicas.get([ReplicaState.STARTING]):
            replica._actor.set_ready()
        dsm.update()

        check_counts(
            ds,
            total=target_replicas,
            by_state=[(ReplicaState.RUNNING, target_replicas, version)],
        )
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
