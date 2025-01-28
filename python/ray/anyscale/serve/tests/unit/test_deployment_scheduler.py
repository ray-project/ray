import random
import sys
from collections import defaultdict
from functools import partial
from typing import Dict, List, Optional
from unittest.mock import patch

import pytest

from ray.anyscale._private.constants import ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL
from ray.serve._private import default_impl
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.config import ReplicaConfig
from ray.serve._private.constants import RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY
from ray.serve._private.deployment_scheduler import (
    DeploymentDownscaleRequest,
    ReplicaSchedulingRequest,
    Resources,
    SpreadDeploymentSchedulingPolicy,
)
from ray.serve._private.test_utils import MockActorClass
from ray.serve._private.test_utils import (
    MockClusterNodeInfoCache as DefaultMockClusterNodeInfoCache,
)
from ray.serve._private.test_utils import MockTimer
from ray.tests.conftest import *  # noqa
from ray.util.scheduling_strategies import NodeLabelSchedulingStrategy


def dummy():
    pass


def rconfig(**config_opts):
    return ReplicaConfig.create(dummy, **config_opts)


class MockClusterNodeInfoCache(DefaultMockClusterNodeInfoCache):
    def get_node_az(self, node_id) -> Optional[str]:
        if node_id not in self.node_labels:
            return None

        return self.node_labels[node_id].get(
            ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL, None
        )


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


# TODO(zcin): support az aware scheduling for compact scheduling strategy
@pytest.mark.skipif(
    RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY,
    reason="Not supported with compact scheduling yet.",
)
class TestAZScheduling:
    def test_upscale_spread_az(self):
        """Test replicas are spread across AZ."""

        d_id = DeploymentID("deployment1", "my_app")

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node(
            "node1", {"CPU": 5}, {ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az1"}
        )
        cluster_node_info_cache.add_node(
            "node2", {"CPU": 5}, {ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az2"}
        )
        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="fake-head-node-id",
            create_placement_group_fn_override=None,
        )

        # STATE: AZ1 -> 0, AZ2 -> 0
        scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_deployed(
            d_id, rconfig(ray_actor_options={"num_cpus": 1})
        )

        replica_azs = []

        def on_scheduled(actor_handle, placement_group):
            assert not placement_group
            scheduling_strategy = actor_handle._options["scheduling_strategy"]
            assert isinstance(scheduling_strategy, NodeLabelSchedulingStrategy)
            for label in scheduling_strategy.soft:
                if label.key == ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL:
                    az = label.operator.values[0]

            replica_azs.append(az)

        # STATE should become: AZ1 -> 2, AZ2 -> 0
        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={
                d_id: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(unique_id="r1", deployment_id=d_id),
                        actor_def=MockActorClass(),
                        actor_resources={"CPU": 1},
                        actor_options={},
                        actor_init_args=(),
                        on_scheduled=on_scheduled,
                    ),
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(unique_id="r2", deployment_id=d_id),
                        actor_def=MockActorClass(),
                        actor_resources={"CPU": 1},
                        actor_options={},
                        actor_init_args=(),
                        on_scheduled=on_scheduled,
                    ),
                ]
            },
            downscales={},
        )
        assert not deployment_to_replicas_to_stop
        assert len(replica_azs) == 2
        assert not scheduler._pending_replicas[d_id]
        assert len(scheduler._launching_replicas[d_id]) == 2
        assert set(replica_azs) == {"az1", "az2"}

        # Make sure running replicas and their AZs are picked up in
        # next call to schedule
        replica_azs.clear()
        scheduler.on_replica_running(ReplicaID("r1", d_id), "node1")
        scheduler.on_replica_running(ReplicaID("r2", d_id), "node1")
        # STATE should become: AZ1 -> 2, AZ2 -> 2
        assert not scheduler.schedule(
            upscales={
                d_id: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(unique_id="r3", deployment_id=d_id),
                        actor_def=MockActorClass(),
                        actor_resources={"CPU": 1},
                        actor_options={},
                        actor_init_args=(),
                        on_scheduled=on_scheduled,
                    ),
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(unique_id="r4", deployment_id=d_id),
                        actor_def=MockActorClass(),
                        actor_resources={"CPU": 1},
                        actor_options={},
                        actor_init_args=(),
                        on_scheduled=on_scheduled,
                    ),
                ]
            },
            downscales={},
        )
        # Launching 2 new replicas
        assert len(replica_azs) == 2
        assert not scheduler._pending_replicas[d_id]
        assert len(scheduler._launching_replicas[d_id]) == 2
        assert replica_azs == ["az2", "az2"]

        # Make sure running AND launching replicas (with their AZs) are
        # picked up in next call to schedule
        # STATE should become: AZ1 -> 2, AZ2 -> 3
        scheduler.on_replica_running(ReplicaID("r5", d_id), "node2")
        replica_azs.clear()
        # STATE should become: AZ1 -> 3, AZ2 -> 3
        assert not scheduler.schedule(
            upscales={
                d_id: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(unique_id="r6", deployment_id=d_id),
                        actor_def=MockActorClass(),
                        actor_resources={"CPU": 1},
                        actor_options={},
                        actor_init_args=(),
                        on_scheduled=on_scheduled,
                    ),
                ]
            },
            downscales={},
        )
        assert len(replica_azs) == 1
        assert not scheduler._pending_replicas[d_id]
        # Since we haven't marked r3, r4 as running, in total there are
        # 3 launching replicas (r3, r4, r6)
        assert len(scheduler._launching_replicas[d_id]) == 3
        # Since there's 2 running replicas (r1,r2) in AZ1, 2 launching
        # replicas (r3,r4) in AZ2, and 1 running replica (r5) in AZ2,
        # this new replica should target AZ1
        assert replica_azs == ["az1"]

    def test_upscale_multiple_deployments_spread_az(self):
        """Test replicas are spread across AZ when there are multiple deployments."""

        d1_id = DeploymentID("deployment1", "my_app")
        d2_id = DeploymentID("deployment2", "my_app")

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node(
            "node1", {"CPU": 2}, {ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az1"}
        )
        cluster_node_info_cache.add_node(
            "node2", {"CPU": 2}, {ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az2"}
        )
        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="fake-head-node-id",
            create_placement_group_fn_override=None,
        )

        scheduler.on_deployment_created(d1_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_created(d2_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_deployed(
            d1_id, rconfig(ray_actor_options={"num_cpus": 1})
        )
        scheduler.on_deployment_deployed(
            d2_id, rconfig(ray_actor_options={"num_cpus": 1})
        )

        replica_azs = defaultdict(list)

        def on_scheduled(deployment_id, actor_handle, placement_group):
            assert not placement_group
            scheduling_strategy = actor_handle._options["scheduling_strategy"]
            assert isinstance(scheduling_strategy, NodeLabelSchedulingStrategy)
            for label in scheduling_strategy.soft:
                if label.key == ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL:
                    az = label.operator.values[0]

            replica_azs[deployment_id].append(az)

        scheduler.on_replica_running(ReplicaID("r1", d1_id), "node1")
        scheduler.on_replica_running(ReplicaID("r2", d2_id), "node2")
        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={
                d1_id: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(unique_id="r3", deployment_id=d1_id),
                        actor_def=MockActorClass(),
                        actor_resources={"CPU": 1},
                        actor_options={},
                        actor_init_args=(),
                        on_scheduled=partial(on_scheduled, d1_id),
                    ),
                ],
                d2_id: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(unique_id="r4", deployment_id=d2_id),
                        actor_def=MockActorClass(),
                        actor_resources={"CPU": 1},
                        actor_options={},
                        actor_init_args=(),
                        on_scheduled=partial(on_scheduled, d2_id),
                    ),
                ],
            },
            downscales={},
        )
        assert not deployment_to_replicas_to_stop
        assert not scheduler._pending_replicas[d1_id]
        assert not scheduler._pending_replicas[d2_id]
        assert len(scheduler._launching_replicas[d1_id]) == 1
        assert len(scheduler._launching_replicas[d2_id]) == 1

        # We should try to spread across AZs
        assert replica_azs[d1_id] == ["az2"]
        assert replica_azs[d2_id] == ["az1"]

    def test_downscale_single_deployment(self):
        """Test to make sure downscale prefers replicas without node id
        and then replicas on a node with fewest replicas of all
        deployments, and then replicas on a node in an AZ with most
        replicas of the target deployment.
        """

        d_id = DeploymentID("deployment1", "my_app")

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node(
            "head",
            {"CPU": 3},
            {ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-1"},
        )
        # worker nodes
        cluster_node_info_cache.add_node(
            "node1",
            {"CPU": 3},
            {ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-1"},
        )
        cluster_node_info_cache.add_node(
            "node2",
            {"CPU": 3},
            {ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-1"},
        )
        cluster_node_info_cache.add_node(
            "node3",
            {"CPU": 3},
            {ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-2"},
        )
        cluster_node_info_cache.add_node(
            "node4",
            {"CPU": 3},
            {ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-2"},
        )

        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="head",
            create_placement_group_fn_override=None,
        )
        scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_deployed(
            d_id, rconfig(ray_actor_options={"num_cpus": 1})
        )
        scheduler.on_replica_running(ReplicaID("r1", d_id), "node1")
        scheduler.on_replica_running(ReplicaID("r2", d_id), "node1")
        scheduler.on_replica_running(ReplicaID("r3", d_id), "node2")
        scheduler.on_replica_running(ReplicaID("r4", d_id), "node2")
        scheduler.on_replica_running(ReplicaID("r5", d_id), "node3")
        scheduler.on_replica_running(ReplicaID("r6", d_id), "node3")
        scheduler.on_replica_running(ReplicaID("r7", d_id), "node4")
        scheduler.on_replica_recovering(ReplicaID("r8", d_id))

        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={},
            downscales={
                d_id: DeploymentDownscaleRequest(deployment_id=d_id, num_to_stop=1)
            },
        )
        assert len(deployment_to_replicas_to_stop) == 1
        # Prefer replica without node id
        assert deployment_to_replicas_to_stop[d_id] == {ReplicaID("r8", d_id)}
        scheduler.on_replica_stopping(ReplicaID("r8", d_id))

        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={
                d_id: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(unique_id="r9", deployment_id=d_id),
                        actor_def=MockActorClass(),
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
                d_id: DeploymentDownscaleRequest(deployment_id=d_id, num_to_stop=1)
            },
        )
        assert len(deployment_to_replicas_to_stop) == 1
        # Prefer replica without node id
        assert deployment_to_replicas_to_stop[d_id] == {ReplicaID("r9", d_id)}
        scheduler.on_replica_stopping(ReplicaID("r9", d_id))

        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={},
            downscales={
                d_id: DeploymentDownscaleRequest(deployment_id=d_id, num_to_stop=1)
            },
        )
        assert len(deployment_to_replicas_to_stop) == 1
        # Prefer replica on a node with fewest replicas of all deployments
        # even though the belonging AZ has fewer replicas in total.
        assert deployment_to_replicas_to_stop[d_id] == {ReplicaID("r7", d_id)}
        scheduler.on_replica_stopping(ReplicaID("r7", d_id))

        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={},
            downscales={
                d_id: DeploymentDownscaleRequest(deployment_id=d_id, num_to_stop=2)
            },
        )
        assert len(deployment_to_replicas_to_stop) == 1
        # Prefer replicas on a node in an AZ with most replicas of the
        # target deployment.
        assert deployment_to_replicas_to_stop[d_id] in [
            {ReplicaID("r1", d_id), ReplicaID("r2", d_id)},
            {ReplicaID("r3", d_id), ReplicaID("r4", d_id)},
        ]

    def test_downscale_multiple_deployments(self):
        """Test to make sure downscale prefers replicas without node id
        and then replicas on a node with fewest replicas of all deployments.
        """

        d1_id = DeploymentID("deployment1", "default")
        d2_id = DeploymentID("deployment2", "default")

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node(
            "head",
            {"CPU": 3},
            {ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-1"},
        )
        # worker nodes
        cluster_node_info_cache.add_node(
            "node1",
            {"CPU": 3},
            {ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-1"},
        )
        cluster_node_info_cache.add_node(
            "node2",
            {"CPU": 3},
            {ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-2"},
        )
        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="head",
            create_placement_group_fn_override=None,
        )
        scheduler.on_deployment_created(d1_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_created(d2_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_replica_running(ReplicaID("r1", d1_id), "node1")
        scheduler.on_replica_running(ReplicaID("r2", d1_id), "node2")
        scheduler.on_replica_running(ReplicaID("r3", d1_id), "node2")
        scheduler.on_replica_running(ReplicaID("r1", d2_id), "node1")
        scheduler.on_replica_running(ReplicaID("r2", d2_id), "node2")
        scheduler.on_replica_running(ReplicaID("r3", d2_id), "node1")
        scheduler.on_replica_running(ReplicaID("r3", d2_id), "node1")
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
        assert deployment_to_replicas_to_stop[d1_id] < {
            ReplicaID("r2", d1_id),
            ReplicaID("r3", d1_id),
        }

        scheduler.on_replica_stopping(ReplicaID("r3", d1_id))
        scheduler.on_replica_stopping(ReplicaID("r3", d2_id))
        scheduler.on_replica_stopping(ReplicaID("r4", d2_id))

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

        scheduler.on_replica_stopping(ReplicaID("r1", d1_id))
        scheduler.on_replica_stopping(ReplicaID("r2", d1_id))
        scheduler.on_replica_stopping(ReplicaID("r1", d2_id))
        scheduler.on_replica_stopping(ReplicaID("r2", d2_id))
        scheduler.on_deployment_deleted(d1_id)
        scheduler.on_deployment_deleted(d2_id)

    def test_downscale_deprioritize_head_node(self):
        """Test to make sure downscale deprioritizes replicas on the head node."""

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node(
            "head",
            {"CPU": 3},
            {ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-1"},
        )
        cluster_node_info_cache.add_node(
            "worker",
            {"CPU": 3},
            {ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-2"},
        )

        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="head",
            create_placement_group_fn_override=None,
        )
        dep_id = DeploymentID("deployment1", "my_app")
        scheduler.on_deployment_created(dep_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_replica_running(ReplicaID("r1", dep_id), "head")
        scheduler.on_replica_running(ReplicaID("r2", dep_id), "worker")
        scheduler.on_replica_running(ReplicaID("r3", dep_id), "worker")
        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={},
            downscales={
                dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
            },
        )
        assert len(deployment_to_replicas_to_stop) == 1
        assert deployment_to_replicas_to_stop[dep_id] < {
            ReplicaID("r2", dep_id),
            ReplicaID("r3", dep_id),
        }
        scheduler.on_replica_stopping(deployment_to_replicas_to_stop[dep_id].pop())

        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={},
            downscales={
                dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
            },
        )
        assert len(deployment_to_replicas_to_stop) == 1
        assert deployment_to_replicas_to_stop[dep_id] < {
            ReplicaID("r2", dep_id),
            ReplicaID("r3", dep_id),
        }
        scheduler.on_replica_stopping(deployment_to_replicas_to_stop[dep_id].pop())

        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={},
            downscales={
                dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
            },
        )
        assert len(deployment_to_replicas_to_stop) == 1
        assert deployment_to_replicas_to_stop[dep_id] == {ReplicaID("r1", dep_id)}
        scheduler.on_replica_stopping(ReplicaID("r1", dep_id))
        scheduler.on_deployment_deleted(dep_id)


@pytest.mark.skipif(
    not RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY, reason="Needs compact strategy."
)
class TestActiveCompaction:
    def test_basic(self):
        d_id1 = DeploymentID(name="deployment1")
        d_id2 = DeploymentID(name="deployment2")

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node("node1", {"GPU": 4, "CPU": 8})
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
            rconfig(ray_actor_options={"num_gpus": 1, "num_cpus": 2}),
        )
        scheduler.on_deployment_deployed(
            d_id2,
            rconfig(ray_actor_options={"num_gpus": 1, "num_cpus": 1}),
        )

        for i in range(3):
            scheduler.on_replica_running(
                ReplicaID(unique_id=f"replica{i}", deployment_id=d_id1), node_id="node1"
            )
        scheduler.on_replica_running(
            ReplicaID(unique_id="replica4", deployment_id=d_id2), node_id="node2"
        )

        node, deadline = scheduler.get_node_to_compact(allow_new_compaction=True)
        assert node == "node2"
        assert deadline == float("inf")
        # Again, should return the same thing
        node, deadline = scheduler.get_node_to_compact(allow_new_compaction=False)
        assert node == "node2"
        assert deadline == float("inf")

    def test_no_compaction_opportunity(self):
        """When none of the nodes can be compacted, `get_node_to_compact`
        should return None.
        """

        d_id1 = DeploymentID(name="deployment1")
        d_id2 = DeploymentID(name="deployment2")

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node("node1", {"GPU": 4, "CPU": 8})
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
            rconfig(ray_actor_options={"num_gpus": 1, "num_cpus": 2}),
        )
        scheduler.on_deployment_deployed(
            d_id2,
            rconfig(ray_actor_options={"num_gpus": 2, "num_cpus": 1}),
        )

        scheduler.on_replica_running(ReplicaID("replica0", d_id1), "node1")
        scheduler.on_replica_running(ReplicaID("replica1", d_id1), "node1")
        scheduler.on_replica_running(ReplicaID("replica2", d_id1), "node1")
        scheduler.on_replica_running(ReplicaID("replica3", d_id2), "node2")

        opp = scheduler.get_node_to_compact(allow_new_compaction=True)
        assert opp is None

    def test_idle_nodes(self):
        """Replicas should not be compacted/migrated to idle nodes."""

        d_id = DeploymentID(name="deployment1")

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node("node1", {"CPU": 3})
        cluster_node_info_cache.add_node("node2", {"CPU": 3})

        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="fake-head-node-id",
            create_placement_group_fn_override=None,
        )

        scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_deployed(
            d_id,
            rconfig(ray_actor_options={"num_cpus": 1}),
        )

        scheduler.on_replica_running(ReplicaID("replica0", d_id), "node1")
        opp = scheduler.get_node_to_compact(allow_new_compaction=True)
        assert opp is None

    def test_compaction_completes(self):
        """When all replicas have been stopped on the target node to be compacted,
        the active compaction should complete.
        """

        d_id = DeploymentID(name="deployment1")

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node("node1", {"CPU": 3})
        cluster_node_info_cache.add_node("node2", {"CPU": 2})
        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="fake-head-node-id",
            create_placement_group_fn_override=None,
        )

        scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_deployed(
            d_id, rconfig(ray_actor_options={"num_cpus": 1})
        )

        scheduler.on_replica_running(ReplicaID("replica0", d_id), "node1")
        scheduler.on_replica_running(ReplicaID("replica1", d_id), "node1")
        scheduler.on_replica_running(ReplicaID("replica2", d_id), "node2")
        opp = scheduler.get_node_to_compact(allow_new_compaction=True)
        assert opp[0] == "node2"
        # Again, should return the same thing
        opp = scheduler.get_node_to_compact(allow_new_compaction=False)
        assert opp[0] == "node2"

        scheduler.on_replica_running(ReplicaID("replica3", d_id), "node1")
        scheduler.on_replica_stopping(ReplicaID("replica2", d_id))

        # Compaction should be complete
        opp = scheduler.get_node_to_compact(allow_new_compaction=False)
        assert opp is None
        assert scheduler._compacting_node is None

    def test_compaction_cancelled(self):
        """When new replicas get scheduled on the target node being compacted,
        the current active compaction should be cancelled.
        """

        d_id = DeploymentID(name="deployment1")

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node("node1", {"CPU": 3})
        cluster_node_info_cache.add_node("node2", {"CPU": 2})
        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="fake-head-node-id",
            create_placement_group_fn_override=None,
        )

        scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_deployed(
            d_id, rconfig(ray_actor_options={"num_cpus": 1})
        )

        scheduler.on_replica_running(ReplicaID("replica0", d_id), "node1")
        scheduler.on_replica_running(ReplicaID("replica1", d_id), "node1")
        scheduler.on_replica_running(ReplicaID("replica2", d_id), "node2")
        opp = scheduler.get_node_to_compact(allow_new_compaction=True)
        assert opp[0] == "node2"
        # Again, should return the same thing
        opp = scheduler.get_node_to_compact(allow_new_compaction=False)
        assert opp[0] == "node2"

        # "Upscale" a replica on node2, compaction should be cancelled
        scheduler.on_replica_running(ReplicaID("replica3", d_id), "node2")
        opp = scheduler.get_node_to_compact(allow_new_compaction=True)
        assert opp is None
        assert scheduler._compacting_node is None

    def test_compaction_timeout(self):
        timer = MockTimer()
        with patch("time.time", new=timer.time):
            d_id = DeploymentID(name="deployment1")

            cluster_node_info_cache = MockClusterNodeInfoCache()
            cluster_node_info_cache.add_node("node1", {"CPU": 3})
            cluster_node_info_cache.add_node("node2", {"CPU": 2})
            scheduler = default_impl.create_deployment_scheduler(
                cluster_node_info_cache,
                head_node_id_override="fake-head-node-id",
                create_placement_group_fn_override=None,
            )

            scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
            scheduler.on_deployment_deployed(
                d_id, rconfig(ray_actor_options={"num_cpus": 1})
            )

            scheduler.on_replica_running(ReplicaID("replica0", d_id), "node1")
            scheduler.on_replica_running(ReplicaID("replica1", d_id), "node1")
            scheduler.on_replica_running(ReplicaID("replica2", d_id), "node2")
            opp = scheduler.get_node_to_compact(allow_new_compaction=True)
            assert opp[0] == "node2"
            # Again, should return the same thing
            opp = scheduler.get_node_to_compact(allow_new_compaction=False)
            assert opp[0] == "node2"

            # Should print first warning
            timer.advance(100)
            for _ in range(10):
                opp = scheduler.get_node_to_compact(allow_new_compaction=False)
                assert opp[0] == "node2"
            # Should print second warning
            timer.advance(1000)
            for _ in range(10):
                opp = scheduler.get_node_to_compact(allow_new_compaction=False)
                assert opp[0] == "node2"

            # Advance a LOT of time. We should give up on the compaction
            # because of timeout.
            timer.advance(10000)
            opp = scheduler.get_node_to_compact(allow_new_compaction=False)
            assert opp is None
            assert scheduler._compacting_node is None

    def test_exponential_backoff_cancellation(self):
        """Test exponential backoff due to repeated cancellations."""

        timer = MockTimer(0)
        with patch("time.time", new=timer.time):
            d_id = DeploymentID(name="deployment1")

            cluster_node_info_cache = MockClusterNodeInfoCache()
            cluster_node_info_cache.add_node("node1", {"CPU": 3})
            cluster_node_info_cache.add_node("node2", {"CPU": 2})
            scheduler = default_impl.create_deployment_scheduler(
                cluster_node_info_cache,
                head_node_id_override="fake-head-node-id",
                create_placement_group_fn_override=None,
            )

            scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
            scheduler.on_deployment_deployed(
                d_id, rconfig(ray_actor_options={"num_cpus": 1})
            )

            scheduler.on_replica_running(ReplicaID("r0", d_id), "node1")
            scheduler.on_replica_running(ReplicaID("r1", d_id), "node1")
            scheduler.on_replica_running(ReplicaID("r2", d_id), "node2")
            opp = scheduler.get_node_to_compact(allow_new_compaction=True)
            assert opp[0] == "node2"

            for i in range(5):
                # Compaction should be cancelled because of new replica
                scheduler.on_replica_running(ReplicaID("r3", d_id), "node2")
                opp = scheduler.get_node_to_compact(allow_new_compaction=True)
                assert opp is None
                scheduler.on_replica_stopping(ReplicaID("r3", d_id))

                # Exponential backoff
                expected_backoff = 2 ** (i + 1)
                timer.advance(expected_backoff / 2)
                opp = scheduler.get_node_to_compact(allow_new_compaction=True)
                assert opp is None
                timer.advance(expected_backoff / 2)
                opp = scheduler.get_node_to_compact(allow_new_compaction=True)
                assert opp[0] == "node2"

    def test_exponential_backoff_timeout(self):
        """Test exponential backoff due to continued timeout."""

        timer = MockTimer(0)
        with patch("time.time", new=timer.time):
            d_id = DeploymentID(name="deployment1")

            cluster_node_info_cache = MockClusterNodeInfoCache()
            cluster_node_info_cache.add_node("node1", {"CPU": 3})
            cluster_node_info_cache.add_node("node2", {"CPU": 2})
            scheduler = default_impl.create_deployment_scheduler(
                cluster_node_info_cache,
                head_node_id_override="fake-head-node-id",
                create_placement_group_fn_override=None,
            )

            scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
            scheduler.on_deployment_deployed(
                d_id, rconfig(ray_actor_options={"num_cpus": 1})
            )

            scheduler.on_replica_running(ReplicaID("r0", d_id), "node1")
            scheduler.on_replica_running(ReplicaID("r1", d_id), "node1")
            scheduler.on_replica_running(ReplicaID("r2", d_id), "node2")
            opp = scheduler.get_node_to_compact(allow_new_compaction=True)
            assert opp[0] == "node2"

            for i in range(5):
                # After 1000 seconds, compaction should still be happening
                timer.advance(1000)
                opp = scheduler.get_node_to_compact(allow_new_compaction=True)
                assert opp[0] == "node2"

                # At 1800 seconds, compaction times out / fails
                timer.advance(800)
                opp = scheduler.get_node_to_compact(allow_new_compaction=True)
                assert opp is None

                # Exponential backoff starts
                expected_backoff = 2 ** (i + 1)
                timer.advance(expected_backoff / 2)
                opp = scheduler.get_node_to_compact(allow_new_compaction=True)
                assert opp is None
                timer.advance(expected_backoff / 2)
                opp = scheduler.get_node_to_compact(allow_new_compaction=True)
                assert opp[0] == "node2"

    def test_head_node_not_considered(self):
        """The head node should not be considered as a compaction target."""

        d_id = DeploymentID(name="deployment1")

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node("head-node-id", {"CPU": 1})
        cluster_node_info_cache.add_node("worker-node-id", {"CPU": 3})

        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="head-node-id",
            create_placement_group_fn_override=None,
        )

        scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_deployed(
            d_id,
            rconfig(ray_actor_options={"num_cpus": 1}),
        )

        # It's possible for all replicas running on the head node to be
        # moved to other nodes, but there's no point in doing that so
        # we shouldn't take that compaction opportunity
        scheduler.on_replica_running(ReplicaID("replica0", d_id), "head-node-id")
        scheduler.on_replica_running(ReplicaID("replica1", d_id), "worker-node-id")
        opp = scheduler.get_node_to_compact(allow_new_compaction=True)
        assert opp is None

    def test_custom_resources(self):
        """Test active compaction with custom resources."""

        d1_id = DeploymentID(name="deployment1")
        d2_id = DeploymentID(name="deployment2")

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node("node1", {"CPU": 3})
        cluster_node_info_cache.add_node("node2", {"CPU": 1, "customx": 1})

        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="head-node-id",
            create_placement_group_fn_override=None,
        )

        scheduler.on_deployment_created(d1_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_created(d2_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_deployed(
            d1_id, rconfig(ray_actor_options={"num_cpus": 1})
        )
        scheduler.on_deployment_deployed(
            d2_id,
            rconfig(ray_actor_options={"num_cpus": 1, "resources": {"customx": 0.1}}),
        )

        # node1: 1CPU available
        # node2: one 1CPU-replica running
        scheduler.on_replica_running(ReplicaID("r1", d1_id), "node1")
        scheduler.on_replica_running(ReplicaID("r2", d1_id), "node1")
        scheduler.on_replica_running(ReplicaID("r3", d2_id), "node2")
        # However we shouldn't be able to compact node2 because the
        # replica on node2 requires resource `customx`
        opp = scheduler.get_node_to_compact(allow_new_compaction=True)
        assert opp is None

        # Add new node3 (identical to node1, except it does have
        # `customx` resource).
        cluster_node_info_cache.add_node("node3", {"CPU": 3, "customx": 1})
        scheduler.on_replica_running(ReplicaID("r4", d1_id), "node3")
        scheduler.on_replica_running(ReplicaID("r5", d1_id), "node3")
        # We should be able to compact node2 because the replica on
        # node2 can be scheduled on node3
        opp = scheduler.get_node_to_compact(allow_new_compaction=True)
        assert opp[0] == "node2"

    def test_migrate_to_multiple_nodes(self):
        """Test when the compaction target node has replicas whose
        migration needs to be spread across multiple other nodes in the
        cluster.
        """

        d_id = DeploymentID(name="deployment1")

        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node("node1", {"CPU": 6})
        cluster_node_info_cache.add_node("node2", {"CPU": 6})
        cluster_node_info_cache.add_node("node3", {"CPU": 3})
        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="fake-head-node-id",
            create_placement_group_fn_override=None,
        )

        scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_deployed(
            d_id, rconfig(ray_actor_options={"num_cpus": 1})
        )

        # 5 replicas on node1
        scheduler.on_replica_running(ReplicaID("replica0", d_id), "node1")
        scheduler.on_replica_running(ReplicaID("replica1", d_id), "node1")
        scheduler.on_replica_running(ReplicaID("replica2", d_id), "node1")
        scheduler.on_replica_running(ReplicaID("replica3", d_id), "node1")
        scheduler.on_replica_running(ReplicaID("replica4", d_id), "node1")
        # 4 replicas on node2
        scheduler.on_replica_running(ReplicaID("replica5", d_id), "node2")
        scheduler.on_replica_running(ReplicaID("replica6", d_id), "node2")
        scheduler.on_replica_running(ReplicaID("replica7", d_id), "node2")
        scheduler.on_replica_running(ReplicaID("replica8", d_id), "node2")
        # 3 replicas on node3
        scheduler.on_replica_running(ReplicaID("replica9", d_id), "node3")
        scheduler.on_replica_running(ReplicaID("replica10", d_id), "node3")
        scheduler.on_replica_running(ReplicaID("replica11", d_id), "node3")

        opp = scheduler.get_node_to_compact(allow_new_compaction=True)
        assert opp[0] == "node3"
        # Again, should return the same thing
        opp = scheduler.get_node_to_compact(allow_new_compaction=False)
        assert opp[0] == "node3"

    def test_prioritize_larger_nodes_to_compact(self):
        """We should always prefer compacting larger nodes (aka more
        resources) since it's assumed that those are more expensive.
        """

        d_id = DeploymentID(name="deployment1")

        cluster_node_info_cache = MockClusterNodeInfoCache()
        # Randomize
        if random.randint(0, 1) == 1:
            expected_node = "node2"
            cluster_node_info_cache.add_node("node1", {"GPU": 4, "CPU": 8})
            cluster_node_info_cache.add_node("node2", {"GPU": 10, "CPU": 2})
        else:
            expected_node = "node1"
            cluster_node_info_cache.add_node("node1", {"GPU": 10, "CPU": 2})
            cluster_node_info_cache.add_node("node2", {"GPU": 4, "CPU": 8})

        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override="fake-head-node-id",
            create_placement_group_fn_override=None,
        )

        scheduler.on_deployment_created(d_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_deployed(
            d_id, rconfig(ray_actor_options={"num_gpus": 1})
        )

        scheduler.on_replica_running(ReplicaID("r1", d_id), node_id="node1")
        scheduler.on_replica_running(ReplicaID("r2", d_id), node_id="node2")

        node, _ = scheduler.get_node_to_compact(allow_new_compaction=True)
        assert node == expected_node
        # Again, should return the same thing
        node, _ = scheduler.get_node_to_compact(allow_new_compaction=False)
        assert node == expected_node


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
