import random
import sys
from copy import copy
from typing import Dict, List, Optional
from unittest.mock import patch

import pytest

from ray.serve._private import default_impl
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.config import ReplicaConfig
from ray.serve._private.constants import RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY
from ray.serve._private.deployment_scheduler import (
    Resources,
    SpreadDeploymentSchedulingPolicy,
)
from ray.serve._private.test_utils import MockClusterNodeInfoCache, MockTimer
from ray.tests.conftest import *  # noqa


def dummy():
    pass


def rconfig(**config_opts):
    return ReplicaConfig.create(dummy, **config_opts)


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
            # Should print second warning
            timer.advance(1000)
            for _ in range(10):
                opp = scheduler.get_node_to_compact(allow_new_compaction=False)

            # Advance a LOT of time. We should give up on the compaction
            # because of timeout.
            timer.advance(10000)
            opp = scheduler.get_node_to_compact(allow_new_compaction=False)
            assert opp is None
            assert scheduler._compacting_node is None

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
