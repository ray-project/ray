import sys

import pytest

import ray
from ray._raylet import GcsClient
from ray.anyscale._private.constants import ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL
from ray.anyscale.serve._private.deployment_scheduler import AnyscaleDeploymentScheduler
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.constants import RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY
from ray.serve._private.default_impl import create_cluster_node_info_cache
from ray.serve._private.deployment_scheduler import (
    DeploymentDownscaleRequest,
    ReplicaSchedulingRequest,
    SpreadDeploymentSchedulingPolicy,
)
from ray.tests.conftest import *  # noqa


@ray.remote(num_cpus=1)
class Replica:
    def get_node_id(self):
        return ray.get_runtime_context().get_node_id()

    def get_placement_group(self):
        return ray.util.get_current_placement_group()


@pytest.mark.skipif(
    RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY, reason="Needs spread strategy."
)
class TestSpreadScheduling:
    def test_upscale_multi_az(self, ray_start_cluster):
        """Test to make sure replicas are spreaded across az."""
        cluster = ray_start_cluster
        cluster.add_node(
            num_cpus=1,
            labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-1"},
        )
        cluster.add_node(
            num_cpus=2,
            labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-1"},
        )
        cluster.add_node(
            num_cpus=1,
            labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-2"},
        )
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)

        cluster_node_info_cache = create_cluster_node_info_cache(
            GcsClient(address=ray.get_runtime_context().gcs_address)
        )
        cluster_node_info_cache.update()

        scheduler = AnyscaleDeploymentScheduler(cluster_node_info_cache)
        dep_id = DeploymentID("deployment1", "my_app")
        scheduler.on_deployment_created(dep_id, SpreadDeploymentSchedulingPolicy())

        replica_actor_handles = []

        def on_scheduled(actor_handle, placement_group):
            replica_actor_handles.append(actor_handle)

        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={
                dep_id: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(
                            unique_id="replica1", deployment_id=dep_id
                        ),
                        actor_def=Replica,
                        actor_resources={"CPU": 1},
                        actor_options={},
                        actor_init_args=(),
                        on_scheduled=on_scheduled,
                    ),
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(
                            unique_id="replica2", deployment_id=dep_id
                        ),
                        actor_def=Replica,
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
        assert len(replica_actor_handles) == 2
        assert not scheduler._pending_replicas[dep_id]
        assert len(scheduler._launching_replicas[dep_id]) == 2
        assert {
            cluster_node_info_cache.get_node_az(
                ray.get(replica_actor_handles[0].get_node_id.remote())
            ),
            cluster_node_info_cache.get_node_az(
                ray.get(replica_actor_handles[1].get_node_id.remote())
            ),
        } == {"az-1", "az-2"}

        # Make sure az-aware spread scheduling is soft
        # and we can still schedule replicas even when nodes
        # are imbalanced across az.
        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={
                dep_id: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(
                            unique_id="replica3", deployment_id=dep_id
                        ),
                        actor_def=Replica,
                        actor_resources={"CPU": 1},
                        actor_options={},
                        actor_init_args=(),
                        on_scheduled=on_scheduled,
                    ),
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(
                            unique_id="replica4", deployment_id=dep_id
                        ),
                        actor_def=Replica,
                        actor_resources={"CPU": 1},
                        actor_options={},
                        actor_init_args=(),
                        on_scheduled=on_scheduled,
                    ),
                ]
            },
            downscales={},
        )
        assert len(replica_actor_handles) == 4
        assert not scheduler._pending_replicas[dep_id]
        assert len(scheduler._launching_replicas[dep_id]) == 4
        assert {
            cluster_node_info_cache.get_node_az(
                ray.get(replica_actor_handles[2].get_node_id.remote())
            ),
            cluster_node_info_cache.get_node_az(
                ray.get(replica_actor_handles[3].get_node_id.remote())
            ),
        } == {"az-1"}

        scheduler.on_replica_stopping(
            ReplicaID(unique_id="replica1", deployment_id=dep_id)
        )
        scheduler.on_replica_stopping(
            ReplicaID(unique_id="replica2", deployment_id=dep_id)
        )
        scheduler.on_replica_stopping(
            ReplicaID(unique_id="replica3", deployment_id=dep_id)
        )
        scheduler.on_replica_stopping(
            ReplicaID(unique_id="replica4", deployment_id=dep_id)
        )
        scheduler.on_deployment_deleted(dep_id)

    @pytest.mark.parametrize(
        "placement_group_config",
        [
            {},
            {"bundles": [{"CPU": 3}]},
            {
                "bundles": [{"CPU": 1}, {"CPU": 1}, {"CPU": 1}],
                "strategy": "STRICT_PACK",
            },
        ],
    )
    def test_upscale_no_az(self, ray_start_cluster, placement_group_config):
        """Test to make sure replicas are spreaded."""
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=3)
        cluster.add_node(num_cpus=3)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)

        cluster_node_info_cache = create_cluster_node_info_cache(
            GcsClient(address=ray.get_runtime_context().gcs_address)
        )
        cluster_node_info_cache.update()

        scheduler = AnyscaleDeploymentScheduler(cluster_node_info_cache)
        dep_id = DeploymentID("deployment1", "default")
        scheduler.on_deployment_created(dep_id, SpreadDeploymentSchedulingPolicy())
        replica_actor_handles = []
        replica_placement_groups = []

        def on_scheduled(actor_handle, placement_group):
            replica_actor_handles.append(actor_handle)
            replica_placement_groups.append(placement_group)

        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={
                dep_id: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(
                            unique_id="replica1", deployment_id=dep_id
                        ),
                        actor_def=Replica,
                        actor_resources={"CPU": 1},
                        actor_options={"name": "deployment1_replica1"},
                        actor_init_args=(),
                        on_scheduled=on_scheduled,
                        placement_group_bundles=placement_group_config.get(
                            "bundles", None
                        ),
                        placement_group_strategy=placement_group_config.get(
                            "strategy", None
                        ),
                    ),
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(
                            unique_id="replica2", deployment_id=dep_id
                        ),
                        actor_def=Replica,
                        actor_resources={"CPU": 1},
                        actor_options={"name": "deployment1_replica2"},
                        actor_init_args=(),
                        on_scheduled=on_scheduled,
                        placement_group_bundles=placement_group_config.get(
                            "bundles", None
                        ),
                        placement_group_strategy=placement_group_config.get(
                            "strategy", None
                        ),
                    ),
                ]
            },
            downscales={},
        )
        assert not deployment_to_replicas_to_stop
        assert len(replica_actor_handles) == 2
        assert len(replica_placement_groups) == 2
        assert not scheduler._pending_replicas[dep_id]
        assert len(scheduler._launching_replicas[dep_id]) == 2
        assert (
            len(
                {
                    ray.get(replica_actor_handles[0].get_node_id.remote()),
                    ray.get(replica_actor_handles[1].get_node_id.remote()),
                }
            )
            == 2
        )
        if "bundles" in placement_group_config:
            assert (
                len(
                    {
                        ray.get(replica_actor_handles[0].get_placement_group.remote()),
                        ray.get(replica_actor_handles[1].get_placement_group.remote()),
                    }
                )
                == 2
            )
        scheduler.on_replica_stopping(
            ReplicaID(unique_id="replica1", deployment_id=dep_id)
        )
        scheduler.on_replica_stopping(
            ReplicaID(unique_id="replica2", deployment_id=dep_id)
        )
        scheduler.on_deployment_deleted(dep_id)

    def test_downscale_single_deployment(self, ray_start_cluster):
        """Test to make sure downscale prefers replicas without node id
        and then replicas on a node with fewest replicas of all
        deployments, and then replicas on a node in an AZ with most
        replicas of the target deployment.
        """
        cluster = ray_start_cluster
        cluster.add_node(
            num_cpus=3,
            resources={"head": 1},
            labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-1"},
        )
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        # worker nodes
        cluster.add_node(
            num_cpus=3,
            resources={"worker1": 1},
            labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-1"},
        )
        cluster.add_node(
            num_cpus=3,
            resources={"worker2": 1},
            labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-1"},
        )
        cluster.add_node(
            num_cpus=3,
            resources={"worker3": 1},
            labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-2"},
        )
        cluster.add_node(
            num_cpus=3,
            resources={"worker4": 1},
            labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-2"},
        )
        cluster.wait_for_nodes()

        @ray.remote
        def get_node_id():
            return ray.get_runtime_context().get_node_id()

        worker1_node_id = ray.get(
            get_node_id.options(resources={"worker1": 1}).remote()
        )
        worker2_node_id = ray.get(
            get_node_id.options(resources={"worker2": 1}).remote()
        )
        worker3_node_id = ray.get(
            get_node_id.options(resources={"worker3": 1}).remote()
        )
        worker4_node_id = ray.get(
            get_node_id.options(resources={"worker4": 1}).remote()
        )

        cluster_node_info_cache = create_cluster_node_info_cache(
            GcsClient(address=ray.get_runtime_context().gcs_address)
        )
        cluster_node_info_cache.update()

        scheduler = AnyscaleDeploymentScheduler(cluster_node_info_cache)
        dep_id = DeploymentID("deployment1", "my_app")
        scheduler.on_deployment_created(dep_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_replica_running(ReplicaID("replica1", dep_id), worker1_node_id)
        scheduler.on_replica_running(ReplicaID("replica2", dep_id), worker1_node_id)
        scheduler.on_replica_running(ReplicaID("replica3", dep_id), worker2_node_id)
        scheduler.on_replica_running(ReplicaID("replica4", dep_id), worker2_node_id)
        scheduler.on_replica_running(ReplicaID("replica5", dep_id), worker3_node_id)
        scheduler.on_replica_running(ReplicaID("replica6", dep_id), worker3_node_id)
        scheduler.on_replica_running(ReplicaID("replica7", dep_id), worker4_node_id)
        scheduler.on_replica_recovering(ReplicaID("replica8", dep_id))

        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={},
            downscales={
                dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
            },
        )
        assert len(deployment_to_replicas_to_stop) == 1
        # Prefer replica without node id
        assert deployment_to_replicas_to_stop[dep_id] == {ReplicaID("replica8", dep_id)}
        scheduler.on_replica_stopping(ReplicaID("replica8", dep_id))

        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={
                dep_id: [
                    ReplicaSchedulingRequest(
                        replica_id=ReplicaID(
                            unique_id="replica9", deployment_id=dep_id
                        ),
                        actor_def=Replica,
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
        assert deployment_to_replicas_to_stop[dep_id] == {ReplicaID("replica9", dep_id)}
        scheduler.on_replica_stopping(ReplicaID("replica9", dep_id))

        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={},
            downscales={
                dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
            },
        )
        assert len(deployment_to_replicas_to_stop) == 1
        # Prefer replica on a node with fewest replicas of all deployments
        # even though the belonging AZ has fewer replicas in total.
        assert deployment_to_replicas_to_stop[dep_id] == {ReplicaID("replica7", dep_id)}
        scheduler.on_replica_stopping(ReplicaID("replica7", dep_id))

        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={},
            downscales={
                dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=2)
            },
        )
        assert len(deployment_to_replicas_to_stop) == 1
        # Prefer replicas on a node in an AZ with most replicas of the
        # target deployment.
        assert deployment_to_replicas_to_stop[dep_id] in [
            {ReplicaID("replica1", dep_id), ReplicaID("replica2", dep_id)},
            {ReplicaID("replica3", dep_id), ReplicaID("replica4", dep_id)},
        ]

    def test_downscale_multiple_deployments(self, ray_start_cluster):
        """Test to make sure downscale prefers replicas without node id
        and then replicas on a node with fewest replicas of all deployments.
        """
        cluster = ray_start_cluster
        cluster.add_node(
            num_cpus=3,
            resources={"head": 1},
            labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-1"},
        )
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        # worker nodes
        cluster.add_node(
            num_cpus=3,
            resources={"worker1": 1},
            labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-1"},
        )
        cluster.add_node(
            num_cpus=3,
            resources={"worker2": 1},
            labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-2"},
        )
        cluster.wait_for_nodes()

        @ray.remote
        def get_node_id():
            return ray.get_runtime_context().get_node_id()

        worker1_node_id = ray.get(
            get_node_id.options(resources={"worker1": 1}).remote()
        )
        worker2_node_id = ray.get(
            get_node_id.options(resources={"worker2": 1}).remote()
        )

        cluster_node_info_cache = create_cluster_node_info_cache(
            GcsClient(address=ray.get_runtime_context().gcs_address)
        )
        cluster_node_info_cache.update()

        scheduler = AnyscaleDeploymentScheduler(cluster_node_info_cache)
        d1_id = DeploymentID("deployment1", "default")
        d2_id = DeploymentID("deployment2", "default")
        scheduler.on_deployment_created(d1_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_deployment_created(d2_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_replica_running(ReplicaID("replica1", d1_id), worker1_node_id)
        scheduler.on_replica_running(ReplicaID("replica2", d1_id), worker2_node_id)
        scheduler.on_replica_running(ReplicaID("replica3", d1_id), worker2_node_id)
        scheduler.on_replica_running(ReplicaID("replica1", d2_id), worker1_node_id)
        scheduler.on_replica_running(ReplicaID("replica2", d2_id), worker2_node_id)
        scheduler.on_replica_running(ReplicaID("replica3", d2_id), worker1_node_id)
        scheduler.on_replica_running(ReplicaID("replica3", d2_id), worker1_node_id)
        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={},
            downscales={
                d1_id: DeploymentDownscaleRequest(deployment_id=d1_id, num_to_stop=1)
            },
        )
        assert len(deployment_to_replicas_to_stop) == 1
        # Even though worker1 has fewest replicas of deployment1
        # but it has more replicas of all deployments so
        # we should stop replicas from worker2.
        assert len(deployment_to_replicas_to_stop[d1_id]) == 1
        assert deployment_to_replicas_to_stop[d1_id] < {
            ReplicaID("replica2", d1_id),
            ReplicaID("replica3", d1_id),
        }

        scheduler.on_replica_stopping(ReplicaID("replica3", d1_id))
        scheduler.on_replica_stopping(ReplicaID("replica3", d2_id))
        scheduler.on_replica_stopping(ReplicaID("replica4", d2_id))

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

        scheduler.on_replica_stopping(ReplicaID("replica1", d1_id))
        scheduler.on_replica_stopping(ReplicaID("replica2", d1_id))
        scheduler.on_replica_stopping(ReplicaID("replica1", d2_id))
        scheduler.on_replica_stopping(ReplicaID("replica2", d2_id))
        scheduler.on_deployment_deleted(d1_id)
        scheduler.on_deployment_deleted(d2_id)

    def test_downscale_head_node(self, ray_start_cluster):
        """Test to make sure downscale deprioritizes replicas on the head node."""
        cluster = ray_start_cluster
        cluster.add_node(
            num_cpus=3,
            resources={"head": 1},
            labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-1"},
        )
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        cluster.add_node(
            num_cpus=3,
            resources={"worker1": 1},
            labels={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: "az-2"},
        )
        cluster.wait_for_nodes()

        @ray.remote
        def get_node_id():
            return ray.get_runtime_context().get_node_id()

        head_node_id = ray.get(get_node_id.options(resources={"head": 1}).remote())
        worker1_node_id = ray.get(
            get_node_id.options(resources={"worker1": 1}).remote()
        )

        cluster_node_info_cache = create_cluster_node_info_cache(
            GcsClient(address=ray.get_runtime_context().gcs_address)
        )
        cluster_node_info_cache.update()

        scheduler = AnyscaleDeploymentScheduler(cluster_node_info_cache)
        dep_id = DeploymentID("deployment1", "my_app")
        scheduler.on_deployment_created(dep_id, SpreadDeploymentSchedulingPolicy())
        scheduler.on_replica_running(ReplicaID("replica1", dep_id), head_node_id)
        scheduler.on_replica_running(ReplicaID("replica2", dep_id), worker1_node_id)
        scheduler.on_replica_running(ReplicaID("replica3", dep_id), worker1_node_id)
        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={},
            downscales={
                dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
            },
        )
        assert len(deployment_to_replicas_to_stop) == 1
        assert deployment_to_replicas_to_stop[dep_id] < {
            ReplicaID("replica2", dep_id),
            ReplicaID("replica3", dep_id),
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
            ReplicaID("replica2", dep_id),
            ReplicaID("replica3", dep_id),
        }
        scheduler.on_replica_stopping(deployment_to_replicas_to_stop[dep_id].pop())

        deployment_to_replicas_to_stop = scheduler.schedule(
            upscales={},
            downscales={
                dep_id: DeploymentDownscaleRequest(deployment_id=dep_id, num_to_stop=1)
            },
        )
        assert len(deployment_to_replicas_to_stop) == 1
        assert deployment_to_replicas_to_stop[dep_id] == {ReplicaID("replica1", dep_id)}
        scheduler.on_replica_stopping(ReplicaID("replica1", dep_id))
        scheduler.on_deployment_deleted(dep_id)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
