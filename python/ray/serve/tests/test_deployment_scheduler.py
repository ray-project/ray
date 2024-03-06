import sys

import pytest

import ray
from ray._raylet import GcsClient
from ray.serve._private.common import DeploymentID
from ray.serve._private.default_impl import create_cluster_node_info_cache
from ray.serve._private.deployment_scheduler import (
    DefaultDeploymentScheduler,
    ReplicaSchedulingRequest,
    SpreadDeploymentSchedulingPolicy,
)
from ray.serve._private.utils import get_head_node_id
from ray.tests.conftest import *  # noqa


@ray.remote(num_cpus=1)
class Replica:
    def get_node_id(self):
        return ray.get_runtime_context().get_node_id()

    def get_placement_group(self):
        return ray.util.get_current_placement_group()


@pytest.mark.parametrize(
    "placement_group_config",
    [
        {},
        {"bundles": [{"CPU": 3}]},
        {"bundles": [{"CPU": 1}, {"CPU": 1}, {"CPU": 1}], "strategy": "STRICT_PACK"},
    ],
)
def test_spread_deployment_scheduling_policy_upscale(
    ray_start_cluster, placement_group_config
):
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

    scheduler = DefaultDeploymentScheduler(cluster_node_info_cache, get_head_node_id())
    dep_id = DeploymentID(name="deployment1")
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
                    deployment_id=dep_id,
                    replica_name="replica1",
                    actor_def=Replica,
                    actor_resources={"CPU": 1},
                    actor_options={"name": "deployment1_replica1"},
                    actor_init_args=(),
                    on_scheduled=on_scheduled,
                    placement_group_bundles=placement_group_config.get("bundles", None),
                    placement_group_strategy=placement_group_config.get(
                        "strategy", None
                    ),
                ),
                ReplicaSchedulingRequest(
                    deployment_id=dep_id,
                    replica_name="replica2",
                    actor_def=Replica,
                    actor_resources={"CPU": 1},
                    actor_options={"name": "deployment1_replica2"},
                    actor_init_args=(),
                    on_scheduled=on_scheduled,
                    placement_group_bundles=placement_group_config.get("bundles", None),
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
    scheduler.on_replica_stopping(dep_id, "replica1")
    scheduler.on_replica_stopping(dep_id, "replica2")
    scheduler.on_deployment_deleted(dep_id)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
