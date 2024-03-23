import sys

import pytest

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray._raylet import GcsClient
from ray.serve._private import default_impl
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.constants import RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY
from ray.serve._private.deployment_scheduler import (
    ReplicaSchedulingRequest,
    SpreadDeploymentSchedulingPolicy,
)
from ray.serve._private.test_utils import check_apps_running, get_node_id
from ray.serve._private.utils import get_head_node_id
from ray.serve.context import _get_global_client
from ray.serve.schema import ServeDeploySchema
from ray.tests.conftest import *  # noqa


@ray.remote(num_cpus=1)
class Replica:
    def get_node_id(self):
        return ray.get_runtime_context().get_node_id()

    def get_placement_group(self):
        return ray.util.get_current_placement_group()


@pytest.mark.skipif(
    RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY, reason="Need to use spread strategy"
)
class TestSpreadScheduling:
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
    def test_spread_deployment_scheduling_policy_upscale(
        self, ray_start_cluster, placement_group_config
    ):
        """Test to make sure replicas are spreaded."""
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=3)
        cluster.add_node(num_cpus=3)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)

        cluster_node_info_cache = default_impl.create_cluster_node_info_cache(
            GcsClient(address=ray.get_runtime_context().gcs_address)
        )
        cluster_node_info_cache.update()

        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache, get_head_node_id(), ray.util.placement_group
        )
        dep_id = DeploymentID(name="deployment1")
        r1_id = ReplicaID(unique_id="replica1", deployment_id=dep_id)
        r2_id = ReplicaID(unique_id="replica2", deployment_id=dep_id)
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
                        replica_id=r1_id,
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
                        replica_id=r2_id,
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
        scheduler.on_replica_stopping(r1_id)
        scheduler.on_replica_stopping(r2_id)
        scheduler.on_deployment_deleted(dep_id)


@serve.deployment
def A():
    return ray.get_runtime_context().get_node_id()


app_A = A.bind()


@pytest.mark.skipif(
    not RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY, reason="Needs compact strategy."
)
class TestCompactScheduling:
    @pytest.mark.parametrize("use_pg", [True, False])
    def test_e2e_basic(self, ray_cluster, use_pg: bool):
        cluster = ray_cluster
        cluster.add_node(num_cpus=2, resources={"head": 1})
        cluster.add_node(num_cpus=3, resources={"worker1": 1})
        cluster.add_node(num_cpus=4, resources={"worker2": 1})
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)

        head_node_id = ray.get(get_node_id.options(resources={"head": 1}).remote())
        worker1_node_id = ray.get(
            get_node_id.options(resources={"worker1": 1}).remote()
        )
        worker2_node_id = ray.get(
            get_node_id.options(resources={"worker2": 1}).remote()
        )
        print("head", head_node_id)
        print("worker1", worker1_node_id)
        print("worker2", worker2_node_id)

        # Both f replicas should be scheduled on head node to minimize
        # fragmentation
        if use_pg:
            app1 = A.options(
                num_replicas=2,
                ray_actor_options={"num_cpus": 0.1},
                placement_group_bundles=[{"CPU": 0.5}, {"CPU": 0.5}],
                placement_group_strategy="STRICT_PACK",
            ).bind()
        else:
            app1 = A.options(num_replicas=2, ray_actor_options={"num_cpus": 1}).bind()

        # Both app1 replicas should have been scheduled on head node
        f_handle = serve.run(app1, name="app1", route_prefix="/app1")
        refs = [f_handle.remote() for _ in range(20)]
        assert {ref.result() for ref in refs} == {head_node_id}

        if use_pg:
            app2 = A.options(
                num_replicas=1,
                ray_actor_options={"num_cpus": 0.1},
                placement_group_bundles=[{"CPU": 1}, {"CPU": 2}],
                placement_group_strategy="STRICT_PACK",
            ).bind()
        else:
            app2 = A.options(num_replicas=1, ray_actor_options={"num_cpus": 3}).bind()

        # Then there should be enough space for the g replica
        # The g replica should be scheduled on worker1, not worker2, to
        # minimize fragmentation
        g_handle = serve.run(app2, name="app2", route_prefix="/app2")
        assert g_handle.remote().result() == worker1_node_id

        serve.shutdown()

    @pytest.mark.parametrize("use_pg", [True, False])
    @pytest.mark.parametrize(
        "app_resources,expected_worker_nodes",
        [
            # [2, 5, 3, 3, 7, 2, 6, 2] -> 3 nodes
            ({5: 1, 3: 2, 7: 1, 2: 3, 6: 1}, 3),
            # [1, 7, 7, 3, 2] -> 2 nodes
            ({1: 1, 7: 2, 3: 1, 2: 1}, 2),
            # [7, 3, 2, 7, 7, 2] -> 3 nodes
            ({7: 3, 3: 1, 2: 2}, 3),
        ],
    )
    def test_e2e_fit_replicas(
        self, ray_cluster, use_pg, app_resources, expected_worker_nodes
    ):
        for _ in range(expected_worker_nodes):
            ray_cluster.add_node(num_cpus=10)
        ray_cluster.wait_for_nodes()
        ray.init(address=ray_cluster.address)

        serve.start()
        client = _get_global_client()

        applications = []
        for n, count in app_resources.items():
            name = n
            num_cpus = 0.1 * n
            app = {
                "name": f"app{name}",
                "import_path": "ray.serve.tests.test_deployment_scheduler.app_A",
                "route_prefix": f"/app{name}",
                "deployments": [
                    {
                        "name": "A",
                        "num_replicas": count,
                        "ray_actor_options": {"num_cpus": 0 if use_pg else num_cpus},
                    }
                ],
            }
            if use_pg:
                app["deployments"][0]["placement_group_bundles"] = [{"CPU": num_cpus}]
                app["deployments"][0]["placement_group_strategy"] = "STRICT_PACK"

            applications.append(app)

        client.deploy_apps(ServeDeploySchema(**{"applications": applications}))
        wait_for_condition(check_apps_running, apps=[f"app{n}" for n in app_resources])
        print("Test passed!")

    @pytest.mark.parametrize("use_pg", [True, False])
    def test_e2e_custom_resources(self, ray_cluster, use_pg):
        cluster = ray_cluster
        cluster.add_node(num_cpus=1, resources={"head": 1})
        cluster.add_node(num_cpus=3, resources={"worker1": 1, "customabcd": 1})
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)

        worker1_node_id = ray.get(
            get_node_id.options(resources={"worker1": 1}).remote()
        )

        if use_pg:
            app = A.options(
                num_replicas=1,
                ray_actor_options={"num_cpus": 0},
                placement_group_bundles=[{"CPU": 0.5}, {"CPU": 0.5, "customabcd": 0.1}],
                placement_group_strategy="STRICT_PACK",
            ).bind()
        else:
            app = A.options(
                num_replicas=1,
                ray_actor_options={"num_cpus": 1, "resources": {"customabcd": 0.1}},
            ).bind()

        handle1 = serve.run(app, name="app1", route_prefix="/app1")
        refs = [handle1.remote() for _ in range(20)]
        assert all(ref.result() == worker1_node_id for ref in refs)

        serve.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
