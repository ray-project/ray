import sys

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray._raylet import GcsClient
from ray.serve._private import default_impl
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.constants import RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY
from ray.serve._private.deployment_scheduler import (
    ReplicaSchedulingRequest,
    SpreadDeploymentSchedulingPolicy,
)
from ray.serve._private.test_utils import check_apps_running, get_node_id
from ray.serve._private.utils import get_head_node_id
from ray.tests.conftest import *  # noqa


@ray.remote(num_cpus=1)
class Replica:
    def get_node_id(self):
        return ray.get_runtime_context().get_node_id()

    def get_placement_group(self):
        return ray.util.get_current_placement_group()


@pytest.mark.skipif(
    RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY, reason="Need to use spread strategy"
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
            cluster_node_info_cache,
            get_head_node_id(),
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

    @pytest.mark.asyncio
    async def test_spread_serve_strict_spread_pg(self, ray_cluster):
        """
        Verifies STRICT_SPREAD PG strategy runs successfully in the Spread Scheduler
        and spreads bundles across distinct nodes.
        """
        cluster = ray_cluster
        cluster.add_node(num_cpus=3)
        cluster.add_node(num_cpus=3)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        @ray.remote(num_cpus=0)
        def get_task_node_id():
            return ray.get_runtime_context().get_node_id()

        @serve.deployment(
            placement_group_bundles=[{"CPU": 1}, {"CPU": 1}],
            placement_group_strategy="STRICT_SPREAD",
        )
        class StrictSpread:
            async def get_bundle_node_id(self, bundle_index: int):
                pg = ray.util.get_current_placement_group()
                return await get_task_node_id.options(
                    scheduling_strategy=ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy(
                        placement_group=pg,
                        placement_group_bundle_index=bundle_index,
                    )
                ).remote()

        handle = serve.run(StrictSpread.bind(), name="strict_spread_app")

        node_0 = await handle.get_bundle_node_id.remote(0)
        node_1 = await handle.get_bundle_node_id.remote(1)

        assert node_0 != node_1

        serve.delete("strict_spread_app")
        serve.shutdown()


@serve.deployment
def A():
    return ray.get_runtime_context().get_node_id()


app_A = A.bind()


@pytest.mark.skipif(
    not RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY, reason="Needs pack strategy."
)
class TestPackScheduling:
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
            # [2, 5, 3, 3, 7, 6, 4] -> 3 nodes
            ({5: 1, 3: 2, 7: 1, 2: 1, 6: 1, 4: 1}, 3),
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
            ray_cluster.add_node(num_cpus=1)
        ray_cluster.wait_for_nodes()
        ray.init(address=ray_cluster.address)

        serve.start()

        @serve.deployment
        def A():
            return ray.get_runtime_context().get_node_id()

        @serve.deployment(ray_actor_options={"num_cpus": 0})
        class Ingress:
            def __init__(self, *handles):
                self.handles = handles

            def __call__(self):
                pass

        deployments = []
        for n, count in app_resources.items():
            num_cpus = 0.1 * n
            deployments.append(
                A.options(
                    name=f"A{n}",
                    num_replicas=count,
                    ray_actor_options={"num_cpus": 0 if use_pg else num_cpus},
                    placement_group_bundles=[{"CPU": num_cpus}] if use_pg else None,
                    placement_group_strategy="STRICT_PACK" if use_pg else None,
                ).bind()
            )

        serve.run(Ingress.bind(*deployments))
        wait_for_condition(check_apps_running, apps=["default"])
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

    @pytest.mark.asyncio
    async def test_e2e_serve_strict_pack_pg_label_selector(
        self, serve_instance_with_labeled_nodes
    ):
        """
        Verifies STRICT_PACK strategy with placement_group_bundle_label_selector in Pack Scheduling Mode.

        Since the strategy is STRICT_PACK, both bundles must be scheduled on the same node,
        and that node must satisfy the label constraints in each selector.
        """
        _, _, us_east_node_id, _ = serve_instance_with_labeled_nodes

        @ray.remote(num_cpus=0)
        def get_task_node_id():
            return ray.get_runtime_context().get_node_id()

        @serve.deployment(
            placement_group_bundles=[{"CPU": 1}, {"CPU": 1}],
            placement_group_strategy="STRICT_PACK",
            placement_group_bundle_label_selector=[
                {"gpu-type": "H100", "region": "us-east"}
            ],
        )
        class StrictPackSelector:
            async def get_bundle_node_id(self, bundle_index: int):
                pg = ray.util.get_current_placement_group()
                return await get_task_node_id.options(
                    scheduling_strategy=ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy(
                        placement_group=pg,
                        placement_group_bundle_index=bundle_index,
                    )
                ).remote()

        handle = serve.run(StrictPackSelector.bind(), name="strict_pack_app")

        # Both bundles are scheduled to the same node which matches the label constraints.
        assert await handle.get_bundle_node_id.remote(0) == us_east_node_id
        assert await handle.get_bundle_node_id.remote(1) == us_east_node_id

        serve.delete("strict_pack_app")

    @pytest.mark.asyncio
    async def test_e2e_serve_pack_pg_forces_spread(
        self, serve_instance_with_labeled_nodes
    ):
        """
        Verifies that using non-strict PACK PG strategy with label selectors works.

        STRICT_PACK throws NotImplementedError for selectors. However, 'PACK' is considered a
        'Non-Strict' strategy which forces the scheduler to fall back to 'Spread Mode'.
        """
        _, _, us_east_node_id, _ = serve_instance_with_labeled_nodes

        @serve.deployment(
            placement_group_bundles=[{"CPU": 1}],
            placement_group_strategy="PACK",
            placement_group_bundle_label_selector=[{"gpu-type": "H100"}],
        )
        class PackSelector:
            def get_node_id(self):
                return ray.get_runtime_context().get_node_id()

        # If this stayed in the Pack Scheduler, it would raise NotImplementedError.
        # Because it forces Spread Mode, it succeeds.
        handle = serve.run(PackSelector.bind(), name="pack_selector_app")
        assert await handle.get_node_id.remote() == us_east_node_id
        serve.delete("pack_selector_app")

    @pytest.mark.asyncio
    async def test_e2e_serve_multiple_bundles_selector(
        self, serve_instance_with_labeled_nodes
    ):
        """Verifies multiple bundles with bundle_label_selector are applied correctly."""
        _, us_west_node_id, us_east_node_id, _ = serve_instance_with_labeled_nodes

        # Helper task to return the node ID it's running on
        @ray.remote(num_cpus=0)
        def get_task_node_id():
            return ray.get_runtime_context().get_node_id()

        @serve.deployment(
            placement_group_bundles=[{"CPU": 1}, {"CPU": 1}],
            placement_group_strategy="SPREAD",
            placement_group_bundle_label_selector=[
                {"gpu-type": "H100"},  # matches us-east node
                {"gpu-type": "A100"},  # matches us-west node
            ],
        )
        class MultiBundleSelector:
            async def get_bundle_node_id(self, bundle_index: int):
                pg = ray.util.get_current_placement_group()
                return await get_task_node_id.options(
                    scheduling_strategy=ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy(
                        placement_group=pg,
                        placement_group_bundle_index=bundle_index,
                    )
                ).remote()

        handle = serve.run(MultiBundleSelector.bind(), name="multi_bundle_app")

        # Verify bundles are scheduled to expected nodes based on label selectors.
        assert await handle.get_bundle_node_id.remote(0) == us_east_node_id
        assert await handle.get_bundle_node_id.remote(1) == us_west_node_id
        serve.delete("multi_bundle_app")

    @pytest.mark.asyncio
    async def test_e2e_serve_multiple_bundles_single_bundle_label_selector(
        self, serve_instance_with_labeled_nodes
    ):
        """
        Verifies that when only one bundle_label_selector is provided for multiple bundles,
        the label_selector is applied to each bundle uniformly.
        """
        _, _, us_east_node_id, _ = serve_instance_with_labeled_nodes

        @ray.remote(num_cpus=0)
        def get_task_node_id():
            return ray.get_runtime_context().get_node_id()

        @serve.deployment(
            placement_group_bundles=[{"CPU": 1}, {"CPU": 1}],
            # Use SPREAD to verify the label constraint forces them to same node.
            placement_group_strategy="SPREAD",
            placement_group_bundle_label_selector=[
                {"gpu-type": "H100"},
            ],
        )
        class MultiBundleSelector:
            async def get_bundle_node_id(self, bundle_index: int):
                pg = ray.util.get_current_placement_group()
                return await get_task_node_id.options(
                    scheduling_strategy=ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy(
                        placement_group=pg,
                        placement_group_bundle_index=bundle_index,
                    )
                ).remote()

        handle = serve.run(MultiBundleSelector.bind(), name="multi_bundle_app")
        assert await handle.get_bundle_node_id.remote(0) == us_east_node_id
        assert await handle.get_bundle_node_id.remote(1) == us_east_node_id
        serve.delete("multi_bundle_app")

    @pytest.mark.asyncio
    async def test_e2e_serve_actor_multiple_fallbacks(
        self, serve_instance_with_labeled_nodes
    ):
        """
        Verifies that the scheduler can iterate through a label selector and multiple fallback options.
        """
        _, us_west_node_id, _, _ = serve_instance_with_labeled_nodes

        @serve.deployment(
            ray_actor_options={
                "label_selector": {"region": "invalid-label-1"},
                "fallback_strategy": [
                    {"label_selector": {"region": "invalid-label-2"}},
                    {"label_selector": {"region": "us-west"}},  # Should match
                ],
            }
        )
        class MultiFallbackActor:
            def get_node_id(self):
                return ray.get_runtime_context().get_node_id()

        handle = serve.run(MultiFallbackActor.bind(), name="multi_fallback_app")
        assert await handle.get_node_id.remote() == us_west_node_id
        serve.delete("multi_fallback_app")


@pytest.mark.asyncio
async def test_e2e_serve_label_selector(serve_instance_with_labeled_nodes):
    """
    Verifies that label selectors work correctly for both Actors and Placement Groups.

    This test also verifies that label selectors are respected when scheduling with a
    preferred node ID for resource compaction. This test verifies both the Pack and
    Spread scheduler paths.
    """
    _, us_west_node_id, us_east_node_id, _ = serve_instance_with_labeled_nodes

    # Validate a Serve deplyoment utilizes a label_selector when passed to the Ray Actor options.
    @serve.deployment(ray_actor_options={"label_selector": {"region": "us-west"}})
    class DeploymentActor:
        def get_node_id(self):
            return ray.get_runtime_context().get_node_id()

    handle = serve.run(DeploymentActor.bind(), name="actor_app")
    assert await handle.get_node_id.remote() == us_west_node_id
    serve.delete("actor_app")

    # Validate placement_group scheduling strategy with placement_group_bundle_label_selector
    # and PACK strategy.
    @serve.deployment(
        placement_group_bundles=[{"CPU": 1}],
        placement_group_strategy="PACK",
        placement_group_bundle_label_selector=[{"gpu-type": "H100"}],
    )
    class DeploymentPGPack:
        def get_node_id(self):
            return ray.get_runtime_context().get_node_id()

    handle_pack = serve.run(DeploymentPGPack.bind(), name="pg_pack_app")
    assert await handle_pack.get_node_id.remote() == us_east_node_id
    serve.delete("pg_pack_app")

    # Validate placement_group scheduling strategy with placement_group_bundle_label_selector
    # and SPREAD strategy.
    @serve.deployment(
        placement_group_bundles=[{"CPU": 1}],
        placement_group_strategy="SPREAD",
        placement_group_bundle_label_selector=[{"gpu-type": "H100"}],
    )
    class DeploymentPGSpread:
        def get_node_id(self):
            return ray.get_runtime_context().get_node_id()

    handle_spread = serve.run(DeploymentPGSpread.bind(), name="pg_spread_app")
    assert await handle_spread.get_node_id.remote() == us_east_node_id
    serve.delete("pg_spread_app")


@pytest.mark.asyncio
async def test_e2e_serve_fallback_strategy(serve_instance_with_labeled_nodes):
    """
    Verifies that fallback strategies allow scheduling on alternative nodes when
    primary constraints fail.
    """
    _, _, h100_node_id, _ = serve_instance_with_labeled_nodes

    # Fallback strategy specified for Ray Actor in Serve deployment.
    @serve.deployment(
        ray_actor_options={
            "label_selector": {"region": "unavailable"},
            "fallback_strategy": [{"label_selector": {"gpu-type": "H100"}}],
        }
    )
    class FallbackDeployment:
        def get_node_id(self):
            return ray.get_runtime_context().get_node_id()

    # TODO (ryanaoleary@): Add a test for fallback_strategy in placement group options
    # when support is added.

    handle = serve.run(FallbackDeployment.bind(), name="fallback_app")
    assert await handle.get_node_id.remote() == h100_node_id
    serve.delete("fallback_app")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "use_pg,strategy",
    [
        (False, None),  # Actor-level label_selector.
        (True, "PACK"),  # PG bundle_label_selector with PACK strategy.
        (True, "STRICT_PACK"),  # PG bundle_label_selector with STRICT_PACK strategy.
        (True, "SPREAD"),  # PG bundle_label_selector with SPREAD strategy.
        (
            True,
            "STRICT_SPREAD",
        ),  # PG bundle_label_selector with STRICT_SPREAD strategy.
    ],
)
async def test_e2e_serve_label_selector_unschedulable(
    serve_instance_with_labeled_nodes, use_pg, strategy
):
    """
    Verifies the interaction between unschedulable a placement_group_bundle_label_selector
    and different scheduling strategies in the Pack and Spread Serve scheduler.
    """
    _, _, _, cluster = serve_instance_with_labeled_nodes

    @serve.deployment
    def A():
        return ray.get_runtime_context().get_node_id()

    # Cluster in fixture only contains us-west and us-east.
    target_label = {"region": "eu-central"}

    if use_pg:
        app = A.options(
            num_replicas=1,
            placement_group_bundles=[{"CPU": 1}],
            placement_group_strategy=strategy,
            placement_group_bundle_label_selector=[target_label],
        ).bind()
    else:
        app = A.options(
            num_replicas=1,
            ray_actor_options={"label_selector": target_label},
        ).bind()

    handle = serve._run(app, name="unschedulable_label_app", _blocking=False)

    def check_status(expected_status):
        try:
            status_info = serve.status().applications["unschedulable_label_app"]
            return status_info.status == expected_status
        except KeyError:
            return False

    def verify_resource_request_stuck():
        """Verifies that the underlying resource request is pending."""
        # Serve deployment should be stuck DEPLOYING.
        if not check_status("DEPLOYING"):
            return False

        # Check PG/Actor is actually pending.
        if use_pg:
            pgs = ray.util.state.list_placement_groups()
            return any(pg["state"] == "PENDING" for pg in pgs)
        else:
            actors = ray.util.state.list_actors()
            return any(a["state"] == "PENDING_CREATION" for a in actors)

    # Serve deployment should remain stuck in deploying because Actor/PG can't be scheduled.
    wait_for_condition(verify_resource_request_stuck, timeout=30)
    assert not check_status("RUNNING"), (
        "Test setup failed: The deployment became RUNNING before the required "
        "node was added. The label selector constraint was ignored."
    )

    # Add a suitable node to the cluster.
    new_node = cluster.add_node(
        num_cpus=2, labels=target_label, resources={"target_node": 1}
    )
    cluster.wait_for_nodes()
    expected_node_id = ray.get(
        get_node_id.options(resources={"target_node": 1}).remote()
    )

    # Validate deployment can now be scheduled since label selector is satisfied.
    wait_for_condition(lambda: check_status("RUNNING"), timeout=30)
    assert await handle.remote() == expected_node_id

    serve.delete("unschedulable_label_app")
    cluster.remove_node(new_node)


@pytest.mark.asyncio
async def test_e2e_serve_fallback_strategy_unschedulable(
    serve_instance_with_labeled_nodes,
):
    """
    Verifies that an unschedulable fallback_strategy causes the Serve deployment to wait
    until a suitable node is added to the cluster.
    """
    _, _, _, cluster = serve_instance_with_labeled_nodes

    @serve.deployment
    def A():
        return ray.get_runtime_context().get_node_id()

    fallback_label = {"region": "me-central2"}

    app = A.options(
        num_replicas=1,
        ray_actor_options={
            "label_selector": {"region": "non-existant"},
            "fallback_strategy": [{"label_selector": fallback_label}],
        },
    ).bind()

    handle = serve._run(app, name="unschedulable_fallback_app", _blocking=False)

    def check_status(expected_status):
        try:
            status_info = serve.status().applications["unschedulable_fallback_app"]
            return status_info.status == expected_status
        except KeyError:
            return False

    def verify_resource_request_stuck():
        """Verifies that the underlying resource request is pending."""
        # Serve deployment should be stuck DEPLOYING.
        if not check_status("DEPLOYING"):
            return False

        actors = ray.util.state.list_actors()
        return any(a["state"] == "PENDING_CREATION" for a in actors)

    # Serve deployment should remain stuck in deploying because Actor/PG can't be scheduled.
    wait_for_condition(verify_resource_request_stuck, timeout=30)
    assert not check_status("RUNNING"), (
        "Test setup failed: The deployment became RUNNING before the required "
        "node was added. The label selector constraint was ignored."
    )

    # Add a node that matches the fallback.
    new_node = cluster.add_node(
        num_cpus=2, labels=fallback_label, resources={"fallback_node": 1}
    )
    cluster.wait_for_nodes()
    expected_node_id = ray.get(
        get_node_id.options(resources={"fallback_node": 1}).remote()
    )

    # The serve deployment should recover and start running on the fallback node.
    wait_for_condition(lambda: check_status("RUNNING"), timeout=30)
    assert await handle.remote() == expected_node_id

    serve.delete("unschedulable_fallback_app")
    cluster.remove_node(new_node)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
