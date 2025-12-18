import sys
from unittest import mock

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray._raylet import GcsClient
from ray.serve._private import default_impl
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.constants import RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY
from ray.serve._private.deployment_scheduler import (
    ReplicaSchedulingRequest,
    Resources,
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


class TestSchedulerUnit:
    def test_schedule_passes_placement_group_options(self):
        """Test that bundle_label_selector is passed to CreatePlacementGroupRequest."""
        cluster_node_info_cache = default_impl.create_cluster_node_info_cache(
            GcsClient(address=ray.get_runtime_context().gcs_address)
        )

        captured_requests = []

        def mock_create_pg(request):
            captured_requests.append(request)

            class MockPG:
                def wait(self, *args):
                    return True

            return MockPG()

        scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            get_head_node_id(),
            create_placement_group_fn_override=mock_create_pg,
        )

        dep_id = DeploymentID(name="pg_options_test")
        # Use Spread policy here, but the logic is shared across policies.
        scheduler.on_deployment_created(dep_id, SpreadDeploymentSchedulingPolicy())

        test_labels = [{"region": "us-west"}]
        # Create a request with the new options
        req = ReplicaSchedulingRequest(
            replica_id=ReplicaID("r1", dep_id),
            actor_def=Replica,
            actor_resources={"CPU": 1},
            actor_options={"name": "r1"},
            actor_init_args=(),
            on_scheduled=lambda *args: None,
            placement_group_bundles=[{"CPU": 1}],
            placement_group_bundle_label_selector=test_labels,
        )

        scheduler.schedule(upscales={dep_id: [req]}, downscales={})

        # Verify the PlacementGroupSchedulingRequest is created.
        assert len(captured_requests) == 1
        pg_request = captured_requests[0]

        # bundle_label_selector should be passed to request.
        assert pg_request.bundle_label_selector == test_labels

    def test_filter_nodes_by_labels(self):
        """Test _filter_nodes_by_labels logic used by _find_best_available_node
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
        filtered = scheduler._filter_nodes_by_labels(
            nodes, {"region": "us-west"}, node_labels
        )
        assert set(filtered.keys()) == {"n1"}

        # not equals operator
        filtered = scheduler._filter_nodes_by_labels(
            nodes, {"region": "!us-west"}, node_labels
        )
        assert set(filtered.keys()) == {"n2", "n3"}

        # in operator
        filtered = scheduler._filter_nodes_by_labels(
            nodes, {"region": "in(us-west, us-east)"}, node_labels
        )
        assert set(filtered.keys()) == {"n1", "n2"}

        # !in operator
        filtered = scheduler._filter_nodes_by_labels(
            nodes, {"env": "!in(dev, staging)"}, node_labels
        )
        assert set(filtered.keys()) == {"n1"}

        # Missing labels treated as not a match for equality.
        filtered = scheduler._filter_nodes_by_labels(
            nodes, {"gpu": "A100"}, node_labels
        )
        assert set(filtered.keys()) == {"n2"}

        # Not equal should match node with missing labels.
        filtered = scheduler._filter_nodes_by_labels(nodes, {"gpu": "!T4"}, node_labels)
        assert set(filtered.keys()) == {"n2", "n3"}

        # Validate we handle whitespace.
        filtered = scheduler._filter_nodes_by_labels(
            nodes, {"region": "in(  us-west , us-east  )"}, node_labels
        )
        assert set(filtered.keys()) == {"n1", "n2"}

    @mock.patch(
        "ray.serve._private.deployment_scheduler.RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY",
        True,
    )
    def test_compact_schedule_respects_labels_and_fallback(self):
        """Test that scheduler respects labels and tries fallback strategies
        when selecting the best node for bin-packing."""

        N1_ID = "1" * 56
        N2_ID = "2" * 56
        N3_ID = "3" * 56

        # Mock some active nodes with varying resources and labels.
        class MockActor:
            def options(self, **kwargs):
                return self

            def remote(self, *args, **kwargs):
                return "mock_handle"

        class MockCache:
            def get_active_node_ids(self):
                return {N1_ID, N2_ID, N3_ID}

            def get_node_labels(self, node_id):
                return {
                    N1_ID: {"region": "us-west"},
                    N2_ID: {"region": "us-east"},
                    N3_ID: {"region": "eu-central"},
                }[node_id]

            def get_available_resources_per_node(self):
                return {
                    N1_ID: {"CPU": 2},
                    N2_ID: {"CPU": 10},
                    N3_ID: {"CPU": 4},
                }

            def get_total_resources_per_node(self):
                return self.get_available_resources_per_node()

        scheduler = default_impl.create_deployment_scheduler(
            MockCache(), "head_node_id"
        )

        # Validate the label_selector constraint is respected.
        # For a request of 1 CPU and "us-west", we pick the node that matches the label
        # (n2) even though a better fit exists (n1).
        dep_id = DeploymentID(name="test_labels")
        scheduler.on_deployment_created(dep_id, SpreadDeploymentSchedulingPolicy())
        scheduler._deployments[dep_id].actor_resources = Resources({"CPU": 1})

        req = ReplicaSchedulingRequest(
            replica_id=ReplicaID("r1", dep_id),
            actor_def=Replica,
            actor_resources={"CPU": 1},
            actor_options={"label_selector": {"region": "us-west"}, "name": "r1"},
            actor_init_args=(),
            on_scheduled=lambda *args, **kwargs: None,
        )

        scheduler.schedule(upscales={dep_id: [req]}, downscales={})
        launch_info = scheduler._launching_replicas[dep_id][req.replica_id]
        assert launch_info.target_node_id == N1_ID

        # Validate fallback strategy is used when selecting a node. If the
        # label constraints and resources of the primary strategy are infeasible,
        # we try each fallback strategy in-order.
        req_fallback = ReplicaSchedulingRequest(
            replica_id=ReplicaID("r2", dep_id),
            actor_def=Replica,
            actor_resources={"CPU": 1},
            actor_options={
                "label_selector": {"region": "us-north"},  # Invalid
                "fallback_strategy": [
                    {"label_selector": {"region": "eu-central"}}
                ],  # Valid (n3)
                "name": "r2",
            },
            actor_init_args=(),
            on_scheduled=lambda *args, **kwargs: None,
        )

        scheduler.schedule(upscales={dep_id: [req_fallback]}, downscales={})
        launch_info = scheduler._launching_replicas[dep_id][req_fallback.replica_id]
        assert launch_info.target_node_id == N3_ID


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
