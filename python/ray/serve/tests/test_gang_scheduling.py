import os
import sys
import tempfile

import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.common import GANG_PG_NAME_PREFIX, DeploymentID, ReplicaState
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve._private.test_utils import check_apps_running
from ray.serve._private.utils import get_all_live_placement_group_names
from ray.serve.config import GangPlacementStrategy, GangSchedulingConfig
from ray.serve.context import _get_global_client
from ray.tests.conftest import *  # noqa
from ray.util.placement_group import get_current_placement_group, placement_group_table
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@ray.remote
class Collector:
    def __init__(self):
        self.items = []

    def add(self, item):
        self.items.append(item)

    def get(self):
        return self.items


@ray.remote(num_cpus=0)
class FailedReplicaStore:
    """Stores the first replica ID that failed, for gang startup failure tests."""

    def __init__(self):
        self._failed_replica_id = None

    def set_if_first(self, replica_id: str) -> bool:
        """Atomically set failed replica if none set. Returns True if we're the first."""
        if self._failed_replica_id is None:
            self._failed_replica_id = replica_id
            return True
        return False

    def get(self):
        return self._failed_replica_id


class TestGangScheduling:
    """Tests for gang scheduling with placement groups."""

    def test_sufficient_resources(self, ray_cluster):
        """Verifies that gang scheduling succeeds when cluster has sufficient resources."""
        cluster = ray_cluster
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=1)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        @serve.deployment(
            num_replicas=8,
            ray_actor_options={"num_cpus": 0.25},
            gang_scheduling_config=GangSchedulingConfig(gang_size=4),
        )
        class GangDeployment:
            def __call__(self):
                return ray.get_runtime_context().get_node_id()

        handle = serve.run(GangDeployment.bind(), name="gang_app_success")
        wait_for_condition(
            check_apps_running,
            apps=["gang_app_success"],
        )

        # Verify all replicas are running and responding
        refs = [handle.remote() for _ in range(8)]
        results = [ref.result() for ref in refs]
        assert len(results) == 8

        serve.delete("gang_app_success")
        serve.shutdown()

    def test_sufficient_resources_with_options(self, ray_cluster):
        """Verifies gang scheduling via .options() succeeds and responds to requests."""
        cluster = ray_cluster
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=1)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        @serve.deployment(num_replicas=1, ray_actor_options={"num_cpus": 0})
        class GangDeployment:
            def __call__(self):
                return ray.get_runtime_context().get_node_id()

        app = GangDeployment.options(
            num_replicas=8,
            ray_actor_options={"num_cpus": 0.25},
            gang_scheduling_config=GangSchedulingConfig(gang_size=4),
        ).bind()

        handle = serve.run(app, name="gang_app_options")
        wait_for_condition(
            check_apps_running,
            apps=["gang_app_options"],
        )

        # Verify all replicas are running and responding
        refs = [handle.remote() for _ in range(8)]
        results = [ref.result() for ref in refs]
        assert len(results) == 8

        serve.delete("gang_app_options")
        serve.shutdown()

    def test_incomplete_deployment(self, ray_cluster):
        """
        Verifies that schedulable gangs serve traffic while unschedulable gangs wait for resources.
        """
        cluster = ray_cluster
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=1)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        @serve.deployment
        class IncompleteGangDeployment:
            def __call__(self):
                return ray.get_runtime_context().get_node_id()

        app = IncompleteGangDeployment.options(
            num_replicas=12,
            ray_actor_options={"num_cpus": 0.25},
            gang_scheduling_config=GangSchedulingConfig(gang_size=4),
        ).bind()

        handle = serve._run(app, name="gang_partial_app", _blocking=False)

        # The deployment should NOT fail. 2 of 3 gangs should be scheduled,
        # and those 8 replicas should serve traffic. The deployment stays
        # DEPLOYING because it hasn't reached 12 replicas.
        def check_replicas_running(expected_count: int):
            try:
                app_status = serve.status().applications["gang_partial_app"]
                # Should be DEPLOYING
                if app_status.status == "DEPLOY_FAILED":
                    raise AssertionError(
                        "Deployment should not fail with partial gang scheduling"
                    )
                # Check that some replicas are running
                dep_status = list(app_status.deployments.values())[0]
                running = dep_status.replica_states.get("RUNNING", 0)
                assert running == expected_count
                return True
            except KeyError:
                return False

        wait_for_condition(check_replicas_running, expected_count=8, timeout=60)

        # Verify the running replicas can serve traffic.
        results = set()
        for _ in range(40):
            results.add(handle.remote().result())
        assert len(results) > 0

        # Verify deployment is still DEPLOYING
        app_status = serve.status().applications["gang_partial_app"]
        assert app_status.status == "DEPLOYING"

        # Now add a 3rd node so the remaining gang can be scheduled.
        cluster.add_node(num_cpus=1)
        cluster.wait_for_nodes()

        # The deployment should become RUNNING with all 12 replicas.
        wait_for_condition(
            check_apps_running,
            apps=["gang_partial_app"],
            timeout=60,
        )

        # Verify all 12 replicas serve traffic.
        results = set()
        for _ in range(100):
            results.add(handle.remote().result())
        assert len(results) == 3

        serve.delete("gang_partial_app")
        serve.shutdown()

    def test_no_partial_gang(self, ray_cluster):
        """Verifies atomic gang scheduling: no partial gangs are created."""
        cluster = ray_cluster
        # 2 CPUs total: enough for 2 full gangs (1.6 CPUs) but not 3 (2.4 CPUs).
        # The leftover 0.4 CPUs must NOT produce a partial gang.
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=1)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        @serve.deployment
        class AtomicGangDeployment:
            def __call__(self):
                return ray.get_runtime_context().get_node_id()

        app = AtomicGangDeployment.options(
            num_replicas=12,
            ray_actor_options={"num_cpus": 0.2},
            gang_scheduling_config=GangSchedulingConfig(gang_size=4),
        ).bind()

        handle = serve._run(app, name="atomic_gang_app", _blocking=False)

        # Wait until exactly 8 replicas (2 gangs) are running.
        def check_replicas_running(expected_count: int):
            try:
                app_status = serve.status().applications["atomic_gang_app"]
                if app_status.status == "DEPLOY_FAILED":
                    raise AssertionError(
                        "Deployment should not fail — partial gangs should "
                        "serve traffic while waiting for resources."
                    )
                dep_status = list(app_status.deployments.values())[0]
                running = dep_status.replica_states.get("RUNNING", 0)
                assert running == expected_count
                return True
            except KeyError:
                return False

        wait_for_condition(check_replicas_running, expected_count=8, timeout=60)

        # Deployment should still be DEPLOYING (not RUNNING, not DEPLOY_FAILED).
        app_status = serve.status().applications["atomic_gang_app"]
        assert app_status.status == "DEPLOYING"

        # Verify the 8 running replicas can serve traffic.
        results = set()
        for _ in range(80):
            results.add(handle.remote().result())
        assert len(results) > 0

        # Add 1 more CPU so the 3rd gang (0.8 CPUs) can be scheduled.
        cluster.add_node(num_cpus=1)
        cluster.wait_for_nodes()

        # The deployment should become RUNNING with all 12 replicas.
        wait_for_condition(check_apps_running, apps=["atomic_gang_app"], timeout=60)

        # All 12 replicas should now serve traffic.
        app_status = serve.status().applications["atomic_gang_app"]
        dep_status = list(app_status.deployments.values())[0]
        running = dep_status.replica_states.get("RUNNING", 0)
        assert running == 12

        serve.delete("atomic_gang_app")
        serve.shutdown()

    def test_pack_strategy(self, ray_cluster):
        """Verifies that PACK strategy places gang replicas on the same node."""
        cluster = ray_cluster
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=1)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        @serve.deployment
        def PackDeployment():
            return os.environ.get(
                "RAY_NODE_ID", ray.get_runtime_context().get_node_id()
            )

        # 1 gang with PACK strategy - all replicas should be on same node
        app = PackDeployment.options(
            num_replicas=4,
            ray_actor_options={"num_cpus": 0.25},
            gang_scheduling_config=GangSchedulingConfig(
                gang_size=4,
                gang_placement_strategy=GangPlacementStrategy.PACK,
            ),
        ).bind()

        handle = serve.run(app, name="gang_pack_app")
        wait_for_condition(check_apps_running, apps=["gang_pack_app"])

        # Query multiple times to hit all replicas and collect node IDs
        node_ids = set()
        for _ in range(40):
            result = handle.remote().result()
            node_ids.add(result)

        # With PACK strategy, all 4 replicas should be on the same node
        assert len(node_ids) == 1

        serve.delete("gang_pack_app")
        serve.shutdown()

    def test_gang_scheduling_spread_strategy(self, ray_cluster):
        """Verifies that SPREAD strategy places gang replicas on different nodes."""
        cluster = ray_cluster
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=1)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        @serve.deployment
        def SpreadDeployment():
            return os.environ.get(
                "RAY_NODE_ID", ray.get_runtime_context().get_node_id()
            )

        # 1 gang with SPREAD strategy - replicas should be on different nodes
        app = SpreadDeployment.options(
            num_replicas=2,
            ray_actor_options={"num_cpus": 0.25},
            gang_scheduling_config=GangSchedulingConfig(
                gang_size=2,
                gang_placement_strategy=GangPlacementStrategy.SPREAD,
            ),
        ).bind()

        handle = serve.run(app, name="gang_spread_app")
        wait_for_condition(check_apps_running, apps=["gang_spread_app"])

        # Query multiple times to hit all replicas and collect node IDs
        node_ids = set()
        for _ in range(40):
            result = handle.remote().result()
            node_ids.add(result)

        # With SPREAD strategy, 2 replicas should be on 2 different nodes
        assert len(node_ids) == 2

        serve.delete("gang_spread_app")
        serve.shutdown()

    def test_gang_context(self, ray_cluster):
        """Verifies GangContext is correctly populated in ReplicaContext."""
        cluster = ray_cluster
        cluster.add_node(num_cpus=1)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        @serve.deployment
        class GangContextDeployment:
            def __call__(self):
                ctx = ray.serve.context._get_internal_replica_context()
                gc = ctx.gang_context
                if gc is None:
                    return None
                return {
                    "gang_id": gc.gang_id,
                    "rank": gc.rank,
                    "world_size": gc.world_size,
                    "member_replica_ids": gc.member_replica_ids,
                    "replica_id": ctx.replica_id.unique_id,
                }

        app = GangContextDeployment.options(
            num_replicas=4,
            ray_actor_options={"num_cpus": 0.25},
            gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        ).bind()

        handle = serve.run(app, name="gang_context_app")
        wait_for_condition(check_apps_running, apps=["gang_context_app"])

        # Collect gang contexts from all replicas
        # Query enough times to hit all 4 replicas
        contexts_by_replica = {}
        for _ in range(100):
            result = handle.remote().result()
            assert result is not None
            replica_id = result["replica_id"]
            if replica_id not in contexts_by_replica:
                contexts_by_replica[replica_id] = result
            if len(contexts_by_replica) == 4:
                break
        assert len(contexts_by_replica) == 4

        # Group replicas by gang_id
        gangs = {}
        for replica_id, ctx in contexts_by_replica.items():
            gang_id = ctx["gang_id"]
            gangs.setdefault(gang_id, []).append(ctx)

        assert len(gangs) == 2

        for gang_id, members in gangs.items():
            assert len(members) == 2
            assert all(member["world_size"] == 2 for member in members)
            assert members[0]["member_replica_ids"] == members[1]["member_replica_ids"]

            expected_ids = sorted([m["replica_id"] for m in members])
            actual_ids = sorted(members[0]["member_replica_ids"])
            assert actual_ids == expected_ids

            ranks = sorted([m["rank"] for m in members])
            assert ranks == [0, 1]

        # Across gangs: gang_ids should be different
        gang_ids = list(gangs.keys())
        assert gang_ids[0] != gang_ids[1]

        # Across gangs: member_replica_ids should be different
        gang_members_list = list(gangs.values())
        assert sorted(gang_members_list[0][0]["member_replica_ids"]) != sorted(
            gang_members_list[1][0]["member_replica_ids"]
        )

        serve.delete("gang_context_app")
        serve.shutdown()

    def test_gang_placement_groups_cleanup_on_deletion(self, ray_cluster):
        """Verifies serve.delete() removes reserved gang placement groups."""
        cluster = ray_cluster
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=1)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        @serve.deployment(
            num_replicas=4,
            ray_actor_options={"num_cpus": 0.25},
            gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        )
        class GangDeleteCleanupDeployment:
            def __call__(self):
                return "ok"

        app_name = "gang_delete_cleanup_app"
        deployment_name = "GangDeleteCleanupDeployment"
        pg_name_prefix = f"{GANG_PG_NAME_PREFIX}{app_name}_{deployment_name}_"

        serve.run(GangDeleteCleanupDeployment.bind(), name=app_name)
        wait_for_condition(check_apps_running, apps=[app_name])

        wait_for_condition(
            lambda: any(
                name.startswith(pg_name_prefix)
                for name in get_all_live_placement_group_names()
            ),
            timeout=60,
        )

        serve.delete(app_name)
        wait_for_condition(
            lambda: not any(
                name.startswith(pg_name_prefix)
                for name in get_all_live_placement_group_names()
            ),
            timeout=60,
        )
        serve.shutdown()

    def test_multiple_gang_deployments_in_one_app(self, ray_cluster):
        """Verifies two gang deployments run together under one Serve app."""
        cluster = ray_cluster
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=1)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        @serve.deployment(
            num_replicas=4,
            ray_actor_options={"num_cpus": 0.25},
            gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        )
        class GangA:
            def __init__(self, gang_b):
                self._gang_b = gang_b

            def __call__(self):
                return "a"

        @serve.deployment(
            num_replicas=4,
            ray_actor_options={"num_cpus": 0.25},
            gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        )
        class GangB:
            def __call__(self):
                return "b"

        app_name = "multi_gang_app"
        serve.run(GangA.bind(GangB.bind()), name=app_name)
        wait_for_condition(check_apps_running, apps=[app_name])

        app_status = serve.status().applications[app_name]
        assert app_status.deployments["GangA"].replica_states.get("RUNNING", 0) == 4
        assert app_status.deployments["GangB"].replica_states.get("RUNNING", 0) == 4

        serve.delete(app_name)
        serve.shutdown()


class TestGangResourceReservation:
    @pytest.mark.parametrize(
        "ray_actor_options, placement_group_bundles, gang_placement_strategy, "
        "expected_bundles, expected_strategy, expect_same_node",
        [
            # Case 1: Only ray_actor_options — one flat bundle per replica, PACK
            (
                {"num_cpus": 0.25},
                None,
                "PACK",
                [{"CPU": 0.25}, {"CPU": 0.25}],
                "PACK",
                True,
            ),
            # Case 2: placement_group_bundles — flattened into the gang PG, PACK
            (
                {"num_cpus": 0},
                [{"CPU": 0.25}] * 2,
                "PACK",
                [{"CPU": 0.25}] * 4,
                "PACK",
                True,
            ),
            # Case 3: placement_group_bundles + SPREAD strategy
            (
                {"num_cpus": 0},
                [{"CPU": 0.25}] * 2,
                "SPREAD",
                [{"CPU": 0.25}] * 4,
                "SPREAD",
                False,
            ),
        ],
    )
    def test_gang_resource_reservation(
        self,
        ray_cluster,
        ray_actor_options,
        placement_group_bundles,
        gang_placement_strategy,
        expected_bundles,
        expected_strategy,
        expect_same_node,
    ):
        """Verifies the gang PG has the correct bundles, strategy, and
        that per-replica bundles are placed according to the strategy."""
        cluster = ray_cluster
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=1)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        deployment_kwargs = {
            "num_replicas": 2,
            "ray_actor_options": ray_actor_options,
            "gang_scheduling_config": GangSchedulingConfig(
                gang_size=2,
                gang_placement_strategy=gang_placement_strategy,
            ),
        }
        if placement_group_bundles is not None:
            deployment_kwargs["placement_group_bundles"] = placement_group_bundles

        @serve.deployment(**deployment_kwargs)
        class GangDeployment:
            def get_pg_info(self):
                pg = get_current_placement_group()
                if pg is None:
                    return None
                pg_table = placement_group_table(pg)
                return {
                    "bundle_specs": pg.bundle_specs,
                    "strategy": pg_table["strategy"],
                    "bundles_to_node_id": pg_table["bundles_to_node_id"],
                }

            def __call__(self):
                return "ok"

        app = GangDeployment.bind()
        handle = serve.run(app, name="gang_reservation_app")
        wait_for_condition(
            check_apps_running,
            apps=["gang_reservation_app"],
        )

        for _ in range(20):
            pg_info = handle.get_pg_info.remote().result()
            assert pg_info is not None
            assert pg_info["bundle_specs"] == expected_bundles
            assert pg_info["strategy"] == expected_strategy

            bundles_per_replica = (
                len(placement_group_bundles) if placement_group_bundles else 1
            )
            gang_size = 2

            for replica_idx in range(gang_size):
                start = replica_idx * bundles_per_replica
                replica_nodes = {
                    pg_info["bundles_to_node_id"][i]
                    for i in range(start, start + bundles_per_replica)
                }
                if expect_same_node:
                    assert len(replica_nodes) == 1
                else:
                    assert len(replica_nodes) == bundles_per_replica

        serve.delete("gang_reservation_app")
        serve.shutdown()

    def test_gang_label_selector(self, ray_cluster):
        """
        Verifies that placement_group_bundle_label_selector steers gang bundles
        onto the labeled node.
        """
        cluster = ray_cluster
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=1, labels={"accelerator": "tpu"})
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        @serve.deployment(
            num_replicas=2,
            ray_actor_options={"num_cpus": 0},
            placement_group_bundles=[{"CPU": 0.25}],
            placement_group_bundle_label_selector=[{"accelerator": "tpu"}],
            gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        )
        class LabeledGangDeployment:
            def get_pg_info(self):
                pg = get_current_placement_group()
                if pg is None:
                    return None
                pg_table = placement_group_table(pg)
                return {
                    "bundle_specs": pg.bundle_specs,
                    "bundles_to_node_id": pg_table["bundles_to_node_id"],
                    "node_labels": ray.get_runtime_context().get_node_labels(),
                }

            def __call__(self):
                return "ok"

        app = LabeledGangDeployment.bind()
        handle = serve.run(app, name="label_selector_app")
        wait_for_condition(
            check_apps_running,
            apps=["label_selector_app"],
        )

        labeled_node_id = None
        for node in ray.nodes():
            if node["Labels"].get("accelerator") == "tpu":
                labeled_node_id = node["NodeID"]
                break
        assert labeled_node_id is not None

        for _ in range(20):
            pg_info = handle.get_pg_info.remote().result()
            assert pg_info is not None
            assert pg_info["bundle_specs"] == [{"CPU": 0.25}, {"CPU": 0.25}]
            # Replica actor itself should be on the labeled node
            assert pg_info["node_labels"].get("accelerator") == "tpu"
            # All bundles in the gang PG should be on the labeled node
            for node_id in pg_info["bundles_to_node_id"].values():
                assert node_id == labeled_node_id

        serve.delete("label_selector_app")
        serve.shutdown()


class TestGangConstructorFailure:
    """Tests for gang scheduling with constructor failures."""

    def test_consistent_constructor_failure(self, ray_shutdown):
        """Validates gang deployment where all replicas consistently fail their constructor."""
        ray.init(num_cpus=1)
        serve.start()

        @serve.deployment(
            num_replicas=4,
            ray_actor_options={"num_cpus": 0.1},
            gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        )
        class GangConstructorFailure:
            def __init__(self):
                raise RuntimeError("Intentionally failing gang replica constructor")

            async def __call__(self, request):
                return "hi"

        with pytest.raises(RuntimeError):
            serve.run(GangConstructorFailure.bind())

        client = serve.context._get_global_client()
        deployment_dict = ray.get(client._controller._all_running_replicas.remote())
        deployment_id = DeploymentID(name="GangConstructorFailure")
        assert len(deployment_dict[deployment_id]) == 0
        app_status = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        assert app_status.status == "DEPLOY_FAILED"
        assert (
            app_status.deployments["GangConstructorFailure"].status == "DEPLOY_FAILED"
        )

    def test_partial_constructor_failure(self, ray_shutdown):
        """Validates gang deployment where one replica consistently fails."""
        ray.init(num_cpus=1)
        serve.start()

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, "test_deploy.txt")

            @serve.deployment(
                num_replicas=4,
                ray_actor_options={"num_cpus": 0.1},
                gang_scheduling_config=GangSchedulingConfig(gang_size=2),
            )
            class GangPartialConstructorFailure:
                def __init__(self):
                    if not os.path.exists(file_path):
                        with open(file_path, "w") as f:
                            f.write(serve.get_replica_context().replica_id.unique_id)
                        raise RuntimeError("Consistently throwing on same replica.")
                    else:
                        with open(file_path) as f:
                            content = f.read()
                        if content == serve.get_replica_context().replica_id.unique_id:
                            raise RuntimeError("Consistently throwing on same replica.")

                async def __call__(self, request):
                    return "hi"

            serve.run(GangPartialConstructorFailure.bind())

        client = serve.context._get_global_client()
        deployment_id = DeploymentID(name="GangPartialConstructorFailure")
        deployment_dict = ray.get(client._controller._all_running_replicas.remote())
        assert len(deployment_dict[deployment_id]) == 4
        app_status = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        assert app_status.status == "RUNNING"
        assert (
            app_status.deployments["GangPartialConstructorFailure"].status == "HEALTHY"
        )

    def test_transient_constructor_failure(self, ray_shutdown):
        """Validates gang deployment where the first constructor call fails then succeeds."""
        ray.init(num_cpus=1)
        serve.start()

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, "test_deploy.txt")

            @serve.deployment(
                num_replicas=4,
                ray_actor_options={"num_cpus": 0.1},
                gang_scheduling_config=GangSchedulingConfig(gang_size=2),
            )
            class GangTransientConstructorFailure:
                def __init__(self):
                    if os.path.exists(file_path):
                        return
                    with open(file_path, "w") as f:
                        f.write("ONE")
                    raise RuntimeError("Intentionally throw on first try.")

                async def __call__(self, request):
                    return "hi"

            serve.run(GangTransientConstructorFailure.bind())

        client = serve.context._get_global_client()
        deployment_id = DeploymentID(name="GangTransientConstructorFailure")
        deployment_dict = ray.get(client._controller._all_running_replicas.remote())
        assert len(deployment_dict[deployment_id]) == 4
        app_status = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        assert app_status.status == "RUNNING"
        assert (
            app_status.deployments["GangTransientConstructorFailure"].status
            == "HEALTHY"
        )


class TestGangFailureRecovery:
    def test_startup_failure_stops_entire_gang(self, ray_shutdown):
        """Startup failure stops both replicas in the affected gang."""
        ray.init(num_cpus=1)
        serve.start()
        failed_replica_store = FailedReplicaStore.remote()
        recovery_signal = SignalActor.remote()

        @serve.deployment(
            num_replicas=4,
            ray_actor_options={"num_cpus": 0.1},
            gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        )
        class StartupFailureDeployment:
            def __init__(self, failed_replica_store, recovery_signal):
                replica_id = serve.get_replica_context().replica_id.unique_id
                is_first_failure = ray.get(
                    failed_replica_store.set_if_first.remote(replica_id)
                )
                if is_first_failure:
                    raise RuntimeError("Fail one startup to trigger gang cleanup.")

                failed_replica_id = ray.get(failed_replica_store.get.remote())
                if replica_id == failed_replica_id:
                    # Hold failed replica retry until the intermediate state is asserted.
                    ray.get(recovery_signal.wait.remote())

            def __call__(self):
                ctx = serve.get_replica_context()
                gc = ctx.gang_context
                return {
                    "replica_id": ctx.replica_id.unique_id,
                    "gang_id": gc.gang_id,
                }

        app_name = "gang_startup_cleanup_app"
        deployment_name = "StartupFailureDeployment"
        handle = serve._run(
            StartupFailureDeployment.bind(failed_replica_store, recovery_signal),
            name=app_name,
            _blocking=False,
        )

        # The unaffected gang should reach 2 RUNNING while the failed
        # gang is being cleaned up and retried.
        wait_for_condition(
            lambda: (
                serve.status()
                .applications[app_name]
                .deployments[deployment_name]
                .replica_states.get("RUNNING", 0)
                == 2
            ),
            timeout=60,
        )

        # The 2 running replicas must belong to the SAME gang,
        # proving no partial gang survived.
        contexts = {}
        for _ in range(50):
            result = handle.remote().result()
            contexts.setdefault(result["replica_id"], result)
            if len(contexts) == 2:
                break
        assert len(contexts) == 2
        assert len({ctx["gang_id"] for ctx in contexts.values()}) == 1

        # Release constructor retry gate so the failed gang can recover.
        ray.get(recovery_signal.send.remote())

        # After retry, all 4 replicas should be RUNNING.
        wait_for_condition(check_apps_running, apps=[app_name], timeout=60)
        app_status = serve.status().applications[app_name]
        dep_status = app_status.deployments[deployment_name]
        assert dep_status.replica_states.get("RUNNING", 0) == 4

        serve.delete(app_name)
        serve.shutdown()

    def test_health_failure_restarts_gang(self, ray_shutdown):
        """Single health check failure tears down and restarts the entire gang."""
        ray.init(num_cpus=1)
        serve.start()
        target_replica_collector = Collector.remote()

        @serve.deployment(
            num_replicas=4,
            ray_actor_options={"num_cpus": 0.1},
            health_check_period_s=1,
            health_check_timeout_s=1,
            gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        )
        class HealthFailureDeployment:
            def __call__(self):
                ctx = serve.get_replica_context()
                gc = ctx.gang_context
                return {
                    "replica_id": ctx.replica_id.unique_id,
                    "gang_id": gc.gang_id,
                }

            def check_health(self):
                targets = ray.get(target_replica_collector.get.remote())
                if not targets:
                    return
                target_id = targets[-1]
                # Only 1 replica fails; its sibling stays healthy.
                # The gang-aware cleanup must stop the sibling too.
                ctx = serve.get_replica_context()
                if ctx.replica_id.unique_id == target_id:
                    raise RuntimeError("Intentional health check failure.")

        app_name = "gang_health_failure_app"
        deployment_name = "HealthFailureDeployment"
        handle = serve.run(HealthFailureDeployment.bind(), name=app_name)
        wait_for_condition(check_apps_running, apps=[app_name], timeout=60)

        # Discover all 4 replica contexts.
        contexts_by_replica = {}
        for _ in range(120):
            result = handle.remote().result()
            contexts_by_replica.setdefault(result["replica_id"], result)
            if len(contexts_by_replica) == 4:
                break
        assert len(contexts_by_replica) == 4

        # Pick 1 replica to fail health checks.
        target_ctx = next(iter(contexts_by_replica.values()))
        target_gang_id = target_ctx["gang_id"]

        target_gang_replica_ids = {
            ctx["replica_id"]
            for ctx in contexts_by_replica.values()
            if ctx["gang_id"] == target_gang_id
        }
        unaffected_replica_ids = (
            set(contexts_by_replica.keys()) - target_gang_replica_ids
        )
        assert len(target_gang_replica_ids) == 2
        assert len(unaffected_replica_ids) == 2

        # Trigger failure for only 1 replica in the target gang.
        ray.get(target_replica_collector.add.remote(target_ctx["replica_id"]))

        client = serve.context._get_global_client()
        deployment_id = DeploymentID(name=deployment_name, app_name=app_name)

        def check_target_gang_restarted():
            replicas = ray.get(
                client._controller._dump_replica_states_for_testing.remote(
                    deployment_id
                )
            )
            running_replicas = replicas.get([ReplicaState.RUNNING])
            running_ids = {r.replica_id.unique_id for r in running_replicas}
            # Both old gang members must be gone (not just the one that
            # failed), and the unaffected gang must be untouched.
            return (
                len(running_ids) == 4
                and len(running_ids & target_gang_replica_ids) == 0
                and len(running_ids & unaffected_replica_ids) == 2
            )

        wait_for_condition(check_target_gang_restarted, timeout=90)
        wait_for_condition(check_apps_running, apps=[app_name], timeout=60)
        serve.delete(app_name)
        serve.shutdown()


class TestGangChildSpawnPlacementGroup:
    @ray.remote(num_cpus=0.1)
    class ChildActor:
        def get_pg(self):
            return get_current_placement_group()

    @ray.remote(num_cpus=0)
    def child_task_get_pg():
        return get_current_placement_group()

    @pytest.mark.parametrize("child_type", ["actor", "task"])
    def test_child_in_gang_pg(self, ray_cluster, child_type):
        """Spawn a child actor/task inside a gang replica and verify it shares the gang placement group."""
        cluster = ray_cluster
        cluster.add_node(num_cpus=2)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        ChildActor = TestGangChildSpawnPlacementGroup.ChildActor
        child_task_get_pg = TestGangChildSpawnPlacementGroup.child_task_get_pg

        @serve.deployment(
            num_replicas=2,
            ray_actor_options={"num_cpus": 0.1},
            # Extra bundle per replica so the child actor has resources
            # inside the gang PG (the first bundle is consumed by the replica).
            placement_group_bundles=[{"CPU": 0.1}, {"CPU": 0.1}],
            gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        )
        class GangWithChild:
            def test_child_in_pg(self):
                parent_pg = get_current_placement_group()
                if child_type == "actor":
                    child = ChildActor.remote()
                    child_pg = ray.get(child.get_pg.remote())
                else:
                    child_pg = ray.get(child_task_get_pg.remote())
                return {
                    "parent_pg_id": parent_pg.id.hex() if parent_pg else None,
                    "child_pg_id": child_pg.id.hex() if child_pg else None,
                }

            def __call__(self):
                return "ok"

        app_name = "gang_child_app"
        handle = serve.run(GangWithChild.bind(), name=app_name)
        wait_for_condition(check_apps_running, apps=[app_name])

        for _ in range(20):
            result = handle.test_child_in_pg.remote().result()
            assert result["parent_pg_id"] is not None
            assert result["child_pg_id"] is not None
            assert result["child_pg_id"] == result["parent_pg_id"]

        serve.delete(app_name)
        serve.shutdown()

    def test_child_actor_gang_pg_bundles_bounded(self, ray_cluster):
        """Gang replicas with placement_group_bundles: verify child actors are resource-bounded by the gang PG."""
        cluster = ray_cluster
        cluster.add_node(num_cpus=2)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        ChildActor = TestGangChildSpawnPlacementGroup.ChildActor

        @serve.deployment(
            num_replicas=1,
            ray_actor_options={"num_cpus": 0.1},
            # Replica consumes the first bundle (0.1 CPU). Worker bundle (0.1
            # CPU) fits exactly one ChildActor, so a second child is blocked.
            placement_group_bundles=[{"CPU": 0.1}, {"CPU": 0.1}],
            gang_scheduling_config=GangSchedulingConfig(gang_size=1),
        )
        class GangWithBundlesAndChild:
            def test_second_worker_blocked(self):
                """The second child actor shouldn't fit in this replica's bundle slice."""
                w1 = ChildActor.remote()
                w2 = ChildActor.remote()
                ready, _ = ray.wait([w2.get_pg.remote()], timeout=1)
                ray.kill(w1)
                ray.kill(w2)
                return len(ready) == 0

            def __call__(self):
                return "ok"

        app_name = "gang_bundles_child_app"
        handle = serve.run(GangWithBundlesAndChild.bind(), name=app_name)
        wait_for_condition(check_apps_running, apps=[app_name])

        # Verify resource limits are enforced within the gang PG bundle slice.
        for _ in range(4):
            assert handle.test_second_worker_blocked.remote().result() is True

        serve.delete(app_name)
        serve.shutdown()

    def test_child_actor_opt_out_gang_pg(self, ray_cluster):
        """Verify a child actor can opt out of the gang PG by passing placement_group=None."""
        cluster = ray_cluster
        cluster.add_node(num_cpus=2)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        ChildActor = TestGangChildSpawnPlacementGroup.ChildActor

        @serve.deployment(
            num_replicas=2,
            ray_actor_options={"num_cpus": 0.1},
            gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        )
        class GangWithEscapedChild:
            def get_child_outside_pg(self):
                parent_pg = get_current_placement_group()
                child = ChildActor.options(
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=None,  # Explicitly schedule outside the placement group
                    )
                ).remote()
                child_pg = ray.get(child.get_pg.remote())
                return {
                    "parent_pg_id": parent_pg.id.hex() if parent_pg else None,
                    "child_pg_id": child_pg.id.hex() if child_pg else None,
                }

            def __call__(self):
                return "ok"

        app_name = "gang_escaped_child_app"
        handle = serve.run(GangWithEscapedChild.bind(), name=app_name)
        wait_for_condition(check_apps_running, apps=[app_name])

        for _ in range(20):
            result = handle.get_child_outside_pg.remote().result()
            assert result["parent_pg_id"] is not None
            assert result["child_pg_id"] is None

        serve.delete(app_name)
        serve.shutdown()


class TestGangControllerRecovery:
    def test_gang_context_recovery(self, ray_cluster):
        """Verifies that the controller recovers all app and deployment states
        after a crash, including gang_context for gang deployments and normal
        replicas for non-gang deployments.
        """
        cluster = ray_cluster
        cluster.add_node(num_cpus=1)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        @serve.deployment(
            num_replicas=4,
            ray_actor_options={"num_cpus": 0.1},
            gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        )
        class Gang1:
            def __call__(self):
                return "ok"

        @serve.deployment(
            num_replicas=2,
            ray_actor_options={"num_cpus": 0.1},
            gang_scheduling_config=GangSchedulingConfig(gang_size=2),
        )
        class Gang2:
            def __call__(self):
                return "ok"

        @serve.deployment(
            num_replicas=2,
            ray_actor_options={"num_cpus": 0.1},
        )
        class NoGang:
            def __call__(self):
                return "ok"

        app_names = ["gang_app1", "gang_app2", "no_gang_app"]
        serve.run(Gang1.bind(), name="gang_app1", route_prefix="/gang1")
        serve.run(Gang2.bind(), name="gang_app2", route_prefix="/gang2")
        serve.run(NoGang.bind(), name="no_gang_app", route_prefix="/no_gang")
        wait_for_condition(check_apps_running, apps=app_names)

        gang_deployment_ids = [
            DeploymentID(name="Gang1", app_name="gang_app1"),
            DeploymentID(name="Gang2", app_name="gang_app2"),
        ]
        no_gang_deployment_id = DeploymentID(name="NoGang", app_name="no_gang_app")
        controller = _get_global_client()._controller

        # Record controller-side gang_context before crash
        gang_ctx_before = {}
        for dep_id in gang_deployment_ids:
            replicas = ray.get(
                controller._dump_replica_states_for_testing.remote(dep_id)
            )
            running = replicas.get([ReplicaState.RUNNING])
            for r in running:
                assert r.gang_context is not None
                gang_ctx_before[r.replica_id.unique_id] = r.gang_context

        # Record non-gang replica count
        no_gang_replicas = ray.get(
            controller._dump_replica_states_for_testing.remote(no_gang_deployment_id)
        )
        no_gang_count_before = len(no_gang_replicas.get([ReplicaState.RUNNING]))
        assert no_gang_count_before == 2

        # Kill the controller and wait for recovery of all apps
        ray.kill(controller, no_restart=False)
        wait_for_condition(check_apps_running, apps=app_names, timeout=60)

        new_controller = _get_global_client()._controller

        def all_states_recovered():
            # Verify gang_context recovered for all gang deployments
            for dep_id in gang_deployment_ids:
                replicas = ray.get(
                    new_controller._dump_replica_states_for_testing.remote(dep_id)
                )
                running = replicas.get([ReplicaState.RUNNING])
                for r in running:
                    before = gang_ctx_before.get(r.replica_id.unique_id)
                    if r.gang_context is None or r.gang_context != before:
                        return False

            # Verify non-gang deployment recovered
            replicas = ray.get(
                new_controller._dump_replica_states_for_testing.remote(
                    no_gang_deployment_id
                )
            )
            if len(replicas.get([ReplicaState.RUNNING])) != no_gang_count_before:
                return False

            return True

        wait_for_condition(all_states_recovered, timeout=60)

        # Verify application and deployment statuses after recovery
        status = serve.status()
        for app_name in app_names:
            app_status = status.applications[app_name]
            assert app_status.status == "RUNNING"
            for dep_name, dep_status in app_status.deployments.items():
                assert dep_status.status == "HEALTHY"

        for app_name in app_names:
            serve.delete(app_name)
        serve.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
