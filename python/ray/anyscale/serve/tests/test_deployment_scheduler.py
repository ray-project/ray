import os
import random
import sys
import time
from typing import List, Optional, Set

import pytest

import ray
from ray import serve
from ray._private.test_utils import SignalActor, kill_raylet, wait_for_condition
from ray._raylet import GcsClient
from ray.anyscale._private.constants import ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL
from ray.cluster_utils import AutoscalingCluster, Cluster
from ray.serve._private import default_impl
from ray.serve._private.common import DeploymentID, DeploymentStatus, ReplicaID
from ray.serve._private.constants import (
    RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY,
    SERVE_CONTROLLER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.deployment_scheduler import (
    ReplicaSchedulingRequest,
    SpreadDeploymentSchedulingPolicy,
)
from ray.serve._private.deployment_state import ReplicaState
from ray.serve._private.test_utils import (
    check_deployment_status,
    check_num_alive_nodes,
    check_replica_counts,
    get_node_id,
)
from ray.serve.context import _get_global_client
from ray.serve.schema import ServeDeploySchema
from ray.serve.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa
from ray.util.state import list_actors


@ray.remote(num_cpus=1)
class Replica:
    def get_node_id(self):
        return ray.get_runtime_context().get_node_id()

    def get_placement_group(self):
        return ray.util.get_current_placement_group()


def check_node_dead(node_id: str):
    target_node = [node for node in ray.nodes() if node["NodeID"] == node_id][0]
    assert not target_node["Alive"]
    return True


def get_controller_pid() -> Optional[int]:
    all_current_actors = list_actors(filters=[("state", "=", "ALIVE")])
    for actor in all_current_actors:
        if SERVE_CONTROLLER_NAME == actor["name"]:
            return actor["pid"]


def kill_controller_and_wait_for_restart(controller):
    old_pid = get_controller_pid()
    ray.kill(controller, no_restart=False)
    wait_for_condition(lambda: get_controller_pid() != old_pid)


@pytest.mark.skipif(
    RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY, reason="Needs spread strategy."
)
class TestSpreadScheduling:
    def test_upscale_multi_az(self, ray_start_cluster):
        """Test replicas are spread across AZ, and the attempt is best-effort so
        replicas will still be scheduled even if they are imbalanced across AZs.
        """

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

        cluster_node_info_cache = default_impl.create_cluster_node_info_cache(
            GcsClient(address=ray.get_runtime_context().gcs_address)
        )
        cluster_node_info_cache.update()

        scheduler = default_impl.create_deployment_scheduler(cluster_node_info_cache)
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

        cluster_node_info_cache = default_impl.create_cluster_node_info_cache(
            GcsClient(address=ray.get_runtime_context().gcs_address)
        )
        cluster_node_info_cache.update()

        scheduler = default_impl.create_deployment_scheduler(cluster_node_info_cache)
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


def get_current_replica_ids(
    deployment_id: DeploymentID,
    states: List[ReplicaState] = None,
) -> Set[str]:
    client = _get_global_client()
    details = client.get_serve_details()
    app = details["applications"][deployment_id.app_name]
    replicas = app["deployments"][deployment_id.name]["replicas"]
    if not states:
        return {r["replica_id"] for r in replicas}

    return {r["replica_id"] for r in replicas if r["state"] in states}


@serve.deployment
class BlockInit:
    def __init__(self):
        signal = ray.get_actor("signal123")
        ray.get(signal.wait.remote())

    def __call__(self):
        return ray.get_runtime_context().get_node_id()

    def get_pid(self):
        return os.getpid()


app_B = BlockInit.bind()


@pytest.fixture
def setup_compact_scheduling(request, monkeypatch):
    """Setup fixture for compact scheduling e2e tests.

    1. Starts an empty autoscaling cluster with 0 worker nodes.
    2. Deploys 2 Serve applications, A (1 replica, 1CPU) and B (1 replica, 2CPU).
       Both should fit on a single worker node (node 1).
    3. Upscales application A from 1 to 6 replicas. This should add 2 more nodes,
       one of them (let's say node 3) have an extra CPU available.
    4. Removes application B. This should leave node 1 with just one 1-CPU replica.
    5. Node 1 should be compacted (the 1-CPU replica can be moved to node 3).
    """

    monkeypatch.setenv("RAY_health_check_failure_threshold", "1")
    monkeypatch.setenv("RAY_health_check_timeout_ms", "1000")
    monkeypatch.setenv("RAY_health_check_period_ms", "1000")
    monkeypatch.setenv("RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S", "0.01")

    params = getattr(request, "param") if hasattr(request, "param") else None
    cluster = AutoscalingCluster(
        **{
            "head_resources": {"CPU": 0},
            "worker_node_types": {
                "cpu_node": {
                    "resources": {"CPU": 3},
                    "node_config": {},
                    "min_workers": 0,
                    "max_workers": 10,
                },
            },
            "idle_timeout_minutes": 0.01,
            "autoscaler_v2": True,
        }
    )
    cluster.start()
    ray.init()
    serve.start()
    client = _get_global_client()
    dep_id = DeploymentID(name="BlockInit", app_name="A")

    # Start with replica initializations unblocked
    signal = SignalActor.options(name="signal123").remote()
    signal.send.remote()

    # Application A: 1-cpu replicas
    # Application B: 2-cpu replicas
    import_path = "ray.anyscale.serve.tests.test_deployment_scheduler.app_B"
    config = {
        "applications": [
            {
                "name": "A",
                "import_path": import_path,
                "route_prefix": "/a",
                "deployments": [
                    params[0]
                    if params
                    else {
                        "name": "BlockInit",
                        "num_replicas": 1,
                        "ray_actor_options": {"num_cpus": 1},
                        "health_check_period_s": 1,
                        "graceful_shutdown_timeout_s": 1,
                    }
                ],
            },
            {
                "name": "B",
                "import_path": import_path,
                "route_prefix": "/b",
                "deployments": [
                    params[1]
                    if params
                    else {
                        "name": "BlockInit",
                        "num_replicas": 1,
                        "ray_actor_options": {"num_cpus": 2},
                        "health_check_period_s": 1,
                    }
                ],
            },
        ]
    }

    client.deploy_apps(ServeDeploySchema(**config))
    client._wait_for_application_running("A")
    client._wait_for_application_running("B")
    # Node1: (B, A1)
    wait_for_condition(check_num_alive_nodes, target=2)  # 1 head + 1 worker node

    config["applications"][0]["deployments"][0]["num_replicas"] = 6
    client.deploy_apps(ServeDeploySchema(**config))
    client._wait_for_application_running("A")
    # Node1: (B, A1) -> 0 CPUs remaining
    # Node2: (A2, A3, A4) -> 0 CPUs remaining
    # Node3: (A5, A6) -> 1 CPUs remaining
    wait_for_condition(check_num_alive_nodes, target=4)  # 1 head + 3 worker node

    # Delete app2. We should try to compact node 1, but the new
    # replacement replica is blocked on init so A1 should stay in
    # PENDING_MIGRATION and node 1 cannot become idle
    signal.send.remote(clear=True)
    del config["applications"][1]
    client.deploy_apps(ServeDeploySchema(**config))
    wait_for_condition(
        check_replica_counts,
        controller=client._controller,
        deployment_id=dep_id,
        total=7,
        by_state=[
            (ReplicaState.RUNNING, 5, None),
            (ReplicaState.STARTING, 1, None),
            (ReplicaState.PENDING_MIGRATION, 1, None),
        ],
    )
    check_num_alive_nodes(4)  # 1 head + 3 worker node

    yield client, config, signal

    serve.shutdown()
    ray.shutdown()
    cluster.shutdown()


@pytest.mark.skipif(
    not RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY, reason="Needs compact strategy."
)
class TestCompactScheduling:
    def test_e2e_compact_node_basic(self, setup_compact_scheduling):
        _, _, signal = setup_compact_scheduling

        # Unblock replica initialization, the compaction should complete
        # The remaining idle node should get killed and the cluster
        # downscales to 2 worker nodes
        signal.send.remote()
        wait_for_condition(check_num_alive_nodes, target=3)  # 1 head + 2 worker nodes

    @pytest.mark.parametrize(
        "setup_compact_scheduling",
        [
            (
                {
                    "name": "BlockInit",
                    "num_replicas": 1,
                    "ray_actor_options": {"num_cpus": 0},
                    "placement_group_bundles": [{"CPU": 0.5}, {"CPU": 0.5}],
                    "placement_group_strategy": "STRICT_PACK",
                },
                {
                    "name": "BlockInit",
                    "num_replicas": 1,
                    "ray_actor_options": {"num_cpus": 0},
                    "placement_group_bundles": [{"CPU": 1.5}, {"CPU": 0.5}],
                    "placement_group_strategy": "STRICT_PACK",
                },
            )
        ],
        indirect=True,
    )
    def test_e2e_placement_group(self, setup_compact_scheduling):
        _, _, signal = setup_compact_scheduling

        # Unblock replica initialization, the compaction should complete
        # The remaining idle node should get killed and the cluster
        # downscales to 2 worker nodes
        signal.send.remote()
        wait_for_condition(check_num_alive_nodes, target=3)  # 1 head + 2 worker nodes

    def test_downscale_during_compaction(self, setup_compact_scheduling):
        """Test replicas downscale while a node is being comopacted.

        1. Start with 6 (1CPU) replicas spread across 3 nodes.
        2. Attempt to compact one node with start-then-stop migration.
        3. Before the new replacement replica starts, downscale target
           `num_replicas` from 6 to 5.
        4. Both the PENDING_MIGRATION and new replacement STARTING
           replicas should be stopped, and the 5 RUNNING replicas should
           be untouched.
        """
        client, config, _ = setup_compact_scheduling
        dep_id = DeploymentID(name="BlockInit", app_name="A")

        running_replicas = get_current_replica_ids(dep_id, states=["RUNNING"])

        # Downscale from 6 -> 5 replicas
        config["applications"][0]["deployments"][0]["num_replicas"] = 5
        client.deploy_apps(ServeDeploySchema(**config))

        wait_for_condition(
            check_replica_counts,
            controller=client._controller,
            deployment_id=DeploymentID(name="BlockInit", app_name="A"),
            total=5,
            by_state=[(ReplicaState.RUNNING, 5, None)],
        )
        assert get_current_replica_ids(dep_id, states=["RUNNING"]) == running_replicas
        wait_for_condition(check_num_alive_nodes, target=3)  # 1 head + 2 worker node

    def test_upscale_during_compaction(self, setup_compact_scheduling):
        """Test replicas upscale while a node is being comopacted.

        1. Start with 6 (1CPU) replicas spread across 3 nodes.
        2. Attempt to compact one node with start-then-stop migration.
        3. Before the new replacement replica starts, upscale target
           `num_replicas` from 6 to 7.
        4. Since compacting down to 2 nodes is no longer possible, the
           previous in-progress node compaction should be cancelled and
           deployment should become HEALTHY with 7 running replicas.
        """

        # another test: Next event loop: A2 died. A8 should start on A234 node
        client, config, signal = setup_compact_scheduling
        dep_id = DeploymentID(name="BlockInit", app_name="A")

        running_replicas = get_current_replica_ids(dep_id, states=["RUNNING"])

        # Upscale from 6 -> 7 replicas
        config["applications"][0]["deployments"][0]["num_replicas"] = 7
        client.deploy_apps(ServeDeploySchema(**config))

        # Unblock replica initializations
        signal.send.remote()
        wait_for_condition(
            check_replica_counts,
            controller=client._controller,
            deployment_id=DeploymentID(name="BlockInit", app_name="A"),
            total=7,
            by_state=[(ReplicaState.RUNNING, 7, None)],
        )
        assert running_replicas < get_current_replica_ids(dep_id, states=["RUNNING"])
        check_num_alive_nodes(4)  # 1 head + 3 worker node

    def test_controller_crashes(self, setup_compact_scheduling):
        client, _, signal = setup_compact_scheduling
        dep_id = DeploymentID(name="BlockInit", app_name="A")

        h = serve.get_app_handle("A")
        pids = [h.get_pid.remote().result() for _ in range(30)]

        # Controller crashes
        kill_controller_and_wait_for_restart(client._controller)

        # When the controller recovers, it should recover 6 RUNNING
        # replicas and 1 STARTING replica. Then it should stop one
        # replica to match `target_num_replicas=6`.
        #
        # Then it should promptly identify the same compaction that was
        # in-progress before and try to compact Node3 (with 1 replica on it)
        wait_for_condition(
            check_replica_counts,
            controller=client._controller,
            deployment_id=dep_id,
            total=7,
            by_state=[
                (ReplicaState.RUNNING, 5, None),
                (ReplicaState.STARTING, 1, None),
                (ReplicaState.PENDING_MIGRATION, 1, None),
            ],
        )
        check_num_alive_nodes(4)  # 1 head + 3 worker node

        # The same set of replicas should be RUNNING/PENDING_MIGRATION
        # from before the controller crashed
        new_pids = [h.get_pid.remote().result() for _ in range(30)]
        assert set(pids) == set(new_pids)

        # Unblock all new starting replica initializations
        signal.send.remote()
        wait_for_condition(
            check_deployment_status,
            name="BlockInit",
            app_name="A",
            expected_status=DeploymentStatus.HEALTHY,
            timeout=20,
        )
        wait_for_condition(check_num_alive_nodes, target=3)  # 1 head + 2 worker node

    def test_worker_node_crashes(self, setup_compact_scheduling):
        _, _, signal = setup_compact_scheduling

        # One of the worker nodes crashes
        node_to_kill = random.choice(
            [
                node
                for node in ray.nodes()
                if not node["Resources"].get("node:__internal_head__")
            ]
        )
        print("killing worker node", node_to_kill, time.time())
        kill_raylet(node_to_kill)

        # At least 1 RUNNING/PENDING_MIGRATION replica was on the worker
        # node that crashed, since compaction was in-progress/blocked:
        # -> health checks should fail
        # -> deployment should transition to UNHEALTHY
        # This should happen quickly since health check period is 1s.
        wait_for_condition(
            check_deployment_status,
            name="BlockInit",
            app_name="A",
            expected_status=DeploymentStatus.UNHEALTHY,
        )

        # This should also happen quickly, health check period/timeout
        # for nodes has been dropped to 1s.
        wait_for_condition(check_node_dead, node_id=node_to_kill["NodeID"])

        # Unblock all new starting replica initializations
        signal.send.remote()
        wait_for_condition(
            check_deployment_status,
            name="BlockInit",
            app_name="A",
            expected_status=DeploymentStatus.HEALTHY,
        )
        wait_for_condition(check_num_alive_nodes, target=3)  # 1 head + 2 worker node

    @pytest.mark.parametrize("use_pg", [True, False])
    def test_custom_resources(self, ray_cluster: Cluster, use_pg: bool):
        """Test that custom resources are taken into consideration when identifying
        node compaction opportunities."""

        depA_id = DeploymentID(name="BlockInit", app_name="A")
        depB_id = DeploymentID(name="BlockInit", app_name="B")

        # Setup cluster and start serve
        cluster = ray_cluster
        cluster.add_node(num_cpus=0)  # Head node
        cluster.add_node(num_cpus=2, resources={"worker1": 1})
        cluster.add_node(num_cpus=2, resources={"worker2": 1})
        cluster.wait_for_nodes()
        node1 = ray.get(get_node_id.options(resources={"worker1": 1}).remote())
        node2 = ray.get(get_node_id.options(resources={"worker2": 1}).remote())
        cluster.connect(namespace=SERVE_NAMESPACE)
        serve.start()
        client = _get_global_client()

        # Start with replica initializations unblocked
        signal = SignalActor.options(name="signal123").remote()
        signal.send.remote()

        # Deploy normal application A with 3 replicas
        import_path = "ray.anyscale.serve.tests.test_deployment_scheduler.app_B"
        config = {
            "applications": [
                {
                    "name": "A",
                    "import_path": import_path,
                    "route_prefix": "/a",
                    "deployments": [
                        {
                            "name": "BlockInit",
                            "num_replicas": 3,
                            "ray_actor_options": {"num_cpus": 0 if use_pg else 1},
                        }
                    ],
                },
            ]
        }
        if use_pg:
            config["applications"][0]["deployments"][0]["placement_group_bundles"] = [
                {"CPU": 0.5},
                {"CPU": 0.5},
            ]
            config["applications"][0]["deployments"][0][
                "placement_group_strategy"
            ] = "STRICT_PACK"

        client.deploy_apps(ServeDeploySchema(**config))
        client._wait_for_application_running("A")
        hA = serve.get_app_handle("A")
        # node1: 2 replicas, node2: 1 replica (or vice versa)
        assert {hA.remote().result() for _ in range(30)} == {node1, node2}

        # Add a new `node3` with custom resource `customz`
        cluster.add_node(num_cpus=1, resources={"worker3": 1, "customz": 1})
        cluster.wait_for_nodes()
        node3 = ray.get(get_node_id.options(resources={"worker3": 1}).remote())

        # Deploy application B with 1 replica that requires custom resource `customz`
        config["applications"].append(
            {
                "name": "B",
                "import_path": import_path,
                "route_prefix": "/b",
                "deployments": [
                    {
                        "name": "BlockInit",
                        "ray_actor_options": {
                            "num_cpus": 0 if use_pg else 1,
                            "resources": {} if use_pg else {"customz": 0.1},
                        },
                    }
                ],
            }
        )
        if use_pg:
            config["applications"][1]["deployments"][0]["placement_group_bundles"] = [
                {"CPU": 0.5},
                {"CPU": 0.5},
                {"customz": 0.1},
            ]
            config["applications"][1]["deployments"][0][
                "placement_group_strategy"
            ] = "STRICT_PACK"

        client.deploy_apps(ServeDeploySchema(**config))
        client._wait_for_application_running("B")
        hB = serve.get_app_handle("B")
        # B's 1 replica should be scheduled on `node3`
        assert {hB.remote().result() for _ in range(10)} == {node3}

        # Block new replicas
        signal.send.remote(clear=True)

        # Deployment scheduler should not identify any compaction opportunities
        def any_starting_or_pending_migration_replicas():
            replicas_A = ray.get(
                client._controller._dump_replica_states_for_testing.remote(depA_id)
            )
            replicas_B = ray.get(
                client._controller._dump_replica_states_for_testing.remote(depB_id)
            )
            rA = replicas_A.get([ReplicaState.STARTING, ReplicaState.PENDING_MIGRATION])
            rB = replicas_B.get([ReplicaState.STARTING, ReplicaState.PENDING_MIGRATION])
            assert len(rA) > 0 or len(rB) > 0
            print("A replicas:", replicas_A._replicas)
            print("B replicas:", replicas_B._replicas)
            return True

        with pytest.raises(RuntimeError):
            wait_for_condition(any_starting_or_pending_migration_replicas)

        serve.shutdown()

    @pytest.mark.parametrize(
        "autoscaling_cluster",
        [
            {
                "head_resources": {"CPU": 0},
                "worker_node_types": {
                    "cpu_node1": {
                        "resources": {"CPU": 4},
                        "node_config": {},
                        "min_workers": 0,
                        "max_workers": 1,
                    },
                    "cpu_node2": {
                        "resources": {"CPU": 5},
                        "node_config": {},
                        "min_workers": 0,
                        "max_workers": 1,
                    },
                },
                "idle_timeout_minutes": 0.05,
            },
        ],
        indirect=True,
    )
    def test_prefer_larger_nodes(self, autoscaling_cluster: AutoscalingCluster):
        client = _get_global_client()
        signal = SignalActor.options(name="signal123").remote()
        signal.send.remote()

        import_path = "ray.anyscale.serve.tests.test_deployment_scheduler.app_B"
        config = {
            "applications": [
                {
                    "name": "A",
                    "import_path": import_path,
                    "route_prefix": "/a",
                    "deployments": [
                        {"name": "BlockInit", "ray_actor_options": {"num_cpus": 1.5}}
                    ],
                },
                {
                    "name": "B",
                    "import_path": import_path,
                    "route_prefix": "/b",
                    "deployments": [
                        {"name": "BlockInit", "ray_actor_options": {"num_cpus": 2.5}}
                    ],
                },
            ]
        }

        # Make sure A(1CPU) + B(2CPU) is scheduled on worker node #1
        client.deploy_apps(ServeDeploySchema(**config))
        wait_for_condition(check_num_alive_nodes, target=2)  # 1 head + 1 worker node

        # THen second A(1CPU) is scheduled by itself on worker node #2
        config["applications"][0]["deployments"][0]["num_replicas"] = 2
        client.deploy_apps(ServeDeploySchema(**config))
        wait_for_condition(check_num_alive_nodes, target=3)  # 1 head + 2 worker nodes

        client._wait_for_application_running("A")
        h = serve.get_app_handle("A")
        node_ids = {h.remote().result() for _ in range(20)}
        assert len(set(node_ids)) == 2

        # Deleting B should allow cluster to compact down to 1 worker node
        del config["applications"][1]
        client.deploy_apps(ServeDeploySchema(**config))
        wait_for_condition(check_num_alive_nodes, target=2)  # 1 head + 1 worker nodes

        # The node that was compacted should have been of type cpu_node2 (5CPUs)
        # So the remaining node should be of type cpu_node1 (4CPUs)
        worker_node = [
            node
            for node in ray.nodes()
            if not node["Resources"].get("node:__internal_head__")
        ][0]
        assert worker_node["Resources"]["CPU"] == 4.0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
