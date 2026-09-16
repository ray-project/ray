import os
import random
import sys
import time
from copy import deepcopy
from typing import List, Optional, Set

import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._common.usage import usage_lib
from ray._private.test_utils import kill_raylet
from ray.cluster_utils import AutoscalingCluster, Cluster
from ray.serve._private.common import (
    DeploymentID,
    DeploymentStatus,
    ReplicaState,
)
from ray.serve._private.constants import (
    RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY,
    SERVE_CONTROLLER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.test_utils import (
    TELEMETRY_ROUTE_PREFIX,
    check_deployment_status,
    check_num_alive_nodes,
    check_replica_counts,
    check_telemetry,
    get_node_id,
    start_telemetry_app,
)
from ray.serve._private.usage import ServeUsageTag
from ray.serve.context import _get_global_client
from ray.serve.schema import ServeDeploySchema
from ray.tests.conftest import *  # noqa
from ray.util.state import list_actors


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
APP_B_IMPORT_PATH = "ray.serve.tests.test_node_compaction.app_B"


@serve.deployment
class Noop:
    pass


app_noop = Noop.bind()
APP_NOOP_IMPORT_PATH = "ray.serve.tests.test_node_compaction.app_noop"

# AutoscalingCluster pops entries from head_resources, so always pass a copy.
CPU_NODE_AUTOSCALING_CONFIG = {
    "head_resources": {"CPU": 0},
    "worker_node_types": {
        "cpu_node": {
            "resources": {"CPU": 3},
            "node_config": {},
            "min_workers": 0,
            "max_workers": 10,
        },
    },
    # Short enough to release compacted nodes quickly, long enough that a fresh
    # node isn't killed before Serve places a replica on it.
    "idle_timeout_minutes": 0.05,
    "autoscaler_v2": True,
}


def check_node_dead(node_id: str):
    target_node = [node for node in ray.nodes() if node["NodeID"] == node_id][0]
    assert not target_node["Alive"]
    return True


def get_controller_pid() -> Optional[int]:
    for actor in list_actors(filters=[("state", "=", "ALIVE")]):
        if actor["name"] == SERVE_CONTROLLER_NAME:
            return actor["pid"]


def kill_controller_and_wait_for_restart(controller):
    old_pid = get_controller_pid()
    ray.kill(controller, no_restart=False)
    wait_for_condition(lambda: get_controller_pid() != old_pid)


def get_current_replica_ids(
    deployment_id: DeploymentID, states: List[ReplicaState] = None
) -> Set[str]:
    details = _get_global_client().get_serve_details()
    app = details["applications"][deployment_id.app_name]
    replicas = app["deployments"][deployment_id.name]["replicas"]
    return {r["replica_id"] for r in replicas if not states or r["state"] in states}


def worker_nodes():
    return [
        node
        for node in ray.nodes()
        if not node["Resources"].get("node:__internal_head__")
    ]


@pytest.fixture
def autoscaling_cluster(request, monkeypatch):
    monkeypatch.setenv("RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S", "0.01")
    params = getattr(request, "param", None) or CPU_NODE_AUTOSCALING_CONFIG
    cluster = AutoscalingCluster(**deepcopy(params))
    cluster.start()
    ray.init()
    serve.start()
    yield
    serve.shutdown()
    ray.shutdown()
    cluster.shutdown()
    usage_lib.reset_global_state()


@pytest.fixture
def autoscaling_cluster_with_telemetry(request, monkeypatch):
    monkeypatch.setenv("RAY_USAGE_STATS_ENABLED", "1")
    monkeypatch.setenv(
        "RAY_USAGE_STATS_REPORT_URL", f"http://127.0.0.1:8000{TELEMETRY_ROUTE_PREFIX}"
    )
    monkeypatch.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "1")
    monkeypatch.setenv("RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S", "0.01")
    cluster = AutoscalingCluster(**deepcopy(CPU_NODE_AUTOSCALING_CONFIG))

    def teardown():
        serve.shutdown()
        ray.shutdown()
        cluster.shutdown()
        usage_lib.reset_global_state()

    request.addfinalizer(teardown)
    cluster.start()
    if ray.is_initialized():
        ray.shutdown()
    ray.init()
    serve.start()
    storage_handle = start_telemetry_app()
    wait_for_condition(
        lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
    )
    yield


@pytest.fixture
def setup_compact_scheduling(request, monkeypatch):
    """Ends with node 1 holding one 1-CPU replica whose migration is blocked."""
    # Detect a killed raylet within a few seconds. Tighter values mark healthy
    # nodes dead under CI load.
    monkeypatch.setenv("RAY_health_check_failure_threshold", "1")
    monkeypatch.setenv("RAY_health_check_timeout_ms", "2000")
    monkeypatch.setenv("RAY_health_check_period_ms", "3000")
    monkeypatch.setenv("RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S", "0.01")

    params = getattr(request, "param", None)
    cluster = AutoscalingCluster(**deepcopy(CPU_NODE_AUTOSCALING_CONFIG))

    def teardown():
        serve.shutdown()
        ray.shutdown()
        cluster.shutdown()

    # Clean up even when setup fails. A leaked driver and cluster otherwise
    # break every later test in the file.
    request.addfinalizer(teardown)
    cluster.start()
    if ray.is_initialized():
        ray.shutdown()
    ray.init()
    serve.start()
    client = _get_global_client()
    dep_id = DeploymentID(name="BlockInit", app_name="A")

    signal = SignalActor.options(name="signal123").remote()
    signal.send.remote()

    config = {
        "applications": [
            {
                "name": "A",
                "import_path": APP_B_IMPORT_PATH,
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
                "import_path": APP_B_IMPORT_PATH,
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
    wait_for_condition(check_num_alive_nodes, target=2, timeout=60)

    config["applications"][0]["deployments"][0]["num_replicas"] = 6
    client.deploy_apps(ServeDeploySchema(**config))
    client._wait_for_application_running("A")
    # Node1: (B, A1), Node2: (A2, A3, A4), Node3: (A5, A6)
    wait_for_condition(check_num_alive_nodes, target=4, timeout=60)

    # Deleting B makes node 1 compactable, but the replacement blocks on init.
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
    wait_for_condition(check_num_alive_nodes, target=4, timeout=60)

    yield client, config, signal


@pytest.mark.skipif(
    not RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY, reason="Needs pack strategy."
)
class TestCompactScheduling:
    def test_e2e_compact_node_basic(self, setup_compact_scheduling):
        _, _, signal = setup_compact_scheduling

        signal.send.remote()
        wait_for_condition(check_num_alive_nodes, target=3, timeout=60)

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

        signal.send.remote()
        wait_for_condition(check_num_alive_nodes, target=3, timeout=60)

    def test_downscale_during_compaction(self, setup_compact_scheduling):
        client, config, _ = setup_compact_scheduling
        dep_id = DeploymentID(name="BlockInit", app_name="A")

        running_replicas = get_current_replica_ids(dep_id, states=["RUNNING"])

        config["applications"][0]["deployments"][0]["num_replicas"] = 5
        client.deploy_apps(ServeDeploySchema(**config))

        # Both the PENDING_MIGRATION and its STARTING replacement get stopped.
        wait_for_condition(
            check_replica_counts,
            controller=client._controller,
            deployment_id=dep_id,
            total=5,
            by_state=[(ReplicaState.RUNNING, 5, None)],
        )
        assert get_current_replica_ids(dep_id, states=["RUNNING"]) == running_replicas
        wait_for_condition(check_num_alive_nodes, target=3, timeout=60)

    def test_upscale_during_compaction(self, setup_compact_scheduling):
        client, config, signal = setup_compact_scheduling
        dep_id = DeploymentID(name="BlockInit", app_name="A")

        running_replicas = get_current_replica_ids(dep_id, states=["RUNNING"])

        config["applications"][0]["deployments"][0]["num_replicas"] = 7
        client.deploy_apps(ServeDeploySchema(**config))

        # 7 replicas no longer fit on 2 nodes, so the compaction is cancelled.
        signal.send.remote()
        wait_for_condition(
            check_replica_counts,
            controller=client._controller,
            deployment_id=dep_id,
            total=7,
            by_state=[(ReplicaState.RUNNING, 7, None)],
        )
        assert running_replicas < get_current_replica_ids(dep_id, states=["RUNNING"])
        wait_for_condition(check_num_alive_nodes, target=4, timeout=60)

    def test_controller_crashes(self, setup_compact_scheduling):
        client, _, signal = setup_compact_scheduling
        dep_id = DeploymentID(name="BlockInit", app_name="A")

        h = serve.get_app_handle("A")
        pids = [h.get_pid.remote().result() for _ in range(30)]

        kill_controller_and_wait_for_restart(client._controller)

        # The restarted controller recovers 6 RUNNING + 1 STARTING and stops one
        # to get back to the target. Recovery timing decides whether it stops
        # the blocked replacement and re-identifies the compaction, or stops
        # the node 1 replica outright. Either way exactly one replica stays
        # blocked in init and nothing else is in flux.
        def settled_after_restart():
            states = [
                r["state"]
                for r in _get_global_client().get_serve_details()["applications"]["A"][
                    "deployments"
                ]["BlockInit"]["replicas"]
            ]
            assert states.count("STARTING") == 1, states
            assert set(states) <= {"STARTING", "RUNNING", "PENDING_MIGRATION"}, states
            return True

        wait_for_condition(settled_after_restart, timeout=60)

        # Requests are still served only by replicas that were running before.
        new_pids = [h.get_pid.remote().result() for _ in range(30)]
        assert set(new_pids) <= set(pids)

        signal.send.remote()
        wait_for_condition(
            check_deployment_status,
            name="BlockInit",
            app_name="A",
            expected_status=DeploymentStatus.HEALTHY,
            timeout=60,
        )
        wait_for_condition(
            check_replica_counts,
            controller=client._controller,
            deployment_id=dep_id,
            total=6,
            by_state=[(ReplicaState.RUNNING, 6, None)],
        )
        wait_for_condition(check_num_alive_nodes, target=3, timeout=60)

    def test_worker_node_crashes(self, setup_compact_scheduling):
        _, _, signal = setup_compact_scheduling

        node_to_kill = random.choice(worker_nodes())
        print("killing worker node", node_to_kill, time.time())
        kill_raylet(node_to_kill)

        wait_for_condition(
            check_deployment_status,
            timeout=30,
            name="BlockInit",
            app_name="A",
            expected_status=DeploymentStatus.UNHEALTHY,
        )
        wait_for_condition(check_node_dead, timeout=30, node_id=node_to_kill["NodeID"])

        signal.send.remote()
        wait_for_condition(
            check_deployment_status,
            name="BlockInit",
            app_name="A",
            expected_status=DeploymentStatus.HEALTHY,
        )
        wait_for_condition(check_num_alive_nodes, target=3, timeout=60)

    @pytest.mark.parametrize("use_pg", [True, False])
    def test_custom_resources(self, ray_cluster: Cluster, use_pg: bool):
        depA_id = DeploymentID(name="BlockInit", app_name="A")
        depB_id = DeploymentID(name="BlockInit", app_name="B")

        cluster = ray_cluster
        cluster.add_node(num_cpus=0)
        cluster.add_node(num_cpus=2, resources={"worker1": 1})
        cluster.add_node(num_cpus=2, resources={"worker2": 1})
        cluster.wait_for_nodes()
        node1 = ray.get(get_node_id.options(resources={"worker1": 1}).remote())
        node2 = ray.get(get_node_id.options(resources={"worker2": 1}).remote())
        cluster.connect(namespace=SERVE_NAMESPACE)
        serve.start()
        client = _get_global_client()

        signal = SignalActor.options(name="signal123").remote()
        signal.send.remote()

        config = {
            "applications": [
                {
                    "name": "A",
                    "import_path": APP_B_IMPORT_PATH,
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
        assert {hA.remote().result() for _ in range(30)} == {node1, node2}

        cluster.add_node(num_cpus=1, resources={"worker3": 1, "customz": 1})
        cluster.wait_for_nodes()
        node3 = ray.get(get_node_id.options(resources={"worker3": 1}).remote())

        config["applications"].append(
            {
                "name": "B",
                "import_path": APP_B_IMPORT_PATH,
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
        assert {hB.remote().result() for _ in range(10)} == {node3}

        signal.send.remote(clear=True)

        # B needs `customz`, which only node3 has, so nothing is compactable.
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
                        "resources": {"CPU": 3},
                        "node_config": {},
                        "min_workers": 0,
                        "max_workers": 1,
                    },
                    "cpu_node2": {
                        "resources": {"CPU": 4},
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
    def test_prefer_larger_nodes(self, autoscaling_cluster):
        client = _get_global_client()
        signal = SignalActor.options(name="signal123").remote()
        signal.send.remote()

        config = {
            "applications": [
                {
                    "name": "A",
                    "import_path": APP_B_IMPORT_PATH,
                    "route_prefix": "/a",
                    "deployments": [
                        {"name": "BlockInit", "ray_actor_options": {"num_cpus": 1.5}}
                    ],
                },
                {
                    "name": "B",
                    "import_path": APP_B_IMPORT_PATH,
                    "route_prefix": "/b",
                    "deployments": [
                        {"name": "BlockInit", "ray_actor_options": {"num_cpus": 2.5}}
                    ],
                },
            ]
        }

        # A(1.5) + B(2.5) land on the 4-CPU node.
        client.deploy_apps(ServeDeploySchema(**config))
        client._wait_for_application_running("A")
        client._wait_for_application_running("B")
        wait_for_condition(check_num_alive_nodes, target=2, timeout=60)

        # The second A lands alone on the 3-CPU node.
        config["applications"][0]["deployments"][0]["num_replicas"] = 2
        client.deploy_apps(ServeDeploySchema(**config))
        wait_for_condition(check_num_alive_nodes, target=3, timeout=60)

        client._wait_for_application_running("A")
        h = serve.get_app_handle("A")
        assert len({h.remote().result() for _ in range(20)}) == 2

        # Deleting B leaves both nodes compactable; the 4-CPU one should go.
        del config["applications"][1]
        client.deploy_apps(ServeDeploySchema(**config))
        wait_for_condition(check_num_alive_nodes, target=2, timeout=60)
        assert worker_nodes()[0]["Resources"]["CPU"] == 3.0

    def test_label_selector_blocks_compaction(self, ray_cluster: Cluster):
        depA_id = DeploymentID(name="BlockInit", app_name="A")

        cluster = ray_cluster
        cluster.add_node(num_cpus=0)
        cluster.add_node(
            num_cpus=1, resources={"west": 1}, labels={"region": "us-west"}
        )
        cluster.add_node(
            num_cpus=2, resources={"east": 1}, labels={"region": "us-east"}
        )
        cluster.wait_for_nodes()
        node_west = ray.get(get_node_id.options(resources={"west": 1}).remote())
        node_east = ray.get(get_node_id.options(resources={"east": 1}).remote())
        cluster.connect(namespace=SERVE_NAMESPACE)
        serve.start()
        client = _get_global_client()

        signal = SignalActor.options(name="signal123").remote()
        signal.send.remote()

        config = {
            "applications": [
                {
                    "name": "A",
                    "import_path": APP_B_IMPORT_PATH,
                    "route_prefix": "/a",
                    "deployments": [
                        {
                            "name": "BlockInit",
                            "num_replicas": 1,
                            "ray_actor_options": {
                                "num_cpus": 1,
                                "label_selector": {"region": "us-west"},
                            },
                        }
                    ],
                },
                {
                    "name": "B",
                    "import_path": APP_B_IMPORT_PATH,
                    "route_prefix": "/b",
                    "deployments": [
                        {
                            "name": "BlockInit",
                            "num_replicas": 1,
                            "ray_actor_options": {
                                "num_cpus": 1,
                                "label_selector": {"region": "us-east"},
                            },
                        }
                    ],
                },
            ]
        }
        client.deploy_apps(ServeDeploySchema(**config))
        client._wait_for_application_running("A")
        client._wait_for_application_running("B")

        hA = serve.get_app_handle("A")
        hB = serve.get_app_handle("B")
        assert {hA.remote().result() for _ in range(10)} == {node_west}
        assert {hB.remote().result() for _ in range(10)} == {node_east}

        signal.send.remote(clear=True)

        # node-east has room but doesn't match A's selector.
        def any_starting_or_pending_migration_replicas():
            replicas_A = ray.get(
                client._controller._dump_replica_states_for_testing.remote(depA_id)
            )
            rA = replicas_A.get([ReplicaState.STARTING, ReplicaState.PENDING_MIGRATION])
            assert len(rA) > 0
            return True

        with pytest.raises(RuntimeError):
            wait_for_condition(any_starting_or_pending_migration_replicas)

        serve.shutdown()

    @pytest.mark.parametrize("use_pg_bundle_selector", [False, True])
    def test_label_selector_compaction_migrates_to_matching_node(
        self, ray_cluster: Cluster, use_pg_bundle_selector: bool
    ):
        cluster = ray_cluster
        cluster.add_node(num_cpus=0)
        cluster.add_node(
            num_cpus=1, resources={"west_small": 1}, labels={"region": "us-west"}
        )
        cluster.add_node(
            num_cpus=2, resources={"west_big": 1}, labels={"region": "us-west"}
        )
        cluster.add_node(
            num_cpus=1, resources={"east": 1}, labels={"region": "us-east"}
        )
        cluster.wait_for_nodes()
        node_west_small = ray.get(
            get_node_id.options(resources={"west_small": 1}).remote()
        )
        node_west_big = ray.get(get_node_id.options(resources={"west_big": 1}).remote())
        node_east = ray.get(get_node_id.options(resources={"east": 1}).remote())
        cluster.connect(namespace=SERVE_NAMESPACE)
        serve.start()
        client = _get_global_client()

        signal = SignalActor.options(name="signal123").remote()
        signal.send.remote()

        if use_pg_bundle_selector:
            deployment_a = {
                "name": "BlockInit",
                "num_replicas": 2,
                "ray_actor_options": {"num_cpus": 0},
                "placement_group_bundles": [{"CPU": 0.5}, {"CPU": 0.5}],
                "placement_group_strategy": "STRICT_PACK",
                "placement_group_bundle_label_selector": [{"region": "us-west"}],
                "health_check_period_s": 1,
                "graceful_shutdown_timeout_s": 1,
            }
        else:
            deployment_a = {
                "name": "BlockInit",
                "num_replicas": 2,
                "ray_actor_options": {
                    "num_cpus": 1,
                    "label_selector": {"region": "us-west"},
                },
                "health_check_period_s": 1,
                "graceful_shutdown_timeout_s": 1,
            }
        # E keeps node-east non-idle with room, to prove A never migrates there.
        config = {
            "applications": [
                {
                    "name": "A",
                    "import_path": APP_B_IMPORT_PATH,
                    "route_prefix": "/a",
                    "deployments": [deployment_a],
                },
                {
                    "name": "E",
                    "import_path": APP_B_IMPORT_PATH,
                    "route_prefix": "/e",
                    "deployments": [
                        {
                            "name": "BlockInit",
                            "num_replicas": 1,
                            "ray_actor_options": {
                                "num_cpus": 0,
                                "resources": {"east": 0.1},
                                "label_selector": {"region": "us-east"},
                            },
                        }
                    ],
                },
            ]
        }
        client.deploy_apps(ServeDeploySchema(**config))
        client._wait_for_application_running("A")
        client._wait_for_application_running("E")

        hA = serve.get_app_handle("A")
        assert {hA.remote().result() for _ in range(20)} == {
            node_west_small,
            node_west_big,
        }

        def both_replicas_on_west_big():
            return {hA.remote().result() for _ in range(20)} == {node_west_big}

        wait_for_condition(both_replicas_on_west_big, timeout=90)
        assert node_east not in {hA.remote().result() for _ in range(20)}

        serve.shutdown()

    def test_node_compaction_telemetry(self, autoscaling_cluster_with_telemetry):
        client = _get_global_client()
        check_telemetry(ServeUsageTag.NUM_NODE_COMPACTIONS, expected=None)

        config = {
            "applications": [
                {
                    "name": "A",
                    "import_path": APP_NOOP_IMPORT_PATH,
                    "route_prefix": "/a",
                    "deployments": [{"name": "Noop"}],
                },
                {
                    "name": "B",
                    "import_path": APP_NOOP_IMPORT_PATH,
                    "route_prefix": "/b",
                    "deployments": [
                        {"name": "Noop", "ray_actor_options": {"num_cpus": 2}}
                    ],
                },
            ]
        }

        client.deploy_apps(ServeDeploySchema(**config))
        client._wait_for_application_running("A")
        client._wait_for_application_running("B")
        wait_for_condition(check_num_alive_nodes, target=2, timeout=60)

        config["applications"][0]["deployments"][0]["num_replicas"] = 2
        client.deploy_apps(ServeDeploySchema(**config))
        client._wait_for_application_running("A")
        wait_for_condition(check_num_alive_nodes, target=3, timeout=60)

        del config["applications"][1]
        client.deploy_apps(ServeDeploySchema(**config))
        wait_for_condition(check_num_alive_nodes, target=2, timeout=60)

        wait_for_condition(
            check_telemetry, tag=ServeUsageTag.NUM_NODE_COMPACTIONS, expected="1"
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
