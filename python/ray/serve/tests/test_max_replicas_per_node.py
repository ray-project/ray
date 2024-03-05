import sys
from collections import defaultdict

import pytest

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve._private.constants import RAY_SERVE_EAGERLY_START_REPLACEMENT_REPLICAS
from ray.util.state import list_actors


def get_node_to_deployment_to_num_replicas():
    actors = list_actors()
    print(actors)
    # {node_id: {deployment_name: num_replicas}}
    node_to_deployment_to_num_replicas = defaultdict(dict)
    for actor in actors:
        if (
            not any(
                name in actor["name"]
                for name in ["app#deploy", "app1#deploy", "app2#deploy"]
            )
            or actor["state"] != "ALIVE"
        ):
            continue
        deployment_name = None
        if "deploy1" in actor["name"]:
            deployment_name = "deploy1"
        else:
            assert "deploy2" in actor["name"]
            deployment_name = "deploy2"
        node_to_deployment_to_num_replicas[actor["node_id"]][deployment_name] = (
            node_to_deployment_to_num_replicas[actor["node_id"]].get(deployment_name, 0)
            + 1
        )

    return node_to_deployment_to_num_replicas


def check_alive_nodes(expected: int):
    nodes = ray.nodes()
    alive_nodes = [node for node in nodes if node["Alive"]]
    assert len(alive_nodes) == expected
    return True


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Flaky on Windows due to https://github.com/ray-project/ray/issues/36926.",
)
@pytest.mark.parametrize(
    "ray_autoscaling_cluster",
    [
        {
            "head_resources": {"CPU": 0},
            "worker_node_types": {
                "cpu_node": {
                    "resources": {
                        "CPU": 9999,
                    },
                    "node_config": {},
                    "min_workers": 0,
                    "max_workers": 100,
                },
            },
            "autoscaler_v2": False,
        },
        {
            "head_resources": {"CPU": 0},
            "worker_node_types": {
                "cpu_node": {
                    "resources": {
                        "CPU": 9999,
                    },
                    "node_config": {},
                    "min_workers": 0,
                    "max_workers": 100,
                },
            },
            "autoscaler_v2": True,
        },
    ],
    indirect=True,
    ids=["v1", "v2"],
)
def test_basic(ray_autoscaling_cluster):
    """Test that max_replicas_per_node is honored."""

    ray.init()

    @serve.deployment
    class D:
        def __call__(self):
            return "hello"

    serve.run(
        D.options(num_replicas=6, max_replicas_per_node=3, name="deploy1").bind(),
        name="app1",
        route_prefix="/deploy1",
    )
    serve.run(
        D.options(num_replicas=2, max_replicas_per_node=1, name="deploy2").bind(),
        name="app2",
        route_prefix="/deploy2",
    )

    # 2 worker nodes should be started.
    # Each worker node should run 3 deploy1 replicas
    # and 1 deploy2 replicas.
    assert len(ray.nodes()) == 3
    node_to_deployment_to_num_replicas = get_node_to_deployment_to_num_replicas()

    assert len(node_to_deployment_to_num_replicas) == 2
    for _, deployment_to_num_replicas in node_to_deployment_to_num_replicas.items():
        assert deployment_to_num_replicas["deploy1"] == 3
        assert deployment_to_num_replicas["deploy2"] == 1


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Flaky on Windows due to https://github.com/ray-project/ray/issues/36926.",
)
@pytest.mark.parametrize(
    "ray_autoscaling_cluster",
    [
        {
            "head_resources": {"CPU": 0},
            "worker_node_types": {
                "cpu_node": {
                    "resources": {
                        "CPU": 9999,
                    },
                    "node_config": {},
                    "min_workers": 0,
                    "max_workers": 100,
                },
            },
            "autoscaler_v2": False,
        },
        {
            "head_resources": {"CPU": 0},
            "worker_node_types": {
                "cpu_node": {
                    "resources": {
                        "CPU": 9999,
                    },
                    "node_config": {},
                    "min_workers": 0,
                    "max_workers": 100,
                },
            },
            "autoscaler_v2": True,
        },
    ],
    indirect=True,
    ids=["v1", "v2"],
)
def test_update_max_replicas_per_node(ray_autoscaling_cluster):
    """Test re-deploying a deployment with different max_replicas_per_node."""

    ray.init()

    @serve.deployment
    class D:
        def __call__(self):
            return "hello"

    # Requires 2 worker nodes.
    serve.run(
        D.options(num_replicas=3, max_replicas_per_node=2, name="deploy1").bind(),
        name="app",
    )

    # Head + 2 worker nodes
    check_alive_nodes(expected=3)
    node_to_deployment_to_num_replicas = get_node_to_deployment_to_num_replicas()

    assert len(node_to_deployment_to_num_replicas) == 2
    # One node has 2 replicas and the other has 1 replica.
    for _, deployment_to_num_replicas in node_to_deployment_to_num_replicas.items():
        assert deployment_to_num_replicas["deploy1"] in {1, 2}

    # Redeploy, requires 3 worker nodes.
    serve.run(
        D.options(num_replicas=3, max_replicas_per_node=1, name="deploy1").bind(),
        name="app",
    )

    if not RAY_SERVE_EAGERLY_START_REPLACEMENT_REPLICAS:
        check_alive_nodes(expected=4)
    node_to_deployment_to_num_replicas = get_node_to_deployment_to_num_replicas()

    assert len(node_to_deployment_to_num_replicas) == 3
    for _, deployment_to_num_replicas in node_to_deployment_to_num_replicas.items():
        # Every node has 1 replica.
        assert deployment_to_num_replicas["deploy1"] == 1

    # Head + 3 worker nodes
    if RAY_SERVE_EAGERLY_START_REPLACEMENT_REPLICAS:
        # We wait for this to be satisfied at the end because there may be
        # more than 3 worker nodes after the deployment finishes deploying,
        # since replicas are being started and stopped at the same time, and
        # there is a strict max replicas per node requirement. However nodes
        # that were hosting the replicas of the old version should eventually
        # be removed from scale-down.
        wait_for_condition(check_alive_nodes, expected=4, timeout=60)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
