import sys
from collections import defaultdict

import pytest

import ray
from ray import serve
from ray.cluster_utils import AutoscalingCluster
from ray.serve.drivers import DAGDriver
from ray.util.state import list_actors


def get_node_to_deployment_to_num_replicas():
    actors = list_actors()
    print(actors)
    # {node_id: {deployment_name: num_replicas}}
    node_to_deployment_to_num_replicas = defaultdict(dict)
    for actor in actors:
        if "app#deploy" not in actor["name"] or actor["state"] != "ALIVE":
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


def test_basic():
    """Test that max_replicas_per_node is honored."""

    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "cpu_node": {
                "resources": {
                    "CPU": 9999,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 100,
            },
        },
    )

    cluster.start()
    ray.init()

    @serve.deployment
    class D:
        def __call__(self):
            return "hello"

    deployments = {
        "/deploy1": D.options(
            num_replicas=6, max_replicas_per_node=3, name="deploy1"
        ).bind(),
        "/deploy2": D.options(
            num_replicas=2, max_replicas_per_node=1, name="deploy2"
        ).bind(),
    }
    serve.run(DAGDriver.bind(deployments), name="app")

    # 2 worker nodes should be started.
    # Each worker node should run 3 deploy1 replicas
    # and 1 deploy2 replicas.
    assert len(ray.nodes()) == 3
    node_to_deployment_to_num_replicas = get_node_to_deployment_to_num_replicas()

    assert len(node_to_deployment_to_num_replicas) == 2
    for _, deployment_to_num_replicas in node_to_deployment_to_num_replicas.items():
        assert deployment_to_num_replicas["deploy1"] == 3
        assert deployment_to_num_replicas["deploy2"] == 1

    serve.shutdown()
    ray.shutdown()
    cluster.shutdown()


def test_update_max_replicas_per_node():
    """Test re-deploying a deployment with different max_replicas_per_node."""

    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "cpu_node": {
                "resources": {
                    "CPU": 9999,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 100,
            },
        },
    )

    cluster.start()
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

    assert len(ray.nodes()) == 3
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

    assert len(ray.nodes()) == 4
    node_to_deployment_to_num_replicas = get_node_to_deployment_to_num_replicas()

    assert len(node_to_deployment_to_num_replicas) == 3
    for _, deployment_to_num_replicas in node_to_deployment_to_num_replicas.items():
        # Every node has 1 replica.
        assert deployment_to_num_replicas["deploy1"] == 1

    serve.shutdown()
    ray.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
