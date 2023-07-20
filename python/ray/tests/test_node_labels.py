import os
import sys
import pytest

import ray
from ray.cluster_utils import AutoscalingCluster
from ray._private.test_utils import wait_for_condition


def add_default_labels(node_info, labels):
    labels["ray.io/node_id"] = node_info["NodeID"]
    return labels


@pytest.mark.parametrize(
    "call_ray_start",
    ['ray start --head --labels={"gpu_type":"A100","region":"us"}'],
    indirect=True,
)
def test_ray_start_set_node_labels(call_ray_start):
    ray.init(address=call_ray_start)
    node_info = ray.nodes()[0]
    assert node_info["Labels"] == add_default_labels(
        node_info, {"gpu_type": "A100", "region": "us"}
    )


@pytest.mark.parametrize(
    "call_ray_start",
    [
        "ray start --head --labels={}",
    ],
    indirect=True,
)
def test_ray_start_set_empty_node_labels(call_ray_start):
    ray.init(address=call_ray_start)
    node_info = ray.nodes()[0]
    assert node_info["Labels"] == add_default_labels(node_info, {})


def test_ray_init_set_node_labels(shutdown_only):
    labels = {"gpu_type": "A100", "region": "us"}
    ray.init(labels=labels)
    node_info = ray.nodes()[0]
    assert node_info["Labels"] == add_default_labels(node_info, labels)
    ray.shutdown()
    ray.init(labels={})
    node_info = ray.nodes()[0]
    assert node_info["Labels"] == add_default_labels(node_info, {})


def test_ray_init_set_node_labels_value_error(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    with pytest.raises(ValueError, match="labels must not be provided"):
        ray.init(address=cluster.address, labels={"gpu_type": "A100"})


def test_cluster_add_node_with_labels(ray_start_cluster):
    labels = {"gpu_type": "A100", "region": "us"}
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, labels=labels)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    node_info = ray.nodes()[0]
    assert node_info["Labels"] == add_default_labels(node_info, labels)
    head_node_id = ray.nodes()[0]["NodeID"]

    cluster.add_node(num_cpus=1, labels={})
    cluster.wait_for_nodes()
    for node in ray.nodes():
        if node["NodeID"] != head_node_id:
            assert node["Labels"] == add_default_labels(node, {})


def test_autoscaler_set_node_labels(shutdown_only):
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "worker_1": {
                "resources": {"CPU": 1},
                "labels": {"region": "us"},
                "node_config": {},
                "min_workers": 1,
                "max_workers": 1,
            }
        },
    )

    try:
        cluster.start()
        ray.init()
        wait_for_condition(lambda: len(ray.nodes()) == 2)

        for node in ray.nodes():
            if node["Resources"].get("CPU", 0) == 1:
                assert node["Labels"] == add_default_labels(node, {"region": "us"})
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
