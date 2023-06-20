import os
import sys
import pytest

import ray


@pytest.mark.parametrize(
    "call_ray_start",
    ['ray start --head --labels={"gpu_type":"A100","region":"us"}'],
    indirect=True,
)
def test_ray_start_set_node_labels(call_ray_start):
    ray.init(address=call_ray_start)
    assert ray.nodes()[0]["Labels"] == {"gpu_type": "A100", "region": "us"}


@pytest.mark.parametrize(
    "call_ray_start",
    [
        "ray start --head --labels={}",
    ],
    indirect=True,
)
def test_ray_start_set_empty_node_labels(call_ray_start):
    ray.init(address=call_ray_start)
    assert ray.nodes()[0]["Labels"] == {}


def test_ray_init_set_node_labels(shutdown_only):
    labels = {"gpu_type": "A100", "region": "us"}
    ray.init(labels=labels)
    assert ray.nodes()[0]["Labels"] == labels
    ray.shutdown()
    ray.init(labels={})
    assert ray.nodes()[0]["Labels"] == {}


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
    assert ray.nodes()[0]["Labels"] == labels
    head_node_id = ray.nodes()[0]["NodeID"]

    cluster.add_node(num_cpus=1, labels={})
    cluster.wait_for_nodes()
    for node in ray.nodes():
        if node["NodeID"] != head_node_id:
            assert node["Labels"] == {}


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
