import os
import sys
import pytest
import subprocess

import ray


def check_cmd_stderr(cmd):
    return subprocess.run(cmd, stderr=subprocess.PIPE).stderr.decode("utf-8")


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

    key = "ray.io/node_id"
    with pytest.raises(
        ValueError,
        match=f"Custom label keys `{key}` cannot start with the prefix `ray.io/`",
    ):
        cluster.add_node(num_cpus=1, labels={key: "111111"})

    key = "ray.io/other_key"
    with pytest.raises(
        ValueError,
        match=f"Custom label keys `{key}` cannot start with the prefix `ray.io/`",
    ):
        ray.init(labels={key: "value"})

    cluster.add_node(num_cpus=1)
    with pytest.raises(ValueError, match="labels must not be provided"):
        ray.init(address=cluster.address, labels={"gpu_type": "A100"})

    with pytest.raises(ValueError, match="labels must not be provided"):
        ray.init(labels={"gpu_type": "A100"})


def test_ray_start_set_node_labels_value_error():
    out = check_cmd_stderr(["ray", "start", "--head", "--labels=xxx"])
    assert "is not a valid JSON string, detail error" in out

    out = check_cmd_stderr(["ray", "start", "--head", '--labels={"gpu_type":1}'])
    assert 'The value of the "gpu_type" is not string type' in out

    out = check_cmd_stderr(
        ["ray", "start", "--head", '--labels={"ray.io/node_id":"111"}']
    )
    assert "cannot start with the prefix `ray.io/`" in out

    out = check_cmd_stderr(
        ["ray", "start", "--head", '--labels={"ray.io/other_key":"111"}']
    )
    assert "cannot start with the prefix `ray.io/`" in out


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


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
