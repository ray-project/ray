import os
import sys
import pytest
import subprocess
import tempfile

import ray
from ray.cluster_utils import AutoscalingCluster
from ray._common.test_utils import wait_for_condition


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
def test_ray_start_set_node_labels_from_json(call_ray_start):
    ray.init(address=call_ray_start)
    node_info = ray.nodes()[0]
    assert node_info["Labels"] == add_default_labels(
        node_info, {"gpu_type": "A100", "region": "us"}
    )


@pytest.mark.parametrize(
    "call_ray_start",
    ['ray start --head --labels "gpu_type=A100,region=us"'],
    indirect=True,
)
def test_ray_start_set_node_labels_from_string(call_ray_start):
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

    with pytest.raises(ValueError, match="labels must not be provided"):
        ray.init(labels={"gpu_type": "A100"})


def test_ray_start_set_node_labels_value_error():
    out = check_cmd_stderr(["ray", "start", "--head", "--labels=xxx"])
    assert "Label string is not a key-value pair." in out

    out = check_cmd_stderr(["ray", "start", "--head", '--labels={"gpu_type":1}'])
    assert "Label string is not a key-value pair." in out

    out = check_cmd_stderr(
        ["ray", "start", "--head", '--labels={"ray.io/node_id":"111"}']
    )
    assert "Label string is not a key-value pair" in out

    out = check_cmd_stderr(
        ["ray", "start", "--head", '--labels={"ray.io/other_key":"111"}']
    )
    assert "Label string is not a key-value pair" in out


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


@pytest.mark.parametrize("autoscaler_v2", [False, True], ids=["v1", "v2"])
def test_autoscaler_set_node_labels(autoscaler_v2, shutdown_only):
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
        autoscaler_v2=autoscaler_v2,
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


def test_ray_start_set_node_labels_from_file():
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as test_file:
        test_file.write('"gpu_type": "A100"\n"region": "us"\n"market-type": "spot"')
        test_file_path = test_file.name

    try:
        cmd = ["ray", "start", "--head", "--labels-file", test_file_path]
        subprocess.check_call(cmd)
        ray.init(address="auto")
        node_info = ray.nodes()[0]
        assert node_info["Labels"] == add_default_labels(
            node_info, {"gpu_type": "A100", "region": "us", "market-type": "spot"}
        )
    finally:
        subprocess.check_call(["ray", "stop", "--force"])
        os.remove(test_file_path)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
