import os
import subprocess
import sys
import tempfile
from unittest.mock import patch

import pytest

import ray
import ray.experimental
from ray._common.test_utils import wait_for_condition
from ray._private.accelerators.tpu import TPUAcceleratorManager
from ray.cluster_utils import AutoscalingCluster
from ray.exceptions import GetTimeoutError


def check_cmd_stderr(cmd):
    return subprocess.run(cmd, stderr=subprocess.PIPE).stderr.decode("utf-8")


def add_default_labels_for_test(node_info, labels):
    labels["ray.io/node-id"] = node_info["NodeID"]
    return labels


@pytest.mark.parametrize(
    "call_ray_start",
    ['ray start --head --labels={"gpu_type":"A100","region":"us"}'],
    indirect=True,
)
def test_ray_start_set_node_labels_from_json(call_ray_start):
    ray.init(address=call_ray_start)
    node_info = ray.nodes()[0]
    assert node_info["Labels"] == add_default_labels_for_test(
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
    assert node_info["Labels"] == add_default_labels_for_test(
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
    assert node_info["Labels"] == add_default_labels_for_test(node_info, {})


def test_ray_init_set_node_labels(shutdown_only):
    labels = {"gpu_type": "A100", "region": "us"}
    ray.init(labels=labels)
    node_info = ray.nodes()[0]
    assert node_info["Labels"] == add_default_labels_for_test(node_info, labels)
    ray.shutdown()
    ray.init(labels={})
    node_info = ray.nodes()[0]
    assert node_info["Labels"] == add_default_labels_for_test(node_info, {})


def test_node_labels_connect_only_false(shutdown_only):
    labels = {"key": "value"}
    ray.init(labels=labels)
    node_labels = ray.get_runtime_context().get_node_labels()
    assert node_labels["key"] == "value"


@pytest.mark.parametrize(
    "call_ray_start",
    ['ray start --head --labels={"key":"value"}'],
    indirect=True,
)
def test_node_labels_connect_only_true(call_ray_start):
    ray.init(address=call_ray_start)
    node_labels = ray.get_runtime_context().get_node_labels()
    assert node_labels["key"] == "value"


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
        ["ray", "start", "--head", '--labels={"ray.io/node-id":"111"}']
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
    assert node_info["Labels"] == add_default_labels_for_test(node_info, labels)
    head_node_id = ray.nodes()[0]["NodeID"]

    cluster.add_node(num_cpus=1, labels={})
    cluster.wait_for_nodes()
    for node in ray.nodes():
        if node["NodeID"] != head_node_id:
            assert node["Labels"] == add_default_labels_for_test(node, {})


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
        wait_for_condition(lambda: len(ray.nodes()) == 2, timeout=20)

        for node in ray.nodes():
            if node["Resources"].get("CPU", 0) == 1:
                assert node["Labels"] == add_default_labels_for_test(
                    node, {"region": "us"}
                )
    finally:
        cluster.shutdown()


def test_ray_start_set_node_labels_from_file(shutdown_only):
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as test_file:
        test_file.write('"gpu_type": "A100"\n"region": "us"\n"market-type": "spot"')
        test_file_path = test_file.name

    try:
        cmd = ["ray", "start", "--head", "--labels-file", test_file_path]
        subprocess.check_call(cmd)
        ray.init(address="auto")
        node_info = ray.nodes()[0]
        assert node_info["Labels"] == add_default_labels_for_test(
            node_info, {"gpu_type": "A100", "region": "us", "market-type": "spot"}
        )
    finally:
        subprocess.check_call(["ray", "stop", "--force"])
        os.remove(test_file_path)


def test_get_default_ray_node_labels(shutdown_only, monkeypatch):
    # Set env vars for this test
    monkeypatch.setenv("RAY_NODE_MARKET_TYPE", "spot")
    monkeypatch.setenv("RAY_NODE_TYPE_NAME", "worker-group-1")
    monkeypatch.setenv("RAY_NODE_REGION", "us-central2")
    monkeypatch.setenv("RAY_NODE_ZONE", "us-central2-b")
    monkeypatch.setenv("TPU_ACCELERATOR_TYPE", "v4-16")

    ray.init(resources={"TPU": 4})
    node_info = ray.nodes()[0]
    labels = node_info["Labels"]

    assert labels.get("ray.io/market-type") == "spot"
    assert labels.get("ray.io/node-group") == "worker-group-1"
    assert labels.get("ray.io/availability-region") == "us-central2"
    assert labels.get("ray.io/availability-zone") == "us-central2-b"
    assert labels.get("ray.io/accelerator-type") == "TPU-V4"


def test_get_default_tpu_labels(shutdown_only, monkeypatch):
    # Set env vars for this test
    monkeypatch.setenv("TPU_NAME", "slice-0")
    monkeypatch.setenv("TPU_WORKER_ID", "0")
    monkeypatch.setenv("TPU_ACCELERATOR_TYPE", "v6e-32")
    monkeypatch.setenv("TPU_TOPOLOGY", "4x8")

    with patch(
        "ray._private.accelerators.get_all_accelerator_resource_names",
        return_value=["TPU"],
    ), patch(
        "ray._private.accelerators.get_accelerator_manager_for_resource",
        return_value=TPUAcceleratorManager(),
    ):
        ray.init(resources={"TPU": 4})
        node_info = ray.nodes()[0]
        labels = node_info["Labels"]

    assert labels.get("ray.io/accelerator-type") == "TPU-V6E"

    # TPU specific labels for SPMD
    assert labels.get("ray.io/tpu-slice-name") == "slice-0"
    assert labels.get("ray.io/tpu-worker-id") == "0"
    assert labels.get("ray.io/tpu-topology") == "4x8"
    assert labels.get("ray.io/tpu-pod-type") == "v6e-32"


def test_update_node_labels_reflected_in_ray_nodes(shutdown_only):
    ray.init(labels={"tier": "cpu", "region": "us"})
    node_id = ray.nodes()[0]["NodeID"]

    ray.experimental.update_node_labels(node_id, {"tier": "gpu"})

    # `region` is replaced away, `tier` updated, reserved ray.io/node-id preserved.
    def labels_updated():
        node_info = ray.nodes()[0]
        return node_info["Labels"] == add_default_labels_for_test(
            node_info, {"tier": "gpu"}
        )

    wait_for_condition(labels_updated)


def test_update_node_labels_affects_scheduling(shutdown_only):
    ray.init(num_cpus=1, labels={"tier": "cpu"})
    node_id = ray.nodes()[0]["NodeID"]

    # Zero-CPU actors so placement is gated purely by the label selector (avoids
    # any dependence on re-triggering of already-pending actors).
    @ray.remote(num_cpus=0)
    class Actor:
        def ping(self):
            return "pong"

    # Before the relabel, the only node does not match tier=gpu.
    before = Actor.options(label_selector={"tier": "gpu"}).remote()
    with pytest.raises(GetTimeoutError):
        ray.get(before.ping.remote(), timeout=5)

    ray.experimental.update_node_labels(node_id, {"tier": "gpu"})
    wait_for_condition(lambda: ray.nodes()[0]["Labels"].get("tier") == "gpu")

    # An actor submitted after the relabel is placed on the now-matching node.
    after = Actor.options(label_selector={"tier": "gpu"}).remote()
    assert ray.get(after.ping.remote(), timeout=30) == "pong"

    # The stale label value no longer matches any node.
    stale = Actor.options(label_selector={"tier": "cpu"}).remote()
    with pytest.raises(GetTimeoutError):
        ray.get(stale.ping.remote(), timeout=5)


def test_update_node_labels_validation(shutdown_only):
    ray.init(labels={"tier": "cpu"})
    node_id = ray.nodes()[0]["NodeID"]

    # Reserved ray.io/ prefix is rejected before the RPC.
    with pytest.raises(ValueError):
        ray.experimental.update_node_labels(node_id, {"ray.io/node-id": "x"})

    # Invalid label value syntax is rejected.
    with pytest.raises(ValueError):
        ray.experimental.update_node_labels(node_id, {"tier": "BAD VALUE!"})


def test_update_node_labels_unknown_node(shutdown_only):
    ray.init(labels={"tier": "cpu"})
    # A well-formed but non-existent node id is rejected because it is not alive.
    with pytest.raises(ValueError, match="not alive"):
        ray.experimental.update_node_labels("00" * 28, {"tier": "gpu"})


def test_update_node_labels_clear(shutdown_only):
    ray.init(labels={"tier": "cpu", "region": "us"})
    node_id = ray.nodes()[0]["NodeID"]

    # An empty mapping clears all user labels; reserved ray.io/ labels are preserved.
    ray.experimental.update_node_labels(node_id, {})

    def cleared():
        labels = ray.nodes()[0]["Labels"]
        user_labels = {k for k in labels if not k.startswith("ray.io/")}
        # No user labels remain, and the reserved node-id label is still present.
        # (Hosts may auto-add other ray.io/* labels, so don't assert an exact set.)
        return not user_labels and "ray.io/node-id" in labels

    wait_for_condition(cleared)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
