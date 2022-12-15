import copy
import mock
import sys

import jsonpatch
import pytest

from ray.autoscaler.batching_node_provider import NodeData
from ray.autoscaler._private.kuberay.node_provider import (
    _worker_group_index,
    _worker_group_max_replicas,
    _worker_group_replicas,
    KuberayNodeProvider,
    ScaleRequest,
)
from ray.autoscaler._private.util import NodeID
from pathlib import Path
import yaml

from ray.tests.kuberay.test_autoscaling_config import get_basic_ray_cr
from typing import Set, List


def _get_basic_ray_cr_workers_to_delete(
    cpu_workers_to_delete: List[NodeID], gpu_workers_to_delete: List[NodeID]
):
    """Generate a Ray cluster with non-empty workersToDelete field."""
    raycluster = get_basic_ray_cr()
    raycluster["spec"]["workerGroupSpecs"][0]["scaleStrategy"] = {
        "workersToDelete": cpu_workers_to_delete
    }
    raycluster["spec"]["workerGroupSpecs"][1]["scaleStrategy"] = {
        "workersToDelete": gpu_workers_to_delete
    }
    return raycluster


def _get_test_yaml(file_name):
    file_path = str(Path(__file__).resolve().parent / "test_files" / file_name)
    return yaml.safe_load(open(file_path).read())


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Not relevant on Windows.")
@pytest.mark.parametrize(
    "group_name,expected_index", [("small-group", 0), ("gpu-group", 1)]
)
def test_worker_group_index(group_name, expected_index):
    """Basic unit test for _worker_group_index.

    Uses a RayCluster CR with worker groups "small-group" and "gpu-group",
    listed in that order.
    """
    raycluster_cr = get_basic_ray_cr()
    assert _worker_group_index(raycluster_cr, group_name) == expected_index


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Not relevant on Windows.")
@pytest.mark.parametrize(
    "group_index,expected_max_replicas,expected_replicas",
    [(0, 300, 1), (1, 200, 1), (2, None, 0)],
)
def test_worker_group_replicas(group_index, expected_max_replicas, expected_replicas):
    """Basic unit test for _worker_group_max_replicas and _worker_group_replicas

    Uses a RayCluster CR with worker groups with 300 maxReplicas, 200 maxReplicas,
    and unspecified maxReplicas, in that order.
    """
    raycluster = get_basic_ray_cr()

    # Add a worker group without maxReplicas to confirm behavior
    # when maxReplicas is not specified.
    no_max_replicas_group = copy.deepcopy(raycluster["spec"]["workerGroupSpecs"][0])
    no_max_replicas_group["groupName"] = "no-max-replicas"
    del no_max_replicas_group["maxReplicas"]
    # Also, replicas field, just for the sake of testing.
    no_max_replicas_group["replicas"] = 0
    raycluster["spec"]["workerGroupSpecs"].append(no_max_replicas_group)

    assert _worker_group_max_replicas(raycluster, group_index) == expected_max_replicas
    assert _worker_group_replicas(raycluster, group_index) == expected_replicas


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Not relevant on Windows.")
@pytest.mark.parametrize(
    "attempted_target_replica_count,expected_target_replica_count",
    [(200, 200), (250, 250), (300, 300), (400, 300), (1000, 300)],
)
def test_create_node_cap_at_max(
    attempted_target_replica_count, expected_target_replica_count
):
    """Validates that KuberayNodeProvider does not attempt to create more nodes than
    allowed by maxReplicas. For the config in this test, maxReplicas is fixed at 300.

    Args:
        attempted_target_replica_count: The mocked desired replica count for a given
            worker group.
        expected_target_replica_count: The actual requested replicaCount. Should be
            capped at maxReplicas (300, for the config in this test.)
    """
    raycluster = get_basic_ray_cr()
    with mock.patch.object(KuberayNodeProvider, "__init__", return_value=None):
        kr_node_provider = KuberayNodeProvider(provider_config={}, cluster_name="fake")
        scale_request = ScaleRequest(
            workers_to_delete=set(),
            desired_num_workers={"small-group": attempted_target_replica_count},
        )
        patch = kr_node_provider._scale_request_to_patch_payload(
            scale_request=scale_request, raycluster=raycluster
        )
        assert patch[0]["value"] == expected_target_replica_count


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Not relevant on Windows.")
@pytest.mark.parametrize(
    "podlist_file,expected_node_data",
    [
        (
            # Pod list obtained by running kubectl get pod -o yaml at runtime.
            "podlist1.yaml",
            {
                "raycluster-autoscaler-head-8zsc8": NodeData(
                    kind="head", type="head-group", ip="10.4.2.6", status="up-to-date"
                ),  # up-to-date status because the Ray container is in running status
                "raycluster-autoscaler-worker-small-group-dkz2r": NodeData(
                    kind="worker", type="small-group", ip="10.4.1.8", status="waiting"
                ),  # waiting status, because Ray container's state is "waiting".
                # The pod list includes a worker with non-null deletion timestamp.
                # It is excluded from the node data because it is considered
                # "terminated".
            },
        ),
        (
            # Pod list obtained by running kubectl get pod -o yaml at runtime.
            "podlist2.yaml",
            {
                "raycluster-autoscaler-head-8zsc8": NodeData(
                    kind="head", type="head-group", ip="10.4.2.6", status="up-to-date"
                ),
                "raycluster-autoscaler-worker-fake-gpu-group-2qnhv": NodeData(
                    kind="worker",
                    type="fake-gpu-group",
                    ip="10.4.0.6",
                    status="up-to-date",
                ),
                "raycluster-autoscaler-worker-small-group-dkz2r": NodeData(
                    kind="worker",
                    type="small-group",
                    ip="10.4.1.8",
                    status="up-to-date",
                ),
                "raycluster-autoscaler-worker-small-group-lbfm4": NodeData(
                    kind="worker",
                    type="small-group",
                    ip="10.4.0.5",
                    status="up-to-date",
                ),
            },
        ),
    ],
)
def test_get_node_data(podlist_file: str, expected_node_data):
    """Test translation of a K8s pod list into autoscaler node data."""
    pod_list = _get_test_yaml(podlist_file)

    def mock_get(node_provider, path):
        if "pods" in path:
            return pod_list
        elif "raycluster" in path:
            return get_basic_ray_cr()
        else:
            raise ValueError("Invalid path.")

    with mock.patch.object(
        KuberayNodeProvider, "__init__", return_value=None
    ), mock.patch.object(KuberayNodeProvider, "_get", mock_get):
        kr_node_provider = KuberayNodeProvider(provider_config={}, cluster_name="fake")
        kr_node_provider.cluster_name = "fake"
        nodes = kr_node_provider.non_terminated_nodes({})
        assert kr_node_provider.node_data_dict == expected_node_data
        assert set(nodes) == set(expected_node_data.keys())


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Not relevant on Windows.")
@pytest.mark.parametrize(
    "node_data_dict,scale_request,expected_patch_payload",
    [
        (
            {
                "raycluster-autoscaler-head-8zsc8": NodeData(
                    kind="head", type="head-group", ip="10.4.2.6", status="up-to-date"
                ),
                "raycluster-autoscaler-worker-fake-gpu-group-2qnhv": NodeData(
                    kind="worker",
                    type="fake-gpu-group",
                    ip="10.4.0.6",
                    status="up-to-date",
                ),
                "raycluster-autoscaler-worker-small-group-dkz2r": NodeData(
                    kind="worker",
                    type="small-group",
                    ip="10.4.1.8",
                    status="up-to-date",
                ),
                "raycluster-autoscaler-worker-small-group-lbfm4": NodeData(
                    kind="worker",
                    type="small-group",
                    ip="10.4.0.5",
                    status="up-to-date",
                ),
            },
            ScaleRequest(
                desired_num_workers={
                    "small-group": 1,  # Delete 1
                    "gpu-group": 1,  # Don't touch
                    "blah-group": 5,  # Create 5
                },
                workers_to_delete={
                    "raycluster-autoscaler-worker-small-group-dkz2r",
                },
            ),
            [
                {
                    "op": "replace",
                    "path": "/spec/workerGroupSpecs/2/replicas",
                    "value": 5,
                },
                {
                    "op": "replace",
                    "path": "/spec/workerGroupSpecs/0/scaleStrategy",
                    "value": {
                        "workersToDelete": [
                            "raycluster-autoscaler-worker-small-group-dkz2r"
                        ]
                    },
                },
            ],
        ),
    ],
)
def test_submit_scale_request(node_data_dict, scale_request, expected_patch_payload):
    """Test the KuberayNodeProvider's RayCluster patch payload given a dict
    of current node counts and a scale request.
    """
    raycluster = get_basic_ray_cr()
    # Add another worker group for the sake of this test.
    blah_group = copy.deepcopy(raycluster["spec"]["workerGroupSpecs"][1])
    blah_group["groupName"] = "blah-group"
    raycluster["spec"]["workerGroupSpecs"].append(blah_group)
    with mock.patch.object(KuberayNodeProvider, "__init__", return_value=None):
        kr_node_provider = KuberayNodeProvider(provider_config={}, cluster_name="fake")
        kr_node_provider.node_data_dict = node_data_dict
        patch_payload = kr_node_provider._scale_request_to_patch_payload(
            scale_request=scale_request, raycluster=raycluster
        )
        assert patch_payload == expected_patch_payload


@pytest.mark.parametrize("node_set", [{"A", "B", "C", "D", "E"}])
@pytest.mark.parametrize("cpu_workers_to_delete", ["A", "Z"])
@pytest.mark.parametrize("gpu_workers_to_delete", ["B", "Y"])
@pytest.mark.skipif(sys.platform.startswith("win"), reason="Not relevant on Windows.")
def test_safe_to_scale(
    node_set: Set[NodeID],
    cpu_workers_to_delete: List[NodeID],
    gpu_workers_to_delete: List[NodeID],
):
    # NodeData values unimportant for this test.
    mock_node_data = NodeData("-", "-", "-", "-")
    node_data_dict = {node_id: mock_node_data for node_id in node_set}

    raycluster = _get_basic_ray_cr_workers_to_delete(
        cpu_workers_to_delete, gpu_workers_to_delete
    )

    def mock_patch(kuberay_provider, path, patch_payload):
        patch = jsonpatch.JsonPatch(patch_payload)
        kuberay_provider._patched_raycluster = patch.apply(kuberay_provider._raycluster)

    with mock.patch.object(
        KuberayNodeProvider, "__init__", return_value=None
    ), mock.patch.object(KuberayNodeProvider, "_patch", mock_patch):
        kr_node_provider = KuberayNodeProvider(provider_config={}, cluster_name="fake")
        kr_node_provider.cluster_name = "fake"
        kr_node_provider._patched_raycluster = raycluster
        kr_node_provider._raycluster = raycluster
        kr_node_provider.node_data_dict = node_data_dict
        actual_safe = kr_node_provider.safe_to_scale()

    expected_safe = not any(
        cpu_worker_to_delete in node_set
        for cpu_worker_to_delete in cpu_workers_to_delete
    ) and not any(
        gpu_worker_to_delete in node_set
        for gpu_worker_to_delete in gpu_workers_to_delete
    )
    assert expected_safe is actual_safe
    patched_cpu_workers_to_delete = kr_node_provider._patched_raycluster["spec"][
        "workerGroupSpecs"
    ][0]["scaleStrategy"]["workersToDelete"]
    patched_gpu_workers_to_delete = kr_node_provider._patched_raycluster["spec"][
        "workerGroupSpecs"
    ][1]["scaleStrategy"]["workersToDelete"]

    if expected_safe:
        # Cleaned up workers to delete
        assert patched_cpu_workers_to_delete == []
        assert patched_gpu_workers_to_delete == []
    else:
        # Did not clean up workers to delete
        assert patched_cpu_workers_to_delete == cpu_workers_to_delete
        assert patched_gpu_workers_to_delete == gpu_workers_to_delete


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
