import copy
import mock
import sys

import pytest

from ray.autoscaler._private.kuberay.node_provider import (
    _worker_group_index,
    _worker_group_max_replicas,
    _worker_group_replicas,
    KuberayNodeProvider,
    ScaleRequest,
)
from typing import Any, Dict

from ray.tests.kuberay.test_autoscaling_config import get_basic_ray_cr

SAMPLE_POD_LIST = []
SAMPLE_NODE_DATA = {}

SAMPLE_SCALE_REQUEST = ScaleRequest
SAMPLE_RAYCLUSTER_PATCH_PAYLOAD = []


def _get_basic_ray_cr_workers_to_delete():
    """Generate a Ray cluster with non-empty workersToDelete field.
    """
    raycluster = get_basic_ray_cr()
    raycluster["spec"]["workerGroupSpecs"][1]["scaleStrategy"] = {
        "workersToDelete": ["random-worker"]
    }
    return raycluster


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
    "pod_list,expected_node_data",
    [(SAMPLE_POD_LIST, SAMPLE_NODE_DATA)],
)
def test_get_node_data(pod_list, expected_node_data):
    def mock_get(node_provider, path):
        if path == "pods":
            return pod_list
        else:
            return get_basic_ray_cr()

    with mock.patch.object(
            KuberayNodeProvider, "__init__", return_value=None
    ), mock.patch.object(
            KuberayNodeProvider, "_get", mock_get
    ):
        kr_node_provider = KuberayNodeProvider(provider_config={}, cluster_name="fake")
        nodes = kr_node_provider.non_terminated_nodes({})
        assert kr_node_provider.node_data_dict == expected_node_data
        assert set(nodes) == set(expected_node_data.keys())


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Not relevant on Windows.")
@pytest.mark.parametrize(
    "scale_request,expected_patch_payload",
    [(SAMPLE_SCALE_REQUEST, SAMPLE_RAYCLUSTER_PATCH_PAYLOAD)],
)
def test_submit_scale_request(scale_request, expected_patch_payload):
    raycluster = get_basic_ray_cr()
    with mock.patch.object(KuberayNodeProvider, "__init__", return_value=None):
        kr_node_provider = KuberayNodeProvider(provider_config={}, cluster_name="fake")
        patch_payload = kr_node_provider._scale_request_to_patch_payload(
            scale_request=scale_request, raycluster=raycluster
        )
        assert patch_payload == expected_patch_payload


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Not relevant on Windows.")
@pytest.mark.parametrize(
    "raycluster,expected",
    [
        (_get_basic_ray_cr_workers_to_delete(), False),
        (get_basic_ray_cr(), True)
    ],
)
def test_safe_to_scale(raycluster: Dict[str, Any], expected: bool):
    with mock.patch.object(KuberayNodeProvider, "__init__", return_value=None):
        kr_node_provider = KuberayNodeProvider(provider_config={}, cluster_name="fake")
        kr_node_provider._raycluster = raycluster
        assert kr_node_provider.safe_to_scale() is expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
