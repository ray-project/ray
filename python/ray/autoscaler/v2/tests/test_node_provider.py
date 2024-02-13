# coding: utf-8
import os
import sys
import unittest
from typing import Any, Dict, List

import pytest  # noqa

from ray._private.test_utils import load_test_config
from ray.autoscaler._private.event_summarizer import EventSummarizer
from ray.autoscaler._private.kuberay.node_provider import IKubernetesHttpApiClient
from ray.autoscaler._private.node_launcher import BaseNodeLauncher
from ray.autoscaler._private.node_provider_availability_tracker import (
    NodeProviderAvailabilityTracker,
)
from ray.autoscaler.node_launch_exception import NodeLaunchException
from ray.autoscaler.v2.instance_manager.cloud_providers.kuberay.cloud_provider import (
    KubeRayProvider,
)
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
from ray.autoscaler.v2.instance_manager.node_provider import (
    CloudInstance,
    NodeProviderAdapter,
)
from ray.autoscaler.v2.tests.util import FakeCounter
from ray.core.generated.instance_manager_pb2 import Instance, NodeKind
from ray.tests.autoscaler_test_utils import MockProvider
from ray.tests.kuberay.test_autoscaling_config import get_basic_ray_cr
from ray.tests.kuberay.test_kuberay_node_provider import _get_test_yaml


class NodeProviderTest(unittest.TestCase):
    def setUp(self):
        self.base_provider = MockProvider()
        self.availability_tracker = NodeProviderAvailabilityTracker()
        self.node_launcher = BaseNodeLauncher(
            self.base_provider,
            FakeCounter(),
            EventSummarizer(),
            self.availability_tracker,
        )
        self.config = AutoscalingConfig(
            load_test_config("test_ray_complex.yaml"), skip_content_hash=True
        )
        self.node_provider = NodeProviderAdapter(
            self.base_provider, self.node_launcher, self.config
        )

    def test_node_providers_pass_through(self):
        nodes = self.node_provider.create_nodes("worker_nodes1", 1)
        assert len(nodes) == 1
        assert nodes[0] == Instance(
            instance_type="worker_nodes1",
            cloud_instance_id="0",
            internal_ip="172.0.0.0",
            external_ip="1.2.3.4",
            status=Instance.UNKNOWN,
        )
        self.assertEqual(len(self.base_provider.mock_nodes), 1)
        self.assertEqual(self.node_provider.get_non_terminated_nodes(), {"0": nodes[0]})
        nodes1 = self.node_provider.create_nodes("worker_nodes", 2)
        assert len(nodes1) == 2
        assert nodes1[0] == Instance(
            instance_type="worker_nodes",
            cloud_instance_id="1",
            internal_ip="172.0.0.1",
            external_ip="1.2.3.4",
            status=Instance.UNKNOWN,
        )
        assert nodes1[1] == Instance(
            instance_type="worker_nodes",
            cloud_instance_id="2",
            internal_ip="172.0.0.2",
            external_ip="1.2.3.4",
            status=Instance.UNKNOWN,
        )
        self.assertEqual(
            self.node_provider.get_non_terminated_nodes(),
            {"0": nodes[0], "1": nodes1[0], "2": nodes1[1]},
        )
        self.assertEqual(
            self.node_provider.get_nodes_by_cloud_instance_id(["0"]),
            {
                "0": nodes[0],
            },
        )
        self.node_provider.terminate_node("0")
        self.assertEqual(
            self.node_provider.get_non_terminated_nodes(),
            {"1": nodes1[0], "2": nodes1[1]},
        )
        self.assertFalse(self.node_provider.is_readonly())

    def test_create_node_failure(self):
        self.base_provider.error_creates = NodeLaunchException(
            "hello", "failed to create node", src_exc_info=None
        )
        self.assertEqual(self.node_provider.create_nodes("worker_nodes1", 1), [])
        self.assertEqual(len(self.base_provider.mock_nodes), 0)
        self.assertTrue(
            "worker_nodes1" in self.availability_tracker.summary().node_availabilities
        )
        self.assertEqual(
            self.node_provider.get_non_terminated_nodes(),
            {},
        )


class MockKubernetesHttpApiClient(IKubernetesHttpApiClient):
    def __init__(self, pod_list: List[Dict[str, Any]], ray_cluster: Dict[str, Any]):
        self._ray_cluster = ray_cluster
        self._pod_list = pod_list
        self._patches = {}

    def get(self, path: str) -> Dict[str, Any]:
        if "pods" in path:
            return self._pod_list
        if "rayclusters" in path:
            return self._ray_cluster

        raise NotImplementedError(f"get {path}")

    def patch(self, path: str, patches: List[Dict[str, Any]]):
        self._patches[path] = patches
        return {path: patches}

    def get_patches(self, path: str) -> List[Dict[str, Any]]:
        return self._patches[path]


class KubeRayProviderTest(unittest.TestCase):
    def setUp(self):
        self.mock_client = MockKubernetesHttpApiClient(
            _get_test_yaml("podlist1.yaml"), get_basic_ray_cr()
        )
        self.provider = KubeRayProvider(
            cluster_name="test",
            namespace="test-namespace",
            k8s_api_client=self.mock_client,
        )

    def test_get_nodes(self):
        nodes = self.provider.get_non_terminated()
        errors = self.provider.poll_errors()

        assert len(nodes) == 2
        assert len(errors) == 0

        assert sorted(nodes) == sorted(
            {
                "raycluster-autoscaler-head-8zsc8": CloudInstance(
                    cloud_instance_id="raycluster-autoscaler-head-8zsc8",
                    node_kind=NodeKind.HEAD,
                    node_type="head-group",
                    is_running=True,
                ),  # up-to-date status because the Ray container is in running status
                "raycluster-autoscaler-worker-small-group-dkz2r": CloudInstance(
                    cloud_instance_id="raycluster-autoscaler-worker-small-group-dkz2r",
                    node_kind=NodeKind.WORKER,
                    node_type="small-group",
                    is_running=False,
                ),  # waiting status, because Ray container's state is pending.
            }
        )

    def test_launch_node(self):
        launch_request = {"small-group": 1}
        self.provider.launch(shape=launch_request, request_id="launch-1")

        patches = self.mock_client.get_patches(
            f"rayclusters/{self.provider._cluster_name}"
        )
        assert len(patches) == 1
        assert patches[0] == {
            "op": "replace",
            "path": "/spec/workerGroupSpecs/0/replicas",
            "value": 2,  # 1 + 1
        }

    def test_terminate_node(self):
        self.provider.terminate(
            ids=["raycluster-autoscaler-worker-small-group-dkz2r"], request_id="term-1"
        )
        patches = self.mock_client.get_patches(
            f"rayclusters/{self.provider._cluster_name}"
        )
        assert len(patches) == 2
        assert patches == [
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/0/replicas",
                "value": 0,
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
        ]

    def test_pending_deletes(self):
        # Modify the cr.yaml to have a pending delete.
        self.mock_client._ray_cluster["spec"]["workerGroupSpecs"][0][
            "scaleStrategy"
        ] = {"workersToDelete": ["raycluster-autoscaler-worker-small-group-dkz2r"]}
        self.mock_client._ray_cluster["spec"]["workerGroupSpecs"][0]["replicas"] = 0

        # Launching new nodes should fail.
        self.provider.launch(shape={"small-group": 1}, request_id="launch-1")
        errors = self.provider.poll_errors()
        assert errors[0].node_type == "small-group"
        assert errors[0].request_id == "launch-1"
        assert "There are workers to be deleted" in errors[0].details

        # Terminating new nodes should fail.
        self.provider.terminate(
            ids=["raycluster-autoscaler-worker-small-group-dkz2r"], request_id="term-1"
        )
        errors = self.provider.poll_errors()
        assert (
            errors[0].cloud_instance_id
            == "raycluster-autoscaler-worker-small-group-dkz2r"
        )
        assert errors[0].request_id == "term-1"
        assert "There are workers to be deleted" in errors[0].details

        # Remove the pod from the pod list.
        self.mock_client._pod_list["items"] = [
            pod
            for pod in self.mock_client._pod_list["items"]
            if pod["metadata"]["name"]
            != "raycluster-autoscaler-worker-small-group-dkz2r"
        ]

        # Launch OK now, and we should also clears the pending delete.
        self.provider.launch(shape={"small-group": 1}, request_id="launch-2")
        errors = self.provider.poll_errors()
        assert len(errors) == 0
        patches = self.mock_client.get_patches(
            f"rayclusters/{self.provider._cluster_name}"
        )
        assert len(patches) == 2
        assert patches == [
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/0/replicas",
                "value": 1,
            },
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/0/scaleStrategy",
                "value": {"workersToDelete": []},
            },
        ]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
