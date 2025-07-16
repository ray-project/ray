import logging
import os
import sys
import time
import unittest

# coding: utf-8
# coding: utf-8
from collections import defaultdict
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest  # noqa

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import get_test_config_path
from ray.autoscaler._private.constants import (
    AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
    AUTOSCALER_MAX_LAUNCH_BATCH,
)
from ray.autoscaler._private.fake_multi_node.node_provider import FakeMultiNodeProvider
from ray.autoscaler._private.kuberay.node_provider import IKubernetesHttpApiClient
from ray.autoscaler.v2.instance_manager.cloud_providers.kuberay.cloud_provider import (
    KubeRayProvider,
)
from ray.autoscaler.v2.instance_manager.config import (
    AutoscalingConfig,
    FileConfigReader,
)
from ray.autoscaler.v2.instance_manager.node_provider import (
    CloudInstance,
    ICloudInstanceProvider,
    LaunchNodeError,
    NodeProviderAdapter,
    TerminateNodeError,
    logger,
)
from ray.core.generated.instance_manager_pb2 import NodeKind
from ray.tests.autoscaler_test_utils import MockProvider
from ray.tests.kuberay.test_autoscaling_config import get_basic_ray_cr
from ray.tests.kuberay.test_kuberay_node_provider import _get_test_yaml

logger.setLevel(logging.DEBUG)


class CloudInstanceProviderTesterBase(ICloudInstanceProvider):
    def __init__(
        self,
        inner_provider: ICloudInstanceProvider,
        config: AutoscalingConfig,
    ):
        self.inner_provider = inner_provider
        self.config = config

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        pass

    def launch(self, request_id, shape):
        self.inner_provider.launch(shape=shape, request_id=request_id)

    def terminate(self, request_id, ids):
        self.inner_provider.terminate(ids=ids, request_id=request_id)

    def poll_errors(self):
        return self.inner_provider.poll_errors()

    def get_non_terminated(self):
        return self.inner_provider.get_non_terminated()

    ############################
    # Test mock methods
    ############################
    def _add_creation_error(self, e: Exception):
        raise NotImplementedError("Subclass should implement it")

    def _add_termination_errors(self, e: Exception):
        raise NotImplementedError("Subclass should implement it")


class FakeMultiNodeProviderTester(CloudInstanceProviderTesterBase):
    def __init__(self, **kwargs):
        self.config_reader = FileConfigReader(
            get_test_config_path("test_ray_complex.yaml"), skip_content_hash=True
        )
        self.config = self.config_reader.get_cached_autoscaling_config()
        self.ray_session = None

        os.environ["RAY_FAKE_CLUSTER"] = "1"
        provider_config = self.config.get_provider_config()
        # This is a bit hacky but we need a fake head node.
        self.ray_session = ray.init()
        provider_config["gcs_address"] = self.ray_session.address_info["gcs_address"]
        provider_config["head_node_id"] = self.ray_session.address_info["node_id"]
        provider_config["launch_multiple"] = True
        self.base_provider = FakeMultiNodeProvider(
            provider_config,
            cluster_name="test",
        )

        provider = NodeProviderAdapter(
            self.base_provider,
            self.config_reader,
        )
        super().__init__(provider, self.config)

    def get_non_terminated(self):
        nodes = self.inner_provider.get_non_terminated()
        nodes.pop(self.ray_session.address_info["node_id"], None)
        return nodes

    def shutdown(self):
        ray.shutdown()

    def _add_creation_error(self, e: Exception):
        self.base_provider._test_set_creation_error(e)

    def _add_termination_errors(self, e: Exception):
        self.base_provider._test_add_termination_errors(e)


class MockProviderTester(CloudInstanceProviderTesterBase):
    def __init__(self, **kwargs):
        self.config_reader = FileConfigReader(
            get_test_config_path("test_ray_complex.yaml"), skip_content_hash=True
        )
        self.config = self.config_reader.get_cached_autoscaling_config()
        self.base_provider = MockProvider()
        provider = NodeProviderAdapter(
            self.base_provider,
            self.config_reader,
        )
        super().__init__(provider, self.config)

    def _add_creation_error(self, e: Exception):
        self.base_provider.creation_error = e

    def _add_termination_errors(self, e: Exception):
        self.base_provider.termination_errors = e


class MagicMockProviderTester(CloudInstanceProviderTesterBase):
    def __init__(
        self,
        max_concurrent_launches=AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
        max_launch_batch_per_type=AUTOSCALER_MAX_LAUNCH_BATCH,
        **kwargs,
    ):
        self.config_reader = FileConfigReader(
            get_test_config_path("test_ray_complex.yaml"), skip_content_hash=True
        )
        self.config = self.config_reader.get_cached_autoscaling_config()
        self.base_provider = MagicMock()
        provider = NodeProviderAdapter(
            self.base_provider,
            self.config_reader,
            max_launch_batch_per_type=max_launch_batch_per_type,
            max_concurrent_launches=max_concurrent_launches,
        )
        super().__init__(provider, self.config)

    def _add_creation_error(self, e: Exception):
        self.base_provider.create_node_with_resources_and_labels.side_effect = e

    def _add_termination_errors(self, e: Exception):
        self.base_provider.terminate_nodes.side_effect = e


@pytest.fixture(scope="function")
def get_provider():
    def _get_provider(name, **kwargs):
        if name == "fake_multi":
            provider = FakeMultiNodeProviderTester(**kwargs)
        elif name == "mock":
            provider = MockProviderTester(**kwargs)
        elif name == "magic_mock":
            provider = MagicMockProviderTester(**kwargs)
        else:
            raise ValueError(f"Invalid provider type: {name}")

        return provider

    yield _get_provider


@pytest.mark.parametrize(
    "provider_name",
    ["fake_multi", "mock"],
)
def test_node_providers_basic(get_provider, provider_name):
    # Test launching.
    provider = get_provider(name=provider_name)
    provider.launch(
        shape={"worker_nodes": 2},
        request_id="1",
    )

    provider.launch(
        request_id="2",
        shape={"worker_nodes": 2, "worker_nodes1": 1},
    )

    def verify():
        nodes_by_type = defaultdict(int)
        for node in provider.get_non_terminated().values():
            nodes_by_type[node.node_type] += 1
        errors = provider.poll_errors()
        print(errors)
        assert nodes_by_type == {"worker_nodes": 4, "worker_nodes1": 1}
        return True

    wait_for_condition(verify)

    nodes = provider.get_non_terminated().keys()

    # Terminate them all
    provider.terminate(
        ids=nodes,
        request_id="3",
    )

    # Launch some.
    provider.launch(
        shape={"worker_nodes": 1},
        request_id="4",
    )

    def verify():
        nodes_by_type = defaultdict(int)
        for node in provider.get_non_terminated().values():
            nodes_by_type[node.node_type] += 1

        assert nodes_by_type == {"worker_nodes": 1}
        for node in provider.get_non_terminated().values():
            assert node.request_id == "4"
        return True

    wait_for_condition(verify)


@pytest.mark.parametrize(
    "provider_name",
    ["fake_multi", "mock"],
)
def test_launch_failure(get_provider, provider_name):
    provider = get_provider(name=provider_name)
    provider._add_creation_error(Exception("failed to create node"))

    provider.launch(
        shape={"worker_nodes": 2},
        request_id="2",
    )

    def verify():
        errors = provider.poll_errors()
        assert len(errors) == 1
        assert isinstance(errors[0], LaunchNodeError)
        assert errors[0].node_type == "worker_nodes"
        assert errors[0].request_id == "2"
        return True

    wait_for_condition(verify)


@pytest.mark.parametrize(
    "provider_name",
    ["fake_multi", "mock"],
)
def test_terminate_node_failure(get_provider, provider_name):
    provider = get_provider(name=provider_name)
    provider._add_termination_errors(Exception("failed to terminate node"))

    provider.launch(request_id="launch1", shape={"worker_nodes": 1})

    def nodes_launched():
        nodes = provider.get_non_terminated()
        return len(nodes) == 1

    wait_for_condition(nodes_launched)

    provider.terminate(request_id="terminate1", ids=["0"])

    def verify():
        errors = provider.poll_errors()
        nodes = provider.get_non_terminated()
        assert len(nodes) == 1
        assert len(errors) == 1
        assert isinstance(errors[0], TerminateNodeError)
        assert errors[0].cloud_instance_id == "0"
        assert errors[0].request_id == "terminate1"
        return True

    wait_for_condition(verify)


def test_launch_executor_concurrency(get_provider):
    import threading

    provider = get_provider(
        name="magic_mock", max_concurrent_launches=1, max_launch_batch_per_type=1
    )

    launch_event = threading.Event()

    def loop(*args, **kwargs):
        launch_event.wait()

    provider.base_provider.create_node_with_resources_and_labels.side_effect = loop

    provider.launch(
        shape={
            "worker_nodes": 1,
            "worker_nodes1": 1,
        },  # 2 types, but concurrent types to launch is 1.
        request_id="1",
    )
    # Assert called only once.
    for _ in range(10):
        assert (
            provider.base_provider.create_node_with_resources_and_labels.call_count <= 1
        )
        time.sleep(0.1)

    # Finish the call.
    launch_event.set()

    def verify():
        assert (
            provider.base_provider.create_node_with_resources_and_labels.call_count == 2
        )
        return True

    wait_for_condition(verify)


#######################################
# Integration test for KubeRay Provider
#######################################
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


class KubeRayProviderIntegrationTest(unittest.TestCase):
    def setUp(self):
        raycluster_cr = get_basic_ray_cr()
        # Remove fake TPU and GPU worker groups from CR since podlist1 only
        # contains small-group.
        raycluster_cr["spec"]["workerGroupSpecs"][1]["replicas"] = 0
        raycluster_cr["spec"]["workerGroupSpecs"][2]["replicas"] = 0
        self.mock_client = MockKubernetesHttpApiClient(
            _get_test_yaml("podlist1.yaml"), raycluster_cr
        )
        self.provider = KubeRayProvider(
            cluster_name="test",
            provider_config={
                "namespace": "default",
                "head_node_type": "headgroup",
            },
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
                    node_type="headgroup",
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
        assert "There are workers to be deleted" in str(errors[0]), errors[0]

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
        assert "There are workers to be deleted" in str(errors[0]), errors[0]

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

    def test_increase_min_replicas_to_scale_up(self):
        # Simulate the case where users manually increase the `minReplicas` field
        # from 0 to $num_pods. KubeRay will create $num_pods worker Pods to meet the new
        # `minReplicas`, even though the `replicas` field is still 0.
        small_group = "small-group"
        num_pods = 0
        assert (
            self.mock_client._ray_cluster["spec"]["workerGroupSpecs"][0]["groupName"]
            == small_group
        )
        for pod in self.mock_client._pod_list["items"]:
            if pod["metadata"]["labels"]["ray.io/group"] == small_group:
                num_pods += 1
        assert num_pods > 0
        self.mock_client._ray_cluster["spec"]["workerGroupSpecs"][0]["replicas"] = 0
        self.mock_client._ray_cluster["spec"]["workerGroupSpecs"][0][
            "minReplicas"
        ] = num_pods

        # Launching a new node and `replicas` should be
        # `max(replicas, minReplicas) + 1`.
        self.provider.launch(shape={small_group: 1}, request_id="launch-1")
        patches = self.mock_client.get_patches(
            f"rayclusters/{self.provider._cluster_name}"
        )
        assert len(patches) == 1
        assert patches[0] == {
            "op": "replace",
            "path": "/spec/workerGroupSpecs/0/replicas",
            "value": num_pods + 1,
        }

    def test_inconsistent_pods_raycr_scale_up(self):
        """
        Test the case where the cluster state has not yet reached the desired state.
        Specifically, the replicas field in the RayCluster CR does not match the actual
        number of Pods.
        """
        # Check the assumptions of the test
        small_group = "small-group"
        num_pods = 0
        for pod in self.mock_client._pod_list["items"]:
            if pod["metadata"]["labels"]["ray.io/group"] == small_group:
                num_pods += 1

        assert (
            self.mock_client._ray_cluster["spec"]["workerGroupSpecs"][0]["groupName"]
            == small_group
        )
        desired_replicas = num_pods + 1
        self.mock_client._ray_cluster["spec"]["workerGroupSpecs"][0][
            "replicas"
        ] = desired_replicas

        # Launch a new node. The replicas field should be incremented by 1, even though
        # the cluster state has not yet reached the goal state.
        launch_request = {"small-group": 1}
        self.provider.launch(shape=launch_request, request_id="launch-1")

        patches = self.mock_client.get_patches(
            f"rayclusters/{self.provider._cluster_name}"
        )
        assert len(patches) == 1
        assert patches[0] == {
            "op": "replace",
            "path": "/spec/workerGroupSpecs/0/replicas",
            "value": desired_replicas + 1,
        }

    def test_inconsistent_pods_raycr_scale_down(self):
        """
        Test the case where the cluster state has not yet reached the desired state.
        Specifically, the replicas field in the RayCluster CR does not match the actual
        number of Pods.
        """
        # Check the assumptions of the test
        small_group = "small-group"
        num_pods = 0
        pod_to_delete = None
        for pod in self.mock_client._pod_list["items"]:
            if pod["metadata"]["labels"]["ray.io/group"] == small_group:
                num_pods += 1
                pod_to_delete = pod["metadata"]["name"]
        assert pod_to_delete is not None

        assert (
            self.mock_client._ray_cluster["spec"]["workerGroupSpecs"][0]["groupName"]
            == small_group
        )
        desired_replicas = num_pods + 1
        self.mock_client._ray_cluster["spec"]["workerGroupSpecs"][0][
            "replicas"
        ] = desired_replicas

        # Terminate a node. The replicas field should be decremented by 1, even though
        # the cluster state has not yet reached the goal state.
        self.provider.terminate(ids=[pod_to_delete], request_id="term-1")
        patches = self.mock_client.get_patches(
            f"rayclusters/{self.provider._cluster_name}"
        )
        assert len(patches) == 2
        assert patches == [
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/0/replicas",
                "value": desired_replicas - 1,
            },
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/0/scaleStrategy",
                "value": {
                    "workersToDelete": [
                        pod_to_delete,
                    ]
                },
            },
        ]

    def test_scale_down_multiple_pods_of_node_type(self):
        """
        Test the case where multiple pods of the same node type are scaled
        down on one autoscaler iteration. This test verifies that the provider
        properly handles multiple pod deletions and counting workers_to_delete.
        """
        # Setup provider with multiple worker pods in podlist. We use podlist2
        # here because podlist1 only contains one running worker.
        raycluster_cr = get_basic_ray_cr()
        raycluster_cr["spec"]["workerGroupSpecs"][0]["replicas"] = 2
        raycluster_cr["spec"]["workerGroupSpecs"][1]["replicas"] = 0
        raycluster_cr["spec"]["workerGroupSpecs"][2]["replicas"] = 0
        mock_client = MockKubernetesHttpApiClient(
            _get_test_yaml("podlist2.yaml"), raycluster_cr
        )
        provider = KubeRayProvider(
            cluster_name="test",
            provider_config={
                "namespace": "default",
                "head_node_type": "headgroup",
            },
            k8s_api_client=mock_client,
        )

        # Identify all pods in the target group
        small_group = "small-group"
        pod_names = []
        for pod in mock_client._pod_list["items"]:
            if pod["metadata"]["labels"]["ray.io/group"] == small_group:
                pod_names.append(pod["metadata"]["name"])

        # Terminate all pods in the group
        provider._sync_with_api_server()
        cur_instance_ids = set(provider.instances.keys())
        pods_to_terminate = [name for name in pod_names if name in cur_instance_ids]

        assert (
            len(pods_to_terminate) > 1
        ), "Expected multiple pods to terminate in the target group."

        provider.terminate(ids=pods_to_terminate, request_id="term-2")

        # Check the patches applied to the RayCluster resource
        patches = mock_client.get_patches(f"rayclusters/{provider._cluster_name}")

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
                    "workersToDelete": pods_to_terminate,
                },
            },
        ]

    def test_worker_to_delete_info(self):
        """
        Validate _get_workers_delete_info correctly returns the worker groups with pending
        deletes, worker groups with finished deletes, and the set of workers to delete.
        """
        # Create a RayCluster CR and set replicas to 0 to simulate the case where the autoscaler
        # patches the RayCluster with `replicas: 0`, but alive Pods still exist in workersToDelete.
        raycluster_cr = get_basic_ray_cr()
        raycluster_cr["spec"]["workerGroupSpecs"][0]["replicas"] = 0
        mock_client = MockKubernetesHttpApiClient(
            _get_test_yaml("podlist2.yaml"), raycluster_cr
        )

        # Add some workers to workersToDelete.
        small_group = "small-group"
        pod_names = []
        for pod in mock_client._pod_list["items"]:
            if pod["metadata"]["labels"]["ray.io/group"] == small_group:
                pod_names.append(pod["metadata"]["name"])
        raycluster_cr["spec"]["workerGroupSpecs"][0]["scaleStrategy"] = {
            "workersToDelete": pod_names,
        }

        (
            pending_deletes,
            finished_deletes,
            workers_to_delete,
        ) = KubeRayProvider._get_workers_delete_info(raycluster_cr, set(pod_names))

        # Validate _get_workers_delete_info populates sets as expected.
        assert pending_deletes == {"small-group"}
        assert finished_deletes == set()
        assert workers_to_delete == {pod_names[0], pod_names[1]}

    def test_scale_down_with_multi_host_group(self):
        """
        Test the case where a worker group has numOfHosts > 1.
        This ensures that the KubeRay provider accounts for multi-host replicas
        during scale down and properly updates the workersToDelete field.
        """
        # Setup mock RayCluster CR with numOfHosts: 2 and replicas: 1,
        # resulting in 2 workers total.
        raycluster_cr = get_basic_ray_cr()
        mock_client = MockKubernetesHttpApiClient(
            _get_test_yaml("podlist2.yaml"), raycluster_cr
        )
        provider = KubeRayProvider(
            cluster_name="test",
            provider_config={
                "namespace": "default",
                "head_node_type": "headgroup",
            },
            k8s_api_client=mock_client,
        )

        # Identify all pods in the multi-host group
        pod_names = []
        for pod in mock_client._pod_list["items"]:
            if pod["metadata"]["labels"]["ray.io/group"] == "tpu-group":
                pod_names.append(pod["metadata"]["name"])

        # Expect 2 pods since replicas=1 and numOfHosts=2
        assert len(pod_names) == 2, "Expected 2 pods in the multi-host group."

        # Sync provider state and mark all pods for deletion
        provider._sync_with_api_server()
        cur_instance_ids = set(provider.instances.keys())
        pods_to_terminate = [name for name in pod_names if name in cur_instance_ids]

        assert (
            len(pods_to_terminate) == 2
        ), "Expected all multi-host pods to be tracked by the provider."

        # Terminate all pods in the group
        provider.terminate(ids=pods_to_terminate, request_id="term-multi")

        # Check that scale request successfully created
        patches = mock_client.get_patches(f"rayclusters/{provider._cluster_name}")

        assert len(patches) == 2
        assert patches == [
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/2/replicas",
                "value": 0,
            },
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/2/scaleStrategy",
                "value": {
                    "workersToDelete": pods_to_terminate,
                },
            },
        ]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
