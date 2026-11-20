import logging
import os
import sys
import time
import unittest

# coding: utf-8
# coding: utf-8
from collections import defaultdict
from typing import Any, Dict, List, Optional, Union
from unittest import mock
from unittest.mock import MagicMock

import pytest  # noqa
import requests

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
    IDLE_SUSPEND_KEY,
    IDLE_TERMINATION_CLEANUP_FINALIZER,
    IDLE_TERMINATION_OPTIONS_KEY,
    KUBERAY_LABEL_KEY_REPLICA_NAME,
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
    ReconcileStepContext,
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
    timeout_s = 30 if provider_name == "fake_multi" else 10
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

    wait_for_condition(verify, timeout=timeout_s)

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

    wait_for_condition(verify, timeout=timeout_s)


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
        self._deletes = []

    def get(self, path: str) -> Dict[str, Any]:
        if "pods" in path:
            return self._pod_list
        if "rayclusters" in path:
            return self._ray_cluster

        raise NotImplementedError(f"get {path}")

    def patch(
        self,
        path: str,
        patches: Union[List[Dict[str, Any]], Dict[str, Any]],
        content_type: str = "application/json-patch+json",
    ):
        self._patches[path] = patches
        return {path: patches}

    def get_patches(self, path: str) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        return self._patches[path]

    def delete(self, path: str) -> Dict[str, Any]:
        self._deletes.append(path)
        return {}


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
            gcs_client=MagicMock(),
            k8s_api_client=self.mock_client,
        )
        # In production _sync_with_api_server caches the CR before the
        # no-driver annotation is set; mirror that for the dispatch tests.
        self.provider._ray_cluster = raycluster_cr

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

    def test_launch_total_max_replicas_cap_fails_and_caches_request(self):
        """A fully capped launch is cached and reports LaunchNodeError."""
        self.mock_client._ray_cluster["spec"]["workerGroupSpecs"][0]["replicas"] = 1
        self.mock_client._ray_cluster["spec"]["workerGroupSpecs"][0]["maxReplicas"] = 1

        self.provider.launch(shape={"small-group": 2}, request_id="launch-cap")

        assert "launch-cap" in self.provider._requests
        assert self.mock_client._patches == {}

        errors = self.provider.poll_errors()
        assert len(errors) == 1
        assert isinstance(errors[0], LaunchNodeError)
        assert errors[0].request_id == "launch-cap"
        assert errors[0].node_type == "small-group"
        assert errors[0].count == 2
        assert "maxReplicas" in str(errors[0])

        # Replay of the same request id must not try again.
        self.provider.launch(shape={"small-group": 2}, request_id="launch-cap")
        assert self.provider.poll_errors() == []
        assert self.mock_client._patches == {}

    def test_launch_partial_max_replicas_cap_fails_leftover(self):
        """A partial cap patches what fits, caches the id, and fails the leftover."""
        self.mock_client._ray_cluster["spec"]["workerGroupSpecs"][0]["replicas"] = 1
        self.mock_client._ray_cluster["spec"]["workerGroupSpecs"][0]["maxReplicas"] = 2

        self.provider.launch(shape={"small-group": 2}, request_id="launch-partial")

        assert "launch-partial" in self.provider._requests
        patches = self.mock_client.get_patches(
            f"rayclusters/{self.provider._cluster_name}"
        )
        assert patches == [
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/0/replicas",
                "value": 2,
            }
        ]

        errors = self.provider.poll_errors()
        assert len(errors) == 1
        assert isinstance(errors[0], LaunchNodeError)
        assert errors[0].request_id == "launch-partial"
        assert errors[0].node_type == "small-group"
        assert errors[0].count == 1

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

    def test_decrease_cr_replicas_below_observed_then_scale_down(self):
        """
        If a user/operator decreases the CR's replicas below the observed number of
        Pods without specifying workersToDelete, scaling down should base the
        new desired on observed (floor), decrement by one, and add the pod to
        workersToDelete.
        """
        # Prepare a RayCluster CR with replicas set to 0 for the small-group
        # while the pod list contains multiple small-group pods.
        raycluster_cr = get_basic_ray_cr()
        mock_client = MockKubernetesHttpApiClient(
            _get_test_yaml("podlist2.yaml"), raycluster_cr
        )

        small_group = "small-group"
        pod_names = []
        for pod in mock_client._pod_list["items"]:
            if pod["metadata"]["labels"]["ray.io/group"] == small_group:
                pod_names.append(pod["metadata"]["name"])
        assert len(pod_names) >= 2

        # Decrease CR replicas below observed without workersToDelete.
        assert raycluster_cr["spec"]["workerGroupSpecs"][0]["groupName"] == small_group
        raycluster_cr["spec"]["workerGroupSpecs"][0]["replicas"] = 0

        provider = KubeRayProvider(
            cluster_name="test",
            provider_config={
                "namespace": "default",
                "head_node_type": "headgroup",
            },
            gcs_client=MagicMock(),
            k8s_api_client=mock_client,
        )

        # Terminate a single observed pod.
        pod_to_delete = pod_names[0]
        provider.terminate(ids=[pod_to_delete], request_id="term-decrease")

        # Expected: replicas becomes observed-1; workersToDelete contains the pod.
        patches = mock_client.get_patches(f"rayclusters/{provider._cluster_name}")
        assert len(patches) == 2
        assert patches == [
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/0/replicas",
                "value": len(pod_names) - 1,
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
        mock_client = MockKubernetesHttpApiClient(
            _get_test_yaml("podlist2.yaml"), raycluster_cr
        )
        provider = KubeRayProvider(
            cluster_name="test",
            provider_config={
                "namespace": "default",
                "head_node_type": "headgroup",
            },
            gcs_client=MagicMock(),
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

    def _overwrite_patch_to_persist_finalizer(self) -> None:
        """Overwrite mock_client.patch() to behave like a real K8s API server for
        the RayCluster finalizer patch. Add the finalizer to the cached CR and
        return the patched resource.
        """

        def patch_and_persist(
            path: str,
            patches: Union[List[Dict[str, Any]], Dict[str, Any]],
            content_type="application/json-patch+json",
        ):
            self.mock_client._patches[path] = patches  # what the default patch() does
            self.provider._ray_cluster.setdefault("metadata", {}).setdefault(
                "finalizers", []
            ).append(IDLE_TERMINATION_CLEANUP_FINALIZER)
            return self.provider._ray_cluster

        self.mock_client.patch = patch_and_persist

    def test_apply_idle_termination_policy_delete_adds_finalizer_then_deletes(self):
        self.provider._idle_termination_policy = "Delete"
        self._overwrite_patch_to_persist_finalizer()
        path = f"rayclusters/{self.provider._cluster_name}"
        self.provider._apply_idle_termination_policy()
        patch = self.mock_client.get_patches(path)

        assert patch == [
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": "123456",
            },
            {
                "op": "add",
                "path": "/metadata/finalizers",
                "value": [IDLE_TERMINATION_CLEANUP_FINALIZER],
            },
        ]
        assert self.mock_client._deletes == [path]

    def test_apply_idle_termination_policy_delete_finalizer_idempotent(self):
        self.provider._ray_cluster.setdefault("metadata", {})["finalizers"] = [
            IDLE_TERMINATION_CLEANUP_FINALIZER
        ]
        self.provider._idle_termination_policy = "Delete"
        self.provider._apply_idle_termination_policy()
        path = f"rayclusters/{self.provider._cluster_name}"
        # Finalizer already present: no patch needed, only the delete.
        assert path not in self.mock_client._patches
        assert self.mock_client._deletes == [path]

    def test_apply_idle_termination_policy_delete_finalizer_appends_when_others_present(
        self,
    ):
        other_finalizer = "GCSFTFinalizer"  # e.g. set by kuberay-operator
        self.provider._ray_cluster.setdefault("metadata", {})["finalizers"] = [
            other_finalizer
        ]
        self._overwrite_patch_to_persist_finalizer()
        path = f"rayclusters/{self.provider._cluster_name}"
        self.provider._idle_termination_policy = "Delete"
        self.provider._apply_idle_termination_policy()

        assert self.mock_client.get_patches(path) == [
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": "123456",
            },
            {
                "op": "add",
                "path": "/metadata/finalizers/-",
                "value": IDLE_TERMINATION_CLEANUP_FINALIZER,
            },
        ]
        # The pre-existing finalizer must survive the append, not get clobbered.
        assert self.provider._ray_cluster["metadata"]["finalizers"] == [
            other_finalizer,
            IDLE_TERMINATION_CLEANUP_FINALIZER,
        ]
        assert self.mock_client._deletes == [path]

    def test_apply_idle_termination_policy_delete_swallows_patch_failure(self):
        path = f"rayclusters/{self.provider._cluster_name}"

        def failing_patch(*args, **kwargs):
            raise RuntimeError("k8s unreachable")

        self.mock_client.patch = failing_patch
        self.provider._idle_termination_policy = "Delete"
        # Should not raise, and should not proceed to delete.
        self.provider._apply_idle_termination_policy()
        assert path not in self.mock_client._patches
        assert self.mock_client._deletes == []

    def test_apply_idle_termination_policy_delete_swallows_delete_failure(self):
        self._overwrite_patch_to_persist_finalizer()
        path = f"rayclusters/{self.provider._cluster_name}"

        delete_calls = []

        def failing_delete(path):
            delete_calls.append(path)
            raise RuntimeError("k8s unreachable")

        self.mock_client.delete = failing_delete
        self.provider._idle_termination_policy = "Delete"
        # Should not raise, even though DELETE itself failed.
        self.provider._apply_idle_termination_policy()

        assert delete_calls == [path]
        assert self.mock_client.get_patches(path) == [
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": "123456",
            },
            {
                "op": "add",
                "path": "/metadata/finalizers",
                "value": [IDLE_TERMINATION_CLEANUP_FINALIZER],
            },
        ]

    def test_apply_idle_termination_policy_delete_skips_delete_on_patch_conflict(self):
        def conflicting_patch(*args, **kwargs):
            resp = requests.Response()
            resp.status_code = 422
            raise requests.HTTPError(response=resp)

        self.mock_client.patch = conflicting_patch
        self.provider._idle_termination_policy = "Delete"
        self.provider._apply_idle_termination_policy()

        assert self.mock_client._deletes == []

    def test_apply_idle_termination_policy_suspend_patches_idle_suspend(self):
        self.provider._idle_termination_policy = "Suspend"
        self.provider._apply_idle_termination_policy()
        path = f"rayclusters/{self.provider._cluster_name}"
        assert self.mock_client.get_patches(path) == {"spec": {IDLE_SUSPEND_KEY: True}}
        assert self.mock_client._deletes == []

    def test_apply_idle_termination_policy_suspend_patches_idempotent(self):
        self.provider._idle_termination_policy = "Suspend"
        self.provider._ray_cluster.setdefault("spec", {}).setdefault(
            IDLE_SUSPEND_KEY, True
        )
        self.provider._apply_idle_termination_policy()
        path = f"rayclusters/{self.provider._cluster_name}"
        assert path not in self.mock_client._patches
        assert self.mock_client._deletes == []

    def test_apply_idle_termination_policy_suspend_swallows_patch_failure(self):
        path = f"rayclusters/{self.provider._cluster_name}"

        def failing_patch(*args, **kwargs):
            raise RuntimeError("k8s unreachable")

        self.mock_client.patch = failing_patch
        self.provider._idle_termination_policy = "Suspend"
        # Should not raise.
        self.provider._apply_idle_termination_policy()
        assert path not in self.mock_client._patches
        assert self.mock_client._deletes == []

    def test_apply_idle_termination_policy_unknown_takes_no_action(self):
        self.provider._idle_termination_policy = "Bogus"
        self.provider._apply_idle_termination_policy()
        path = f"rayclusters/{self.provider._cluster_name}"
        assert path not in self.mock_client._patches
        assert self.mock_client._deletes == []

    # --- No-driver termination predicate + dispatch ---

    def _make_gcs(self, *jobs):
        class _Config:
            def __init__(self, ray_namespace):
                self.ray_namespace = ray_namespace

        class _Job:
            def __init__(self, dead, ray_namespace, end_time=0):
                self.is_dead = dead
                self.end_time = end_time
                self.config = _Config(ray_namespace)

        class _Gcs:
            def get_all_job_info(self, **_):
                return {i: _Job(*job) for i, job in enumerate(jobs)}

        return _Gcs()

    def test_driver_status_filters_internal(self):
        gcs = self._make_gcs(
            (False, "_ray_internal_dashboard"),
            (False, "_ray_internal_something"),
        )
        self.provider._gcs_client = gcs
        assert self.provider._driver_status()[0] is False

    def test_driver_status_counts_user_driver(self):
        gcs = self._make_gcs(
            (False, "_ray_internal_dashboard"),
            (False, "default"),
        )
        self.provider._gcs_client = gcs
        assert self.provider._driver_status()[0] is True

    def test_driver_status_ignores_dead(self):
        gcs = self._make_gcs((True, "default", 42))
        self.provider._gcs_client = gcs
        assert self.provider._driver_status() == (False, 42)

    def test_driver_status_fail_closed(self):
        class _FailingGcs:
            def get_all_job_info(self, **_):
                raise RuntimeError("gcs unreachable")

        self.provider._gcs_client = _FailingGcs()
        assert self.provider._driver_status()[0] is True

    def test_evaluate_idle_termination_disabled_when_timeout_none(self):
        path = f"rayclusters/{self.provider._cluster_name}"
        self.provider._gcs_client = self._make_gcs()  # no drivers
        self.provider._idle_termination_timeout_seconds = None
        self.provider._evaluate_idle_termination()
        assert path not in self.mock_client._patches
        assert self.provider._no_driver_observed_since is None

    def test_evaluate_idle_termination_waits_for_timeout(self):
        self.provider._gcs_client = self._make_gcs()  # no drivers
        self.provider._idle_termination_timeout_seconds = 100.0
        self.provider._idle_termination_policy = "Delete"

        self._overwrite_patch_to_persist_finalizer()

        def evaluate_at(t):
            with mock.patch("time.monotonic", return_value=t):
                self.provider._evaluate_idle_termination()

        path = f"rayclusters/{self.provider._cluster_name}"
        evaluate_at(0.0)
        assert path not in self.mock_client._patches  # anchored, not yet
        evaluate_at(50.0)
        assert path not in self.mock_client._patches  # still below timeout
        evaluate_at(100.0)
        assert self.mock_client._patches.get(path) == [
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": "123456",
            },
            {
                "op": "add",
                "path": "/metadata/finalizers",
                "value": [IDLE_TERMINATION_CLEANUP_FINALIZER],
            },
        ]
        assert self.mock_client._deletes == [path]

    def test_evaluate_idle_termination_resets_when_driver_attaches(self):
        path = f"rayclusters/{self.provider._cluster_name}"
        self.provider._idle_termination_timeout_seconds = 100.0

        with mock.patch("time.monotonic", return_value=0.0):
            self.provider._gcs_client = self._make_gcs()  # no drivers
            self.provider._evaluate_idle_termination()
        assert self.provider._no_driver_observed_since == 0.0

        # Driver attaches → anchor cleared, no patch.
        with mock.patch("time.monotonic", return_value=50.0):
            self.provider._gcs_client = self._make_gcs((False, "default"))
            self.provider._evaluate_idle_termination()
        assert self.provider._no_driver_observed_since is None
        assert path not in self.mock_client._patches

    def test_evaluate_idle_termination_resets_on_intermittent_driver(self):
        self.provider._idle_termination_timeout_seconds = 100.0

        # No driver: anchor at t=0.
        with mock.patch("time.monotonic", return_value=0.0):
            self.provider._gcs_client = self._make_gcs()
            self.provider._evaluate_idle_termination()
        assert self.provider._no_driver_observed_since == 0.0

        # A short-lived driver started and finished between loops (a dead job
        # with a newer end time): the timer must restart.
        with mock.patch("time.monotonic", return_value=50.0):
            self.provider._gcs_client = self._make_gcs((True, "default", 42))
            self.provider._evaluate_idle_termination()
        assert self.provider._no_driver_observed_since == 50.0
        assert self.provider._last_seen_job_end_time == 42

    def test_refresh_idle_termination_config_reads_value(self):
        self.provider._ray_cluster = {
            "spec": {
                "idleTerminationOptions": {
                    "timeoutSeconds": 1800,
                    "policy": "Suspend",
                }
            }
        }
        self.provider._refresh_idle_termination_config()
        assert self.provider._idle_termination_timeout_seconds == 1800.0
        assert self.provider._idle_termination_policy == "Suspend"

    def test_refresh_idle_termination_config_unset(self):
        self.provider._ray_cluster = {"spec": {IDLE_TERMINATION_OPTIONS_KEY: {}}}
        self.provider._refresh_idle_termination_config()
        assert self.provider._idle_termination_timeout_seconds is None
        assert self.provider._idle_termination_policy == "Suspend"

    def test_refresh_idle_termination_config_policy_defaults_to_suspend(self):
        self.provider._ray_cluster = {
            "spec": {"idleTerminationOptions": {"timeoutSeconds": 1800}}
        }
        self.provider._refresh_idle_termination_config()
        assert self.provider._idle_termination_timeout_seconds == 1800.0
        assert self.provider._idle_termination_policy == "Suspend"

    def test_refresh_idle_termination_config_no_idle_termination_options(self):
        self.provider._ray_cluster = {"spec": {}}
        self.provider._refresh_idle_termination_config()
        assert self.provider._idle_termination_timeout_seconds is None

    def test_scale_down_with_multi_host_group(self):
        """
        Test the case where a worker group has numOfHosts > 1.
        This ensures that the KubeRay provider accounts for multi-host replicas
        during scale down and properly updates the workersToDelete field.
        """
        # Setup mock RayCluster CR with numOfHosts: 2 and replicas: 1,
        # resulting in 2 workers total.
        raycluster_cr = get_basic_ray_cr()
        raycluster_cr["spec"]["workerGroupSpecs"][0]["replicas"] = 2
        mock_client = MockKubernetesHttpApiClient(
            _get_test_yaml("podlist2.yaml"), raycluster_cr
        )
        provider = KubeRayProvider(
            cluster_name="test",
            provider_config={
                "namespace": "default",
                "head_node_type": "headgroup",
            },
            gcs_client=MagicMock(),
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

        # When minReplicas >= 1, deleting dead pods via workersToDelete never patches
        # replicas below minReplicas (allowing KubeRay to recreate a healthy replica).
        raycluster_min1 = get_basic_ray_cr()
        raycluster_min1["spec"]["workerGroupSpecs"][0]["replicas"] = 2
        raycluster_min1["spec"]["workerGroupSpecs"][2]["minReplicas"] = 1
        raycluster_min1["spec"]["workerGroupSpecs"][2]["replicas"] = 1
        mock_client_min1 = MockKubernetesHttpApiClient(
            _get_test_yaml("podlist2.yaml"), raycluster_min1
        )
        provider_min1 = KubeRayProvider(
            cluster_name="test",
            provider_config={"namespace": "default", "head_node_type": "headgroup"},
            gcs_client=MagicMock(),
            k8s_api_client=mock_client_min1,
        )
        provider_min1._sync_with_api_server()
        provider_min1.terminate(ids=pods_to_terminate, request_id="term-min1")
        assert mock_client_min1.get_patches(
            f"rayclusters/{provider_min1._cluster_name}"
        ) == [
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/2/scaleStrategy",
                "value": {"workersToDelete": pods_to_terminate},
            }
        ]

    @staticmethod
    def _make_multihost_pod(
        name: str,
        kind: str = "worker",
        group: str = "tpu-group",
        rep: Optional[str] = None,
    ) -> Dict[str, Any]:
        labels = {
            "ray.io/cluster": "test",
            "ray.io/node-type": kind,
            "ray.io/group": group,
        }
        if rep is not None:
            labels[KUBERAY_LABEL_KEY_REPLICA_NAME] = rep
        return {
            "metadata": {"name": name, "labels": labels},
            "status": {"containerStatuses": [{"state": {"running": {}}}]},
        }

    @classmethod
    def _make_kuberay_provider(
        cls,
        worker_groups: List[Dict[str, Any]],
        pods: List[Dict[str, Any]],
        head_rep: Optional[str] = None,
    ):
        class _CountingK8sClient(MockKubernetesHttpApiClient):
            def __init__(self, pod_list, ray_cluster):
                super().__init__(pod_list, ray_cluster)
                self.get_calls = []
                self.patch_history = []

            def get(self, path: str) -> Dict[str, Any]:
                self.get_calls.append(path)
                return super().get(path)

            def patch(
                self,
                path: str,
                patches: Union[List[Dict[str, Any]], Dict[str, Any]],
                content_type: str = "application/json-patch+json",
            ):
                self.patch_history.append(list(patches))
                return super().patch(path, patches, content_type)

        cr = get_basic_ray_cr()
        cr["spec"]["workerGroupSpecs"] = worker_groups
        plist = {
            "metadata": {"resourceVersion": "1"},
            "items": [
                cls._make_multihost_pod(
                    "head-0", kind="head", group="headgroup", rep=head_rep
                )
            ]
            + pods,
        }
        client = _CountingK8sClient(plist, cr)
        prov = KubeRayProvider(
            cluster_name="test",
            provider_config={"namespace": "default", "head_node_type": "headgroup"},
            gcs_client=MagicMock(),
            k8s_api_client=client,
        )
        return prov, client

    @classmethod
    def _make_tpu_group_provider(
        cls,
        replicas: int,
        min_rep: int,
        pods: List[Dict[str, Any]],
        max_rep: int = 10,
        num_of_hosts: int = 4,
    ):
        return cls._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "tpu-group",
                    "replicas": replicas,
                    "minReplicas": min_rep,
                    "maxReplicas": max_rep,
                    "numOfHosts": num_of_hosts,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            pods=pods,
            head_rep="ign",
        )

    def test_multihost_replica_id_extraction(self):
        """KubeRay v1.6+ replica label sets CloudInstance.replica_id on workers only."""
        _pod = self._make_multihost_pod
        prov, _ = self._make_tpu_group_provider(
            replicas=1,
            min_rep=0,
            pods=[
                *(_pod(f"r0-{i}", rep="test-tpu-worker-abc") for i in range(4)),
                _pod("r1-boot-0", rep="test-tpu-worker-def"),
                _pod("empty-lbl", rep=""),
            ],
        )
        nodes = prov.get_non_terminated()
        assert nodes["head-0"].replica_id is None
        assert nodes["r0-0"].replica_id == "test-tpu-worker-abc"
        assert nodes["r1-boot-0"].replica_id == "test-tpu-worker-def"
        assert nodes["empty-lbl"].replica_id is None

    def test_multihost_terminate_preserves_partial_surviving_replicas(self):
        """Scale-down counts whole replicas so mid-boot survivors are not truncated."""
        _pod = self._make_multihost_pod
        prov_sd, client_sd = self._make_tpu_group_provider(
            replicas=1,
            min_rep=0,
            pods=[*(_pod(f"r0-{i}", rep="rep-0") for i in range(4))]
            + [_pod("r1-0", rep="rep-1")],
        )
        prov_sd.terminate(ids=[f"r0-{i}" for i in range(4)], request_id="sd-1")
        assert client_sd.get_patches("rayclusters/test") == [
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/0/scaleStrategy",
                "value": {"workersToDelete": [f"r0-{i}" for i in range(4)]},
            }
        ]

        prov_deg, client_deg = self._make_tpu_group_provider(
            replicas=3,
            min_rep=0,
            pods=[
                _pod("r0-0", rep="rep-0"),
                _pod("r0-1", rep="rep-0"),
                _pod("r1-0", rep="rep-1"),
                _pod("r2-0", rep="rep-2"),
                _pod("r2-1", rep="rep-2"),
            ],
        )
        prov_deg.terminate(ids=["r0-0", "r0-1", "r1-0"], request_id="sd-deg")
        assert client_deg.get_patches("rayclusters/test") == [
            {"op": "replace", "path": "/spec/workerGroupSpecs/0/replicas", "value": 1},
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/0/scaleStrategy",
                "value": {"workersToDelete": ["r0-0", "r0-1", "r1-0"]},
            },
        ]

    def test_multihost_terminate_expands_to_full_replicas_and_blocks_mid_delete(self):
        """Terminating any pod of a labeled replica expands workersToDelete to all
        siblings so mid-deletion pod lists keep blocking premature launches."""
        _pod = self._make_multihost_pod
        prov_z, client_z = self._make_tpu_group_provider(
            replicas=2,
            min_rep=0,
            pods=[*(_pod(f"r0-{i}", rep="rep-0") for i in range(4))]
            + [*(_pod(f"r1-{i}", rep="rep-1") for i in range(4))],
        )
        prov_z.terminate(
            ids=["r0-2", "r0-3", "r1-2", "r1-3"], request_id="evict-faulted-pods"
        )
        assert client_z.get_patches("rayclusters/test") == [
            {"op": "replace", "path": "/spec/workerGroupSpecs/0/replicas", "value": 0},
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/0/scaleStrategy",
                "value": {
                    "workersToDelete": [
                        "r0-2",
                        "r0-3",
                        "r1-2",
                        "r1-3",
                        "r0-0",
                        "r0-1",
                        "r1-0",
                        "r1-1",
                    ]
                },
            },
        ]

        prov_half, client_half = self._make_tpu_group_provider(
            replicas=2,
            min_rep=0,
            pods=[*(_pod(f"r0-{i}", rep="rep-0") for i in range(4))]
            + [*(_pod(f"r1-{i}", rep="rep-1") for i in range(4))],
        )
        prov_half.terminate(ids=["r0-2"], request_id="term-single")
        for op in client_half.get_patches("rayclusters/test"):
            _, _, _, idx, field = op["path"].split("/")
            client_half._ray_cluster["spec"]["workerGroupSpecs"][int(idx)][field] = op[
                "value"
            ]
        client_half._patches.clear()
        client_half._pod_list["items"] = [
            p for p in client_half._pod_list["items"] if p["metadata"]["name"] != "r0-2"
        ]
        prov_half.launch(shape={"tpu-group": 4}, request_id="launch-mid-del")
        assert client_half._patches == {}
        assert [(e.node_type, e.count) for e in prov_half.poll_errors()] == [
            ("tpu-group", 4)
        ]

    def test_multihost_launch_partial_and_unaligned_counts(self):
        """Scale-up preserves partial existing replicas, keeps legacy standalone
        unaligned launches quiet (R1), and reports unaligned remainders only for
        replica-managed groups in a reconcile step."""
        _pod = self._make_multihost_pod
        prov_up, client_up = self._make_tpu_group_provider(
            replicas=0,
            min_rep=0,
            pods=[_pod("r0-0", rep="rep-0"), _pod("r0-1", rep="rep-0")],
        )
        prov_up.launch(shape={"tpu-group": 4}, request_id="up-1")
        assert client_up.get_patches("rayclusters/test") == [
            {"op": "replace", "path": "/spec/workerGroupSpecs/0/replicas", "value": 2}
        ]

        # Standalone / legacy multi-host without replica_config does NOT emit
        # LaunchNodeError on unaligned counts (R1).
        prov_legacy, client_legacy = self._make_tpu_group_provider(
            replicas=0, min_rep=0, pods=[]
        )
        prov_legacy.launch(shape={"tpu-group": 6}, request_id="up-legacy")
        assert client_legacy.get_patches("rayclusters/test") == [
            {"op": "replace", "path": "/spec/workerGroupSpecs/0/replicas", "value": 1}
        ]
        assert prov_legacy.poll_errors() == []

        # Replica-managed group inside reconcile_step DOES report unaligned remainder.
        prov_unaligned, client_unaligned = self._make_tpu_group_provider(
            replicas=0, min_rep=0, pods=[]
        )
        with prov_unaligned.reconcile_step(
            ReconcileStepContext(
                known_cloud_instance_ids={"head-0"},
                unclaimed_replicas_by_lid={"tpu-group": {}},
            )
        ):
            prov_unaligned.launch(shape={"tpu-group": 6}, request_id="up-unaligned")
        assert client_unaligned.get_patches("rayclusters/test") == [
            {"op": "replace", "path": "/spec/workerGroupSpecs/0/replicas", "value": 1}
        ]
        errs = prov_unaligned.poll_errors()
        assert len(errs) == 1
        assert errs[0].node_type == "tpu-group"
        assert errs[0].count == 2
        assert errs[0].request_id == "up-unaligned"

    def test_multihost_mixed_and_unlabeled_replica_scaling(self):
        """Mixed labeled/unlabeled accounting and per-pod unlabeled removal so
        multi-tick unlabeled scale-down converges cleanly."""
        _pod = self._make_multihost_pod
        prov_stray, client_stray = self._make_tpu_group_provider(
            replicas=1,
            min_rep=0,
            pods=[*(_pod(f"r0-{i}", rep="rep-0") for i in range(4))]
            + [_pod("stray", rep=None)],
        )
        prov_stray.launch(shape={"tpu-group": 4}, request_id="up-stray")
        assert client_stray.get_patches("rayclusters/test") == [
            {"op": "replace", "path": "/spec/workerGroupSpecs/0/replicas", "value": 3}
        ]
        assert prov_stray.poll_errors() == []

        prov_mix_del, client_mix_del = self._make_tpu_group_provider(
            replicas=2,
            min_rep=0,
            pods=[*(_pod(f"r0-{i}", rep="rep-0") for i in range(4))]
            + [_pod("legacy-0", rep=None), _pod("legacy-1", rep=None)],
        )
        prov_mix_del.terminate(ids=["r0-0"], request_id="del-mix")
        assert client_mix_del.get_patches("rayclusters/test") == [
            {"op": "replace", "path": "/spec/workerGroupSpecs/0/replicas", "value": 1},
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/0/scaleStrategy",
                "value": {"workersToDelete": ["r0-0", "r0-1", "r0-2", "r0-3"]},
            },
        ]

        # Deleting unlabeled pods when spec.replicas was manually lowered to 0
        # clamps cleanly to 0 without asserting.
        prov_neg, client_neg = self._make_tpu_group_provider(
            replicas=0, min_rep=0, pods=[_pod("legacy-0", rep=None)]
        )
        prov_neg.terminate(ids=["legacy-0"], request_id="del-legacy")
        assert client_neg.get_patches("rayclusters/test") == [
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/0/scaleStrategy",
                "value": {"workersToDelete": ["legacy-0"]},
            }
        ]

        # Unlabeled multi-host scale-down across two ticks converges to 0
        # (removing 3 of 4 pods lowers spec.replicas from 1 -> 0 via floor div).
        prov_conv, client_conv = self._make_tpu_group_provider(
            replicas=1,
            min_rep=0,
            pods=[_pod(f"u-{i}", rep=None) for i in range(4)],
        )
        prov_conv.terminate(ids=["u-0", "u-1", "u-2"], request_id="leg-3of4")
        assert client_conv.get_patches("rayclusters/test") == [
            {"op": "replace", "path": "/spec/workerGroupSpecs/0/replicas", "value": 0},
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/0/scaleStrategy",
                "value": {"workersToDelete": ["u-0", "u-1", "u-2"]},
            },
        ]

        # Unlabeled 1-pod removal from 8 pods lowers replicas 2 -> 1 (matches base).
        prov_leg_1, client_leg_1 = self._make_tpu_group_provider(
            replicas=2,
            min_rep=0,
            pods=[_pod(f"u-{i}", rep=None) for i in range(8)],
        )
        prov_leg_1.terminate(ids=["u-0"], request_id="leg-1pod")
        assert client_leg_1.get_patches("rayclusters/test") == [
            {"op": "replace", "path": "/spec/workerGroupSpecs/0/replicas", "value": 1},
            {
                "op": "replace",
                "path": "/spec/workerGroupSpecs/0/scaleStrategy",
                "value": {"workersToDelete": ["u-0"]},
            },
        ]

        # Unlabeled scale-up preserves partial unlabeled replicas via ceil units.
        prov_leg_up, client_leg_up = self._make_tpu_group_provider(
            replicas=1,
            min_rep=0,
            pods=[_pod(f"u-{i}", rep=None) for i in range(7)],
        )
        prov_leg_up.launch(shape={"tpu-group": 4}, request_id="leg-up")
        assert client_leg_up.get_patches("rayclusters/test") == [
            {"op": "replace", "path": "/spec/workerGroupSpecs/0/replicas", "value": 3}
        ]
        assert prov_leg_up.poll_errors() == []

    def test_reconcile_step_single_sync_and_untargeted_group_isolation(self):
        """A reconcile step fetches CR + pods once across multiple calls and leaves
        untargeted or zero-count groups untouched."""
        prov, client = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "cpu",
                    "replicas": 5,
                    "minReplicas": 0,
                    "maxReplicas": 10,
                    "template": {"spec": {"containers": [{}]}},
                },
                {
                    "groupName": "tpu-a",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": 5,
                    "numOfHosts": 4,
                    "template": {"spec": {"containers": [{}]}},
                },
            ],
            pods=[],
        )
        with prov.reconcile_step(
            ReconcileStepContext(
                known_cloud_instance_ids={"head-0"},
                unclaimed_replicas_by_lid={"tpu-a": {}},
            )
        ):
            assert prov.get_node_types_with_pending_deletes() == set()
            # A zero-count entry for cpu must not overwrite cpu's spec.replicas=5.
            prov.launch(shape={"cpu": 0, "tpu-a": 4}, request_id="rep-req-1")
            prov.launch(shape={"tpu-a": 4}, request_id="rep-req-2")
        assert len(client.get_calls) == 2
        assert client.patch_history == [
            [
                {
                    "op": "replace",
                    "path": "/spec/workerGroupSpecs/1/replicas",
                    "value": 1,
                }
            ],
            [
                {
                    "op": "replace",
                    "path": "/spec/workerGroupSpecs/1/replicas",
                    "value": 2,
                }
            ],
        ]
        assert prov.poll_errors() == []

    def test_reconcile_step_max_replicas_and_pending_delete_group_isolation(self):
        """In-step launches respect maxReplicas with partial existing replicas, and
        pending deletes isolate only the affected worker group."""
        _pod = self._make_multihost_pod
        prov2, client2 = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "tpu-a",
                    "replicas": 2,
                    "minReplicas": 1,
                    "maxReplicas": 3,
                    "numOfHosts": 4,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            pods=[*(_pod(f"r0-{i}", group="tpu-a", rep="rep-0") for i in range(3))]
            + [*(_pod(f"r1-{i}", group="tpu-a", rep="rep-1") for i in range(4))],
        )
        known2 = {
            "head-0",
            *(f"r0-{i}" for i in range(3)),
            *(f"r1-{i}" for i in range(4)),
        }
        with prov2.reconcile_step(
            ReconcileStepContext(
                known_cloud_instance_ids=known2,
                unclaimed_replicas_by_lid={"tpu-a": {}},
            )
        ):
            prov2.launch(shape={"tpu-a": 4}, request_id="heal-req-1")
            assert prov2.poll_errors() == []
            prov2.launch(shape={"tpu-a": 4}, request_id="heal-req-2")
            assert [
                (e.request_id, e.node_type, e.count) for e in prov2.poll_errors()
            ] == [("heal-req-2", "tpu-a", 4)]
        assert client2.patch_history == [
            [{"op": "replace", "path": "/spec/workerGroupSpecs/0/replicas", "value": 3}]
        ]

        prov3, client3 = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "tpu-a",
                    "replicas": 1,
                    "minReplicas": 0,
                    "maxReplicas": 4,
                    "numOfHosts": 2,
                    "scaleStrategy": {"workersToDelete": ["a-0"]},
                    "template": {"spec": {"containers": [{}]}},
                },
                {
                    "groupName": "tpu-b",
                    "replicas": 1,
                    "minReplicas": 0,
                    "maxReplicas": 4,
                    "numOfHosts": 2,
                    "template": {"spec": {"containers": [{}]}},
                },
            ],
            pods=[
                _pod("a-0", group="tpu-a", rep="ra-0"),
                _pod("a-1", group="tpu-a", rep="ra-0"),
                _pod("b-0", group="tpu-b", rep="rb-0"),
                _pod("b-1", group="tpu-b", rep="rb-0"),
            ],
        )
        assert prov3.get_node_types_with_pending_deletes() == {"tpu-a"}
        assert prov3.get_node_types_with_pending_deletes({"b-0"}) == {"tpu-a", "tpu-b"}
        prov3.launch(shape={"tpu-b": 2}, request_id="b-up")
        assert prov3.poll_errors() == []
        assert client3.patch_history[-1] == [
            {"op": "replace", "path": "/spec/workerGroupSpecs/1/replicas", "value": 2}
        ]
        prov3.launch(shape={"tpu-a": 2}, request_id="a-up")
        assert [(e.request_id, e.node_type, e.count) for e in prov3.poll_errors()] == [
            ("a-up", "tpu-a", 2)
        ]
        prov3.terminate(ids=["a-1"], request_id="a-del")
        assert [e.cloud_instance_id for e in prov3.poll_errors()] == ["a-1"]
        prov3.terminate(ids=["b-0"], request_id="b-del")
        assert prov3.poll_errors() == []

    def test_reconcile_step_legacy_multihost_and_invalid_num_of_hosts(self):
        """Legacy numOfHosts>1 groups preserve additive launches, and numOfHosts<=0
        fails both launches and terminations cleanly without patching workersToDelete."""
        _pod = self._make_multihost_pod
        prov4, client4 = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "tpu-a",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": 5,
                    "numOfHosts": 4,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            pods=[],
        )
        with prov4.reconcile_step(
            ReconcileStepContext(known_cloud_instance_ids={"head-0"})
        ):
            prov4.launch(shape={"tpu-a": 4}, request_id="legacy-1")
            prov4.launch(shape={"tpu-a": 4}, request_id="legacy-2")
        assert client4.patch_history == [
            [
                {
                    "op": "replace",
                    "path": "/spec/workerGroupSpecs/0/replicas",
                    "value": 1,
                }
            ],
            [
                {
                    "op": "replace",
                    "path": "/spec/workerGroupSpecs/0/replicas",
                    "value": 2,
                }
            ],
        ]

        # Split sub-replica launches (1 + 3) on a legacy multi-host group accumulate
        # across in-flight request_ids to scale 0 -> 1 replica without LaunchNodeError.
        prov4_split, client4_split = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "tpu-a",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": 5,
                    "numOfHosts": 4,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            pods=[],
        )
        with prov4_split.reconcile_step(
            ReconcileStepContext(
                known_cloud_instance_ids={"head-0"},
                single_host_in_flight={"tpu-a": {"split-1": 1}},
            )
        ):
            prov4_split.launch(shape={"tpu-a": 3}, request_id="split-2")
        assert prov4_split.poll_errors() == []
        assert client4_split.patch_history == [
            [{"op": "replace", "path": "/spec/workerGroupSpecs/0/replicas", "value": 1}]
        ]

        for raw_bad_hosts in (0, -1):
            for in_step in (False, True):
                prov5, client5 = self._make_kuberay_provider(
                    worker_groups=[
                        {
                            "groupName": "tpu-a",
                            "replicas": 2,
                            "minReplicas": 0,
                            "maxReplicas": 5,
                            "numOfHosts": raw_bad_hosts,
                            "template": {"spec": {"containers": [{}]}},
                        }
                    ],
                    pods=[_pod("bad-0", group="tpu-a"), _pod("bad-1", group="tpu-a")],
                )
                ctx = ReconcileStepContext(
                    known_cloud_instance_ids={"head-0", "bad-0", "bad-1"}
                )
                if in_step:
                    with prov5.reconcile_step(ctx):
                        prov5.launch(shape={"tpu-a": 4}, request_id="bad-hosts")
                else:
                    prov5.launch(shape={"tpu-a": 4}, request_id="bad-hosts")
                assert client5.patch_history == []
                assert [
                    (e.request_id, e.node_type, e.count) for e in prov5.poll_errors()
                ] == [("bad-hosts", "tpu-a", 4)]

                # Terminate also fails closed without patching workersToDelete.
                if in_step:
                    with prov5.reconcile_step(ctx):
                        prov5.terminate(ids=["bad-0"], request_id="bad-term")
                else:
                    prov5.terminate(ids=["bad-0"], request_id="bad-term")
                assert client5.patch_history == []
                term_errs = prov5.poll_errors()
                assert len(term_errs) == 1
                assert isinstance(term_errs[0], TerminateNodeError)
                assert (term_errs[0].cloud_instance_id, term_errs[0].request_id) == (
                    "bad-0",
                    "bad-term",
                )

        # Mixed multi-group terminate: invalid numOfHosts=0 group is rejected while
        # valid sibling group terminates cleanly.
        prov_mix, client_mix = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "bad-grp",
                    "replicas": 1,
                    "minReplicas": 0,
                    "maxReplicas": 5,
                    "numOfHosts": 0,
                    "template": {"spec": {"containers": [{}]}},
                },
                {
                    "groupName": "good-grp",
                    "replicas": 2,
                    "minReplicas": 0,
                    "maxReplicas": 5,
                    "numOfHosts": 1,
                    "template": {"spec": {"containers": [{}]}},
                },
            ],
            pods=[
                _pod("bad-a", group="bad-grp"),
                _pod("good-a", group="good-grp"),
                _pod("good-b", group="good-grp"),
            ],
        )
        with prov_mix.reconcile_step(
            ReconcileStepContext(
                known_cloud_instance_ids={"head-0", "bad-a", "good-a", "good-b"}
            )
        ):
            prov_mix.terminate(ids=["bad-a", "good-a"], request_id="mix-term")
        assert client_mix.patch_history == [
            [
                {
                    "op": "replace",
                    "path": "/spec/workerGroupSpecs/1/replicas",
                    "value": 1,
                },
                {
                    "op": "replace",
                    "path": "/spec/workerGroupSpecs/1/scaleStrategy",
                    "value": {"workersToDelete": ["good-a"]},
                },
            ]
        ]
        assert [e.cloud_instance_id for e in prov_mix.poll_errors()] == ["bad-a"]

        # Explicit `"numOfHosts": None` defaults to 1 host per replica cleanly.
        for in_step in (False, True):
            prov6, client6 = self._make_kuberay_provider(
                worker_groups=[
                    {
                        "groupName": "cpu-none",
                        "replicas": 0,
                        "minReplicas": 0,
                        "maxReplicas": 5,
                        "numOfHosts": None,
                        "template": {"spec": {"containers": [{}]}},
                    }
                ],
                pods=[],
            )
            if in_step:
                with prov6.reconcile_step(
                    ReconcileStepContext(known_cloud_instance_ids={"head-0"})
                ):
                    prov6.launch(shape={"cpu-none": 2}, request_id="none-hosts")
            else:
                prov6.launch(shape={"cpu-none": 2}, request_id="none-hosts")
            assert prov6.poll_errors() == []
            assert client6.patch_history == [
                [
                    {
                        "op": "replace",
                        "path": "/spec/workerGroupSpecs/0/replicas",
                        "value": 2,
                    }
                ]
            ]

    def test_reconcile_step_intra_tick_pod_deduplication(self):
        """Intra-tick newly materialized pods deduplicate against in-flight counts,
        replayed multi-host request_ids are excluded from prior_reps, and unlabeled
        multi-host backfills scale cleanly without LaunchNodeError."""
        _pod = self._make_multihost_pod
        prov7, client7 = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "cpu",
                    "replicas": 1,
                    "minReplicas": 0,
                    "maxReplicas": 5,
                    "template": {"spec": {"containers": [{}]}},
                },
                {
                    "groupName": "tpu-a",
                    "replicas": 2,
                    "minReplicas": 0,
                    "maxReplicas": 5,
                    "numOfHosts": 4,
                    "template": {"spec": {"containers": [{}]}},
                },
            ],
            pods=[
                _pod("cpu-new", group="cpu"),
                _pod("tpu-new-0", group="tpu-a", rep="rep-new"),
                _pod("u-0", group="tpu-a", rep=None),
                _pod("u-1", group="tpu-a", rep=None),
                _pod("u-new-same-rep", group="tpu-a", rep=None),
            ],
        )
        with prov7.reconcile_step(
            ReconcileStepContext(
                known_cloud_instance_ids={"head-0", "u-0", "u-1"},
                single_host_in_flight={"cpu": {"cpu-in-flight": 1}},
                # Pre-seeding tpu-next=1 verifies replayed request_id exclusion.
                unclaimed_replicas_by_lid={
                    "tpu-a": {"tpu-in-flight": 1, "tpu-next": 1}
                },
            )
        ):
            # cpu: 1 observed (new) + max(0, 1 + 1 - 1) = 2 (not 3).
            prov7.launch(shape={"cpu": 1}, request_id="cpu-next")
            # tpu-a: obs = 1 labeled (rep-new, all new -> new_labeled_reps=1) +
            # 1 unlabeled (u-0,u-1 already known -> new_unlabeled_reps=0) = 2 reps;
            # in_flight_reps = max(0, 1 + 1 - 1) = 1 -> desired = 3 replicas (not 4).
            prov7.launch(shape={"tpu-a": 4}, request_id="tpu-next")
        assert client7.patch_history == [
            [
                {
                    "op": "replace",
                    "path": "/spec/workerGroupSpecs/0/replicas",
                    "value": 2,
                }
            ],
            [
                {
                    "op": "replace",
                    "path": "/spec/workerGroupSpecs/1/replicas",
                    "value": 3,
                }
            ],
        ]

        # Unlabeled 1-pod backfill at 11/12 pods scales 2 -> 3 cleanly.
        prov_bf, client_bf = self._make_tpu_group_provider(
            replicas=2,
            min_rep=0,
            max_rep=3,
            num_of_hosts=4,
            pods=[_pod(f"u-{i}", rep=None) for i in range(11)],
        )
        known_bf = set(prov_bf.get_non_terminated())
        with prov_bf.reconcile_step(
            ReconcileStepContext(
                known_cloud_instance_ids=known_bf,
                unclaimed_replicas_by_lid={"tpu-group": {}},
            )
        ):
            prov_bf.launch(shape={"tpu-group": 1}, request_id="bf-1")
        assert prov_bf.poll_errors() == []
        assert client_bf.patch_history == [
            [{"op": "replace", "path": "/spec/workerGroupSpecs/0/replicas", "value": 3}]
        ]

    def test_reconcile_step_not_older_than_and_rv_rollback(self):
        """In-step sync forwards the last pod-list resourceVersion with NotOlderThan
        and recovers cleanly when API-server resourceVersion rolls back (e.g. etcd restore)."""
        _pod = self._make_multihost_pod

        prov, client = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "cpu",
                    "replicas": 5,
                    "minReplicas": 0,
                    "maxReplicas": 10,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            pods=[_pod(f"cpu-{i}", group="cpu") for i in range(5)],
        )
        client._pod_list["metadata"]["resourceVersion"] = "10"
        known = set(prov.get_non_terminated())
        seen_pod_paths = []
        real_get = client.get

        def spy_get(path, _real_get=real_get, _paths=seen_pod_paths):
            if "pods" in path:
                _paths.append(path)
            return _real_get(path)

        client.get = spy_get
        with prov.reconcile_step(ReconcileStepContext(known_cloud_instance_ids=known)):
            prov.launch(shape={"cpu": 1}, request_id="scale-6")
        client.get = real_get
        assert any(
            "resourceVersion=10&resourceVersionMatch=NotOlderThan" in p
            for p in seen_pod_paths
        )
        assert client.patch_history == [
            [
                {
                    "op": "replace",
                    "path": "/spec/workerGroupSpecs/0/replicas",
                    "value": 6,
                }
            ]
        ]

        # API-server resourceVersion rollback (5000 -> 4000, e.g. etcd restore) inside
        # reconcile_step raises on the stale floor once, resets _last_pod_list_rv to
        # None, and succeeds on the next in-step sync at RV 4000.
        prov_rb, client_rb = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "cpu",
                    "replicas": 2,
                    "minReplicas": 0,
                    "maxReplicas": 10,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            pods=[_pod(f"cpu-{i}", group="cpu") for i in range(2)],
        )
        client_rb._pod_list["metadata"]["resourceVersion"] = "5000"
        known_rb = set(prov_rb.get_non_terminated())
        assert prov_rb._last_pod_list_rv == "5000"
        client_rb._pod_list["metadata"]["resourceVersion"] = "4000"
        rb_real_get = client_rb.get
        rv_checks = []

        def strict_rv_get(path):
            if path.startswith("pods?") and "resourceVersion=" in path:
                floor = int(path.split("resourceVersion=")[1].split("&")[0])
                current = int(client_rb._pod_list["metadata"]["resourceVersion"])
                rv_checks.append((floor, current))
                if floor > current:
                    raise RuntimeError(
                        f"504 Too large resource version: {floor}, current: {current}"
                    )
            return rb_real_get(path)

        client_rb.get = strict_rv_get
        with prov_rb.reconcile_step(
            ReconcileStepContext(known_cloud_instance_ids=known_rb)
        ):
            prov_rb.launch(shape={"cpu": 1}, request_id="rb-fail")
            errs_rb = prov_rb.poll_errors()
            assert len(errs_rb) == 1
            assert prov_rb._last_pod_list_rv is None
            prov_rb.launch(shape={"cpu": 1}, request_id="rb-ok")
        assert rv_checks == [(5000, 4000)]
        assert prov_rb._last_pod_list_rv == "4000"
        assert [
            op["value"]
            for ops in client_rb.patch_history
            for op in ops
            if op["path"].endswith("/replicas")
        ] == [3]

    def test_reconcile_step_preserves_user_cr_replicas_scale(self):
        """User increases to CR spec.replicas before operator creates pods are preserved
        across both single-host and multi-host in-step launches and terminations."""
        _pod = self._make_multihost_pod

        for cold_start in (True, False):
            for action, expected_reps in (("launch", 7), ("terminate", 4)):
                prov_u, client_u = self._make_kuberay_provider(
                    worker_groups=[
                        {
                            "groupName": "cpu",
                            "replicas": 6 if cold_start else 2,
                            "minReplicas": 0,
                            "maxReplicas": 10,
                            "template": {"spec": {"containers": [{}]}},
                        }
                    ],
                    pods=[_pod(f"cpu-{i}", group="cpu") for i in range(2)],
                )
                known_u = set(prov_u.get_non_terminated())
                if not cold_start:
                    client_u._ray_cluster["spec"]["workerGroupSpecs"][0]["replicas"] = 6
                with prov_u.reconcile_step(
                    ReconcileStepContext(known_cloud_instance_ids=known_u)
                ):
                    if action == "launch":
                        prov_u.launch(shape={"cpu": 1}, request_id="u-l1")
                    else:
                        prov_u.terminate(ids=["cpu-0", "cpu-1"], request_id="u-t1")
                assert [
                    op["value"]
                    for ops in client_u.patch_history
                    for op in ops
                    if op["path"].endswith("/replicas")
                ] == [expected_reps]

        for action, expected_reps in (("launch", 4), ("terminate", 2)):
            prov_mh, client_mh = self._make_kuberay_provider(
                worker_groups=[
                    {
                        "groupName": "tpu",
                        "replicas": 1,
                        "minReplicas": 0,
                        "maxReplicas": 6,
                        "numOfHosts": 4,
                        "template": {"spec": {"containers": [{}]}},
                    }
                ],
                pods=[_pod(f"tpu-{i}", group="tpu", rep="rep-0") for i in range(4)],
            )
            known_mh = set(prov_mh.get_non_terminated())
            client_mh._ray_cluster["spec"]["workerGroupSpecs"][0]["replicas"] = 3
            with prov_mh.reconcile_step(
                ReconcileStepContext(
                    known_cloud_instance_ids=known_mh,
                    unclaimed_replicas_by_lid={"tpu": {}},
                )
            ):
                if action == "launch":
                    prov_mh.launch(shape={"tpu": 4}, request_id="mh-l1")
                else:
                    prov_mh.terminate(ids=["tpu-0"], request_id="mh-t1")
            assert [
                op["value"]
                for ops in client_mh.patch_history
                for op in ops
                if op["path"].endswith("/replicas")
            ] == [expected_reps]

        # Counter-case 1: genuine operator deletion advances resourceVersion 10 -> 11,
        # so the deleted pod shrinks observed count (4 + 1 = 5 -> no patch needed).
        prov_del, client_del = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "cpu",
                    "replicas": 5,
                    "minReplicas": 0,
                    "maxReplicas": 10,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            pods=[_pod(f"cpu-{i}", group="cpu") for i in range(5)],
        )
        client_del._pod_list["metadata"]["resourceVersion"] = "10"
        known_del = set(prov_del.get_non_terminated())
        client_del._pod_list["metadata"]["resourceVersion"] = "11"
        client_del._pod_list["items"] = [
            p for p in client_del._pod_list["items"] if p["metadata"]["name"] != "cpu-4"
        ]
        with prov_del.reconcile_step(
            ReconcileStepContext(known_cloud_instance_ids=known_del)
        ):
            prov_del.launch(shape={"cpu": 1}, request_id="refill-5")
        assert client_del.patch_history == []

        # Counter-case 2: pod marked with deletionTimestamp is dropped even if RV did
        # not advance.
        prov_ts, client_ts = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "cpu",
                    "replicas": 5,
                    "minReplicas": 0,
                    "maxReplicas": 10,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            pods=[_pod(f"cpu-{i}", group="cpu") for i in range(5)],
        )
        client_ts._pod_list["metadata"]["resourceVersion"] = "10"
        known_ts = set(prov_ts.get_non_terminated())
        for p in client_ts._pod_list["items"]:
            if p["metadata"]["name"] == "cpu-4":
                p["metadata"]["deletionTimestamp"] = "2026-01-01T00:00:00Z"
        with prov_ts.reconcile_step(
            ReconcileStepContext(known_cloud_instance_ids=known_ts)
        ):
            prov_ts.launch(shape={"cpu": 1}, request_id="refill-ts")
        assert client_ts.patch_history == []

    def test_reconcile_step_noop_standalone_sibling_patch_and_replay_rejection(self):
        """Preserves external user CR scale-ups after cold-start no-op launches,
        avoids double-counting standalone launches, preserves concurrent user edits on
        unpatched sibling groups during PATCH, handles external CR decreases, and
        retains partial maxReplicas rejections across idempotent request_id replays."""
        _pod = self._make_multihost_pod

        # 1. Cold-start no-op in-step launch (minReplicas=2, replicas=2, 0 pods yet)
        # followed by user CR scale-up (2 -> 5) + +1 launch patches replicas to 6.
        prov_noop, client_noop = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "cpu",
                    "replicas": 2,
                    "minReplicas": 2,
                    "maxReplicas": 10,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            pods=[],
        )
        known0 = set(prov_noop.get_non_terminated())
        with prov_noop.reconcile_step(
            ReconcileStepContext(known_cloud_instance_ids=known0)
        ):
            prov_noop.launch(shape={"cpu": 2}, request_id="noop-0")
        assert client_noop.patch_history == []
        client_noop._ray_cluster["spec"]["workerGroupSpecs"][0]["replicas"] = 5
        known1 = set(prov_noop.get_non_terminated())
        with prov_noop.reconcile_step(
            ReconcileStepContext(
                known_cloud_instance_ids=known1,
                single_host_in_flight={"cpu": {"noop-0": 2}},
            )
        ):
            prov_noop.launch(shape={"cpu": 1}, request_id="step-1")
        assert [
            op["value"]
            for ops in client_noop.patch_history
            for op in ops
            if op["path"].endswith("/replicas")
        ] == [6]

        # External user CR decrease (6 -> 2) shrinks external unmaterialized capacity
        # so a subsequent +1 launch with 2 in-flight patches replicas to 3 (not 7).
        client_noop.patch_history.clear()
        client_noop._ray_cluster["spec"]["workerGroupSpecs"][0]["replicas"] = 2
        known2 = set(prov_noop.get_non_terminated())
        with prov_noop.reconcile_step(
            ReconcileStepContext(
                known_cloud_instance_ids=known2,
                single_host_in_flight={"cpu": {"noop-0": 2}},
            )
        ):
            prov_noop.launch(shape={"cpu": 1}, request_id="step-after-dec")
        assert [
            op["value"]
            for ops in client_noop.patch_history
            for op in ops
            if op["path"].endswith("/replicas")
        ] == [3]

        # 2. Standalone launch(2) followed by in-step launch(2) patches to 4 (not 6).
        prov_sa, client_sa = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "cpu",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": 10,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            pods=[],
        )
        prov_sa.launch(shape={"cpu": 2}, request_id="sa-0")
        known_sa = set(prov_sa.get_non_terminated())
        with prov_sa.reconcile_step(
            ReconcileStepContext(
                known_cloud_instance_ids=known_sa,
                single_host_in_flight={"cpu": {"sa-0": 2}},
            )
        ):
            prov_sa.launch(shape={"cpu": 2}, request_id="sa-step-1")
        assert [
            op["value"]
            for ops in client_sa.patch_history
            for op in ops
            if op["path"].endswith("/replicas")
        ] == [2, 4]

        # 3. Concurrent external CR scale-up on sibling group "gpu" (0 -> 4) returned
        # in the live PATCH response for "cpu" (0 -> 1) is preserved on the next tick.
        prov_sib, client_sib = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "cpu",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": 10,
                    "template": {"spec": {"containers": [{}]}},
                },
                {
                    "groupName": "gpu",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": 10,
                    "template": {"spec": {"containers": [{}]}},
                },
            ],
            pods=[],
        )
        real_sib_patch = client_sib.patch

        def concurrent_gpu_edit_patch(path, payload):
            client_sib._ray_cluster["spec"]["workerGroupSpecs"][1]["replicas"] = 4
            return real_sib_patch(path, payload)

        client_sib.patch = concurrent_gpu_edit_patch
        known_sib0 = set(prov_sib.get_non_terminated())
        with prov_sib.reconcile_step(
            ReconcileStepContext(known_cloud_instance_ids=known_sib0)
        ):
            prov_sib.launch(shape={"cpu": 1}, request_id="cpu-1")
        client_sib.patch = real_sib_patch
        known_sib1 = set(prov_sib.get_non_terminated())
        with prov_sib.reconcile_step(
            ReconcileStepContext(
                known_cloud_instance_ids=known_sib1,
                single_host_in_flight={"cpu": {"cpu-1": 1}},
            )
        ):
            prov_sib.launch(shape={"gpu": 1}, request_id="gpu-1")
        assert [
            (op["path"], op["value"])
            for ops in client_sib.patch_history
            for op in ops
            if op["path"].endswith("/replicas")
        ] == [
            ("/spec/workerGroupSpecs/0/replicas", 1),
            ("/spec/workerGroupSpecs/1/replicas", 5),
        ]

        # 4. Replaying an already-processed request_id after partial maxReplicas=2
        # rejection retains accepted=2 so a subsequent +1 launch after raising
        # maxReplicas to 6 patches replicas to 3 (not 5).
        prov_rep, client_rep = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "cpu",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": 2,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            pods=[],
        )
        known_rep = set(prov_rep.get_non_terminated())
        with prov_rep.reconcile_step(
            ReconcileStepContext(known_cloud_instance_ids=known_rep)
        ):
            prov_rep.launch(shape={"cpu": 4}, request_id="req-cap")
            errs_cap = prov_rep.poll_errors()
            assert len(errs_cap) == 1 and errs_cap[0].count == 2
            prov_rep.launch(shape={"cpu": 4}, request_id="req-cap")
            prov_rep._ray_cluster["spec"]["workerGroupSpecs"][0]["maxReplicas"] = 6
            prov_rep.launch(shape={"cpu": 1}, request_id="req-more")
        assert [
            op["value"]
            for ops in client_rep.patch_history
            for op in ops
            if op["path"].endswith("/replicas")
        ] == [2, 3]

    def test_single_host_timeout_replay_clamping_and_live_min_replicas_update(self):
        """Verify:
        1. Replaying a shared single-host request_id on partial timeout retry
           clamps to the retried shape rather than re-adding materialized pods
           or co-launched sibling single-host worker groups.
        2. Live RayCluster updates raising minReplicas alongside replicas do
           not credit minReplicas capacity to _external_unmaterialized, while
           surplus external replicas above minReplicas remain preserved.
        """
        # 1. Partial single-host timeout retry on shared request_id="L1".
        prov_sh, client_sh = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "sh-a",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": 10,
                    "template": {"spec": {"containers": [{}]}},
                },
                {
                    "groupName": "sh-b",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": 10,
                    "template": {"spec": {"containers": [{}]}},
                },
            ],
            pods=[],
        )
        known_t1 = set(prov_sh.get_non_terminated())
        with prov_sh.reconcile_step(
            ReconcileStepContext(known_cloud_instance_ids=known_t1)
        ):
            prov_sh.launch(shape={"sh-a": 2, "sh-b": 3}, request_id="L1")
        assert prov_sh._ray_cluster["spec"]["workerGroupSpecs"][0]["replicas"] == 2
        assert prov_sh._ray_cluster["spec"]["workerGroupSpecs"][1]["replicas"] == 3

        # Materialize 1 sh-a + 3 sh-b pods; 1 sh-a timed out back to QUEUED and
        # retries with shape={"sh-a": 1} under request_id="L1", followed by a
        # fresh {"sh-a": 1, "sh-b": 1} request "L2" in the same step.
        client_sh._pod_list["items"].extend(
            [
                self._make_multihost_pod("sh-a-0", group="sh-a"),
                self._make_multihost_pod("sh-b-0", group="sh-b"),
                self._make_multihost_pod("sh-b-1", group="sh-b"),
                self._make_multihost_pod("sh-b-2", group="sh-b"),
            ]
        )
        client_sh._pod_list["metadata"]["resourceVersion"] = "2"
        known_t3 = set(prov_sh.get_non_terminated())
        with prov_sh.reconcile_step(
            ReconcileStepContext(known_cloud_instance_ids=known_t3)
        ):
            prov_sh.launch(shape={"sh-a": 1}, request_id="L1")
            prov_sh.launch(shape={"sh-a": 1, "sh-b": 1}, request_id="L2")
        assert prov_sh._ray_cluster["spec"]["workerGroupSpecs"][0]["replicas"] == 3
        assert prov_sh._ray_cluster["spec"]["workerGroupSpecs"][1]["replicas"] == 4

        # 2. Live RayCluster update raising minReplicas alongside replicas.
        prov_min, client_min = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "cpu",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": 10,
                    "template": {"spec": {"containers": [{}]}},
                },
                {
                    "groupName": "tpu",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": 10,
                    "numOfHosts": 4,
                    "template": {"spec": {"containers": [{}]}},
                },
            ],
            pods=[],
        )
        prov_min.get_non_terminated()
        # External user edits CR: cpu raises minReplicas=2, replicas=2 (0 surplus);
        # tpu raises minReplicas=1, replicas=3 (2 surplus external replicas = 8 workers).
        client_min._ray_cluster["spec"]["workerGroupSpecs"][0]["minReplicas"] = 2
        client_min._ray_cluster["spec"]["workerGroupSpecs"][0]["replicas"] = 2
        client_min._ray_cluster["spec"]["workerGroupSpecs"][1]["minReplicas"] = 1
        client_min._ray_cluster["spec"]["workerGroupSpecs"][1]["replicas"] = 3
        known_min = set(prov_min.get_non_terminated())
        assert prov_min._external_unmaterialized["cpu"] == 0
        assert prov_min._external_unmaterialized["tpu"] == 8
        with prov_min.reconcile_step(
            ReconcileStepContext(
                known_cloud_instance_ids=known_min,
                unclaimed_replicas_by_lid={"tpu": {}},
            )
        ):
            prov_min.launch(shape={"cpu": 2}, request_id="min-cpu")
            prov_min.launch(shape={"tpu": 4}, request_id="min-tpu")
        assert prov_min._ray_cluster["spec"]["workerGroupSpecs"][0]["replicas"] == 2
        assert prov_min._ray_cluster["spec"]["workerGroupSpecs"][1]["replicas"] == 3

    def test_bounded_request_and_pre_step_lid_history(self):
        """Request IDs and pre-step launch IDs stay FIFO-bounded and prune retired entries."""
        prov, client = self._make_kuberay_provider(
            worker_groups=[
                {
                    "groupName": "cpu",
                    "replicas": 2,
                    "minReplicas": 0,
                    "maxReplicas": 20,
                    "template": {"spec": {"containers": [{}]}},
                },
                {
                    "groupName": "tpu",
                    "replicas": 4,
                    "minReplicas": 0,
                    "maxReplicas": 20,
                    "numOfHosts": 4,
                    "template": {"spec": {"containers": [{}]}},
                },
            ],
            pods=[
                self._make_multihost_pod(f"cpu-pod-{i}", group="cpu") for i in range(2)
            ],
        )
        prov._max_tracked_request_entries = 3
        known = set(prov.get_non_terminated())

        # 1. Below cap (2 <= 3): zero evictions.
        prov.launch(shape={"cpu": 1}, request_id="req-0")
        prov.launch(shape={"cpu": 1}, request_id="req-1")
        assert list(prov._requests) == ["req-0", "req-1"]
        assert list(prov._accepted_launch_workers) == ["req-0", "req-1"]

        # 2. Exceeding cap across launch + terminate: FIFO evicts oldest entries.
        prov.launch(shape={"cpu": 1}, request_id="req-2")
        prov.launch(shape={"cpu": 1}, request_id="req-3")
        prov.terminate(ids=["cpu-pod-0"], request_id="term-4")
        assert len(prov._requests) == 3
        assert list(prov._requests) == ["req-2", "req-3", "term-4"]
        assert len(prov._accepted_launch_workers) <= 3
        assert "req-0" not in prov._requests
        assert "req-1" not in prov._requests

        # 3. Pre-step claimed lids: no double subtraction in-step, FIFO cap on burst,
        # and immediate pruning once lids leave in-flight maps on the next step.
        assert prov._external_unmaterialized["tpu"] == 16
        step_ctx = ReconcileStepContext(
            known_cloud_instance_ids=known,
            unclaimed_replicas_by_lid={
                "tpu": {"lid-a": 1, "lid-b": 1, "lid-c": 1, "lid-d": 1}
            },
        )
        with prov.reconcile_step(step_ctx):
            assert prov._external_unmaterialized["tpu"] == 0
            assert len(prov._claimed_pre_step_lids) == 3
            assert ("tpu", "lid-a") not in prov._claimed_pre_step_lids
            assert ("tpu", "lid-d") in prov._claimed_pre_step_lids
            prov.update_reconcile_step(
                ReconcileStepContext(
                    known_cloud_instance_ids=known,
                    unclaimed_replicas_by_lid={
                        "tpu": {"lid-b": 1, "lid-c": 1, "lid-d": 1}
                    },
                )
            )
            assert prov._external_unmaterialized["tpu"] == 0

        with prov.reconcile_step(
            ReconcileStepContext(
                known_cloud_instance_ids=known,
                unclaimed_replicas_by_lid={"tpu": {}},
            )
        ):
            assert len(prov._claimed_pre_step_lids) == 0


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
