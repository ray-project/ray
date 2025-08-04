# coding: utf-8
import os
import sys
import unittest
from unittest.mock import patch
from queue import Queue

import pytest  # noqa

from ray._private.test_utils import load_test_config
from ray.autoscaler.tags import TAG_RAY_NODE_KIND
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.ray_installer import RayInstaller
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.instance_manager.subscribers.threaded_ray_installer import (
    ThreadedRayInstaller,
)
from ray.core.generated.instance_manager_pb2 import Instance, NodeKind
from ray.tests.autoscaler_test_utils import MockProcessRunner, MockProvider


class ThreadedRayInstallerTest(unittest.TestCase):
    def setUp(self):
        self.base_provider = MockProvider()
        self.config = AutoscalingConfig(load_test_config("test_ray_complex.yaml"))
        self.runner = MockProcessRunner()
        self.ray_installer = RayInstaller(self.base_provider, self.config, self.runner)
        self.instance_storage = InstanceStorage(
            cluster_id="test_cluster_id",
            storage=InMemoryStorage(),
        )
        self.error_queue = Queue()
        self.threaded_ray_installer = ThreadedRayInstaller(
            head_node_ip="127.0.0.1",
            instance_storage=self.instance_storage,
            ray_installer=self.ray_installer,
            error_queue=self.error_queue,
        )

    def test_install_ray_on_new_node_version_mismatch(self):
        self.base_provider.create_node({}, {TAG_RAY_NODE_KIND: "worker_nodes1"}, 1)
        instance = Instance(
            instance_id="0",
            instance_type="worker_nodes1",
            cloud_instance_id="0",
            status=Instance.RAY_INSTALLING,
            node_kind=NodeKind.WORKER,
        )
        success, verison = self.instance_storage.upsert_instance(instance)
        assert success
        self.runner.respond_to_call("json .Config.Env", ["[]" for i in range(1)])

        self.threaded_ray_installer._install_ray_on_single_node(instance)
        instances, _ = self.instance_storage.get_instances(
            instance_ids={instance.instance_id}
        )
        assert instances[instance.instance_id].status == Instance.RAY_INSTALLING
        assert instances[instance.instance_id].version == verison

    @patch.object(RayInstaller, "install_ray")
    def test_install_ray_on_new_node_install_failed(self, mock_method):
        self.base_provider.create_node({}, {TAG_RAY_NODE_KIND: "worker_nodes1"}, 1)
        instance = Instance(
            instance_id="0",
            instance_type="worker_nodes1",
            cloud_instance_id="0",
            status=Instance.RAY_INSTALLING,
            node_kind=NodeKind.WORKER,
        )
        success, verison = self.instance_storage.upsert_instance(instance)
        assert success
        instance.version = verison

        mock_method.side_effect = RuntimeError("Installation failed")
        self.threaded_ray_installer._install_retry_interval = 0
        self.threaded_ray_installer._max_install_attempts = 1
        self.threaded_ray_installer._install_ray_on_single_node(instance)

        instances, _ = self.instance_storage.get_instances(
            instance_ids={instance.instance_id}
        )
        # Make sure the instance status is not updated by the ThreadedRayInstaller
        # since it should be updated by the Reconciler.
        assert instances[instance.instance_id].status == Instance.RAY_INSTALLING
        # Make sure the error is added to the error queue.
        error = self.error_queue.get()
        assert error.im_instance_id == instance.instance_id
        assert "Installation failed" in error.details

    def test_install_ray_on_new_nodes(self):
        self.base_provider.create_node({}, {TAG_RAY_NODE_KIND: "worker_nodes1"}, 1)
        instance = Instance(
            instance_id="0",
            instance_type="worker_nodes1",
            cloud_instance_id="0",
            status=Instance.RAY_INSTALLING,
            node_kind=NodeKind.WORKER,
        )
        success, verison = self.instance_storage.upsert_instance(instance)
        assert success
        instance.version = verison
        self.runner.respond_to_call("json .Config.Env", ["[]" for i in range(1)])

        self.threaded_ray_installer._install_ray_on_new_nodes(instance.instance_id)
        self.threaded_ray_installer._ray_installation_executor.shutdown(wait=True)
        instances, _ = self.instance_storage.get_instances(
            instance_ids={instance.instance_id}
        )
        # Make sure the instance status is not updated by the ThreadedRayInstaller
        # since it should be updated by the Reconciler.
        assert instances[instance.instance_id].status == Instance.RAY_INSTALLING


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
