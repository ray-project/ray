# coding: utf-8
import os
import sys
import unittest

import pytest  # noqa

from ray._private.test_utils import load_test_config
from ray.autoscaler._private.event_summarizer import EventSummarizer
from ray.autoscaler._private.node_launcher import BaseNodeLauncher
from ray.autoscaler._private.node_provider_availability_tracker import (
    NodeProviderAvailabilityTracker,
)
from ray.autoscaler.v2.instance_manager.config import NodeProviderConfig
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.node_provider import NodeProviderAdapter
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.instance_manager.subscribers.reconciler import InstanceReconciler
from ray.autoscaler.v2.tests.util import FakeCounter
from ray.core.generated.instance_manager_pb2 import Instance
from ray.tests.autoscaler_test_utils import MockProvider


class InstanceReconcilerTest(unittest.TestCase):
    def setUp(self):
        self.base_provider = MockProvider()
        self.availability_tracker = NodeProviderAvailabilityTracker()
        self.node_launcher = BaseNodeLauncher(
            self.base_provider,
            FakeCounter(),
            EventSummarizer(),
            self.availability_tracker,
        )
        self.instance_config_provider = NodeProviderConfig(
            load_test_config("test_ray_complex.yaml")
        )
        self.node_provider = NodeProviderAdapter(
            self.base_provider, self.node_launcher, self.instance_config_provider
        )

        self.instance_storage = InstanceStorage(
            cluster_id="test_cluster_id",
            storage=InMemoryStorage(),
        )
        self.reconciler = InstanceReconciler(
            instance_storage=self.instance_storage,
            node_provider=self.node_provider,
        )

    def tearDown(self):
        self.reconciler.shutdown()

    def test_handle_ray_failure(self):
        self.node_provider.create_nodes("worker_nodes1", 1)
        instance = Instance(
            instance_id="0",
            instance_type="worker_nodes1",
            cloud_instance_id="0",
            status=Instance.ALLOCATED,
            ray_status=Instance.RAY_STOPPED,
        )
        assert not self.base_provider.is_terminated(instance.cloud_instance_id)
        success, verison = self.instance_storage.upsert_instance(instance)
        assert success
        instance.version = verison
        self.reconciler._handle_ray_failure([instance.instance_id])

        instances, _ = self.instance_storage.get_instances(
            instance_ids={instance.instance_id}
        )
        assert instances[instance.instance_id].status == Instance.STOPPING
        assert self.base_provider.is_terminated(instance.cloud_instance_id)

        # reconciler will detect the node is terminated and update the status.
        self.reconciler._reconcile_with_node_provider()
        instances, _ = self.instance_storage.get_instances(
            instance_ids={instance.instance_id}
        )
        assert instances[instance.instance_id].status == Instance.STOPPED


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
