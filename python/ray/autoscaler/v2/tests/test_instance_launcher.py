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
from ray.autoscaler.v2.instance_manager.subscribers.instance_launcher import (
    InstanceLauncher,
)
from ray.autoscaler.v2.tests.util import FakeCounter, create_instance
from ray.core.generated.instance_manager_pb2 import Instance
from ray.tests.autoscaler_test_utils import MockProvider


class InstanceLauncherTest(unittest.TestCase):
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
        self.instance_launcher = InstanceLauncher(
            instance_storage=self.instance_storage,
            node_provider=self.node_provider,
            max_concurrent_requests=1,
            max_instances_per_request=1,
        )
        self.instance_storage.add_status_change_subscriber(self.instance_launcher)

    def test_launch_new_instance_by_type(self):
        instance = create_instance("1")
        success, verison = self.instance_storage.upsert_instance(instance)
        assert success
        instance.version = verison
        assert 1 == self.instance_launcher._launch_new_instances_by_type(
            "worker_nodes1", [instance]
        )
        instances, _ = self.instance_storage.get_instances()
        assert len(instances) == 1
        assert instances["1"].status == Instance.ALLOCATED
        assert instances["1"].cloud_instance_id == "0"

    def test_launch_failed(self):
        # launch failed: instance is not in storage
        instance = create_instance("1")
        assert 0 == self.instance_launcher._launch_new_instances_by_type(
            "worker_nodes1", [instance]
        )
        instances, _ = self.instance_storage.get_instances()
        assert len(instances) == 0

        # launch failed: instance version mismatch
        instance = create_instance("1")
        self.instance_storage.upsert_instance(instance)
        instance.version = 2
        assert 0 == self.instance_launcher._launch_new_instances_by_type(
            "worker_nodes1", [instance]
        )
        instances, _ = self.instance_storage.get_instances()
        assert len(instances) == 1
        assert instances["1"].status == Instance.UNKNOWN

    def test_launch_partial_success(self):
        self.base_provider.partical_success_count = 1
        instance1 = create_instance("1")
        instance2 = create_instance("2")
        success, version = self.instance_storage.batch_upsert_instances(
            [instance1, instance2]
        )
        assert success
        instance1.version = version
        instance2.version = version
        self.instance_launcher._launch_new_instances_by_type(
            "worker_nodes1", [instance1, instance2]
        )
        instances, _ = self.instance_storage.get_instances()
        assert len(instances) == 2
        assert instances["1"].status == Instance.ALLOCATION_FAILED
        assert instances["2"].status == Instance.ALLOCATED


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
