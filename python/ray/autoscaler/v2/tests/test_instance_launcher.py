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
from ray.autoscaler.node_launch_exception import NodeLaunchException
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
        self.instance_storage.upsert_instance(instance)
        self.instance_launcher._launch_new_instances_by_type(
            "worker_nodes1", [instance]
        )
        instances, verison = self.instance_storage.get_instances()
        assert len(instances) == 1
        assert instances["1"].status == Instance.ALLOCATED
        assert instances["1"].cloud_instance_id == "0"


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
