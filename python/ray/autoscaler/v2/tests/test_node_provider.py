# coding: utf-8
import os
import sys
import unittest

import pytest  # noqa
from ray._private.test_utils import MockProvider, load_test_config
from ray.autoscaler._private.event_summarizer import EventSummarizer
from ray.autoscaler._private.node_launcher import BaseNodeLauncher
from ray.autoscaler._private.node_provider_availability_tracker import (
    NodeProviderAvailabilityTracker,
)
from ray.autoscaler.v2.instance_manager.config import NodeProviderConfig
from ray.autoscaler.v2.instance_manager.node_provider import NodeProviderAdapter


class FakeCounter:
    def dec(self, *args, **kwargs):
        pass


class NodeProviderTest(unittest.TestCase):
    def setUp(self):
        self.base_provider = MockProvider()
        self.node_launcher = BaseNodeLauncher(
            self.base_provider,
            FakeCounter(),
            EventSummarizer(),
            NodeProviderAvailabilityTracker(),
        )
        self.instance_config_provider = NodeProviderConfig(
            load_test_config("test_ray_complex.yaml")
        )
        self.node_provider = NodeProviderAdapter(
            self.base_provider, self.node_launcher, self.instance_config_provider
        )

    def test_create_nodes(self):
        nodes = self.node_provider.create_nodes("worker_nodes1", 1)
        assert len(nodes) == 1
        assert nodes[0] == "worker_nodes1-0"
        self.assertEqual(len(self.base_provider.mock_nodes), 1)

    def test_terminate_nodes(self):
        self.node_provider.create_nodes("worker_nodes1", 1)
        self.assertEqual(len(self.base_provider.mock_nodes), 1)

    def test_get_nodes(self):
        self.node_provider.create_nodes("worker_nodes1", 1)
        self.assertEqual(len(self.base_provider.mock_nodes), 1)

    def get_nodes_by_cloud_instance_id(self):
        self.node_provider.create_nodes("worker_nodes1", 1)
        self.assertEqual(len(self.base_provider.mock_nodes), 1)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
