# coding: utf-8
import copy
import os
import sys
import unittest

import pytest  # noqa
from ray._private.test_utils import MockNode, MockProvider, load_test_config
from ray.autoscaler._private.event_summarizer import EventSummarizer
from ray.autoscaler._private.node_launcher import BaseNodeLauncher
from ray.autoscaler._private.node_provider_availability_tracker import (
    NodeProviderAvailabilityTracker,
)
from ray.autoscaler._private.util import ConcurrentCounter
from ray.autoscaler.v2.instance_manager.config import NodeProviderConfig
from ray.autoscaler.v2.instance_manager.node_provider import NodeProviderAdapter
from ray.core.generated.instance_manager_pb2 import Instance


class NodeProviderTest(unittest.TestCase):
    def setUp(self):
        self.base_provider = MockProvider()
        self.node_launcher = BaseNodeLauncher(
            self.base_provider,
            ConcurrentCounter(),
            EventSummarizer(),
            NodeProviderAvailabilityTracker(),
        )
        self.instance_config_provider = NodeProviderConfig(
            load_test_config("test_multi_node.yaml")
        )
        self.node_provider = NodeProviderAdapter(
            self.base_provider, self.node_launcher, self.instance_config_provider
        )

    def test_create_nodes(self):
        self.node_provider.create_nodes("test", 1)
        self.assertEqual(len(self.base_provider.mock_nodes), 1)

    def test_terminate_nodes(self):
        self.node_provider.create_nodes("test", 1)
        self.assertEqual(len(self.base_provider.mock_nodes), 1)

    def test_get_nodes(self):
        self.node_provider.create_nodes("test", 1)
        self.assertEqual(len(self.base_provider.mock_nodes), 1)

    def get_nodes_by_cloud_instance_id(self):
        self.node_provider.create_nodes("test", 1)
        self.assertEqual(len(self.base_provider.mock_nodes), 1)
