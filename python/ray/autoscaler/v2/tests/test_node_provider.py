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
from ray.autoscaler.v2.instance_manager.node_provider import NodeProviderAdapter
from ray.autoscaler.v2.tests.util import FakeCounter
from ray.core.generated.instance_manager_pb2 import Instance
from ray.tests.autoscaler_test_utils import MockProvider


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
        self.instance_config_provider = NodeProviderConfig(
            load_test_config("test_ray_complex.yaml")
        )
        self.node_provider = NodeProviderAdapter(
            self.base_provider, self.node_launcher, self.instance_config_provider
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


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
