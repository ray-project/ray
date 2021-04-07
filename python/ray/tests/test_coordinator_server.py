import os
import unittest
import socket
import json

from ray.autoscaler.local.coordinator_server import OnPremCoordinatorServer
from ray.autoscaler._private.providers import _NODE_PROVIDERS, \
    _get_node_provider
from ray.autoscaler._private.local.node_provider import LocalNodeProvider
from ray.autoscaler._private.local.coordinator_node_provider import (
    CoordinatorSenderNodeProvider)
from ray.autoscaler.tags import (TAG_RAY_NODE_KIND, TAG_RAY_CLUSTER_NAME,
                                 TAG_RAY_NODE_NAME, NODE_KIND_WORKER,
                                 NODE_KIND_HEAD)
import pytest


class OnPremCoordinatorServerTest(unittest.TestCase):
    def setUp(self):
        self.list_of_node_ips = ["0.0.0.0:1", "0.0.0.0:2"]
        self.host, self.port = socket.gethostbyname(socket.gethostname()), 1234
        self.server = OnPremCoordinatorServer(
            list_of_node_ips=self.list_of_node_ips,
            host=self.host,
            port=self.port,
        )
        self.coordinator_address = self.host + ":" + str(self.port)

    def tearDown(self):
        self.server.shutdown()
        state_save_path = "/tmp/coordinator.state"
        if os.path.exists(state_save_path):
            os.remove(state_save_path)

    def testImportingCorrectClass(self):
        """Check correct import when coordinator_address is in config yaml."""

        provider_config = {"coordinator_address": "fake_address:1234"}
        coordinator_node_provider = _NODE_PROVIDERS.get("local")(
            provider_config)
        assert coordinator_node_provider is CoordinatorSenderNodeProvider
        local_node_provider = _NODE_PROVIDERS.get("local")({})
        assert local_node_provider is LocalNodeProvider

    def testClusterStateInit(self):
        """Check ClusterState __init__ func generates correct state file.

        Test the general use case and if num_workers increase/decrease.
        """

        cluster_config = {
            "cluster_name": "random_name",
            "min_workers": 0,
            "max_workers": 0,
            "provider": {
                "type": "local",
                "head_ip": "0.0.0.0:2",
                "worker_ips": ["0.0.0.0:1"]
            },
        }
        provider_config = cluster_config["provider"]
        node_provider = _get_node_provider(
            provider_config, cluster_config["cluster_name"], use_cache=False)
        assert isinstance(node_provider, LocalNodeProvider)
        expected_workers = {}
        expected_workers[provider_config["head_ip"]] = {
            "tags": {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD
            },
            "state": "terminated",
        }
        expected_workers[provider_config["worker_ips"][0]] = {
            "tags": {
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER
            },
            "state": "terminated",
        }

        state_save_path = "/tmp/cluster-{}.state".format(
            cluster_config["cluster_name"])
        assert os.path.exists(state_save_path)
        workers = json.loads(open(state_save_path).read())
        assert workers == expected_workers

        # Test removing workers updates the cluster state.
        del expected_workers[provider_config["worker_ips"][0]]
        removed_ip = provider_config["worker_ips"].pop()
        node_provider = _get_node_provider(
            provider_config, cluster_config["cluster_name"], use_cache=False)
        workers = json.loads(open(state_save_path).read())
        assert workers == expected_workers

        # Test adding back workers updates the cluster state.
        expected_workers[removed_ip] = {
            "tags": {
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER
            },
            "state": "terminated",
        }
        provider_config["worker_ips"].append(removed_ip)
        node_provider = _get_node_provider(
            provider_config, cluster_config["cluster_name"], use_cache=False)
        workers = json.loads(open(state_save_path).read())
        assert workers == expected_workers

    def testOnPremCoordinatorStateInit(self):
        """If OnPremCoordinatorState __init__ generates correct state file.

        Test the general use case and if the coordinator server crashes or
        updates the list of node ips with more/less nodes.
        """

        expected_nodes = {}
        for ip in self.list_of_node_ips:
            expected_nodes[ip] = {
                "tags": {},
                "state": "terminated",
            }

        state_save_path = "/tmp/coordinator.state"
        assert os.path.exists(state_save_path)
        nodes = json.loads(open(state_save_path).read())
        assert nodes == expected_nodes

        # Test removing workers updates the cluster state.
        del expected_nodes[self.list_of_node_ips[1]]
        self.server.shutdown()
        self.server = OnPremCoordinatorServer(
            list_of_node_ips=self.list_of_node_ips[0:1],
            host=self.host,
            port=self.port,
        )
        nodes = json.loads(open(state_save_path).read())
        assert nodes == expected_nodes

        # Test adding back workers updates the cluster state.
        expected_nodes[self.list_of_node_ips[1]] = {
            "tags": {},
            "state": "terminated",
        }
        self.server.shutdown()
        self.server = OnPremCoordinatorServer(
            list_of_node_ips=self.list_of_node_ips,
            host=self.host,
            port=self.port,
        )
        nodes = json.loads(open(state_save_path).read())
        assert nodes == expected_nodes

    def testCoordinatorSenderNodeProvider(self):
        """Integration test of CoordinatorSenderNodeProvider."""
        cluster_config = {
            "cluster_name": "random_name",
            "min_workers": 0,
            "max_workers": 0,
            "provider": {
                "type": "local",
                "coordinator_address": self.coordinator_address,
            },
            "head_node": {},
            "worker_nodes": {},
        }
        provider_config = cluster_config["provider"]
        node_provider_1 = _get_node_provider(
            provider_config, cluster_config["cluster_name"], use_cache=False)
        assert isinstance(node_provider_1, CoordinatorSenderNodeProvider)

        assert not node_provider_1.non_terminated_nodes({})
        assert not node_provider_1.is_running(self.list_of_node_ips[0])
        assert node_provider_1.is_terminated(self.list_of_node_ips[0])
        assert not node_provider_1.node_tags(self.list_of_node_ips[0])
        head_node_tags = {
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
        }
        assert not node_provider_1.non_terminated_nodes(head_node_tags)
        head_node_tags[TAG_RAY_NODE_NAME] = "ray-{}-head".format(
            cluster_config["cluster_name"])
        node_provider_1.create_node(cluster_config["head_node"],
                                    head_node_tags, 1)
        assert node_provider_1.non_terminated_nodes(
            {}) == [self.list_of_node_ips[0]]
        head_node_tags[TAG_RAY_CLUSTER_NAME] = cluster_config["cluster_name"]
        assert node_provider_1.node_tags(
            self.list_of_node_ips[0]) == head_node_tags
        assert node_provider_1.is_running(self.list_of_node_ips[0])
        assert not node_provider_1.is_terminated(self.list_of_node_ips[0])

        # Add another cluster.
        cluster_config["cluster_name"] = "random_name_2"
        provider_config = cluster_config["provider"]
        node_provider_2 = _get_node_provider(
            provider_config, cluster_config["cluster_name"], use_cache=False)
        assert not node_provider_2.non_terminated_nodes({})
        assert not node_provider_2.is_running(self.list_of_node_ips[1])
        assert node_provider_2.is_terminated(self.list_of_node_ips[1])
        assert not node_provider_2.node_tags(self.list_of_node_ips[1])
        assert not node_provider_2.non_terminated_nodes(head_node_tags)
        head_node_tags[TAG_RAY_NODE_NAME] = "ray-{}-head".format(
            cluster_config["cluster_name"])
        node_provider_2.create_node(cluster_config["head_node"],
                                    head_node_tags, 1)
        assert node_provider_2.non_terminated_nodes(
            {}) == [self.list_of_node_ips[1]]
        head_node_tags[TAG_RAY_CLUSTER_NAME] = cluster_config["cluster_name"]
        assert node_provider_2.node_tags(
            self.list_of_node_ips[1]) == head_node_tags
        assert node_provider_2.is_running(self.list_of_node_ips[1])
        assert not node_provider_2.is_terminated(self.list_of_node_ips[1])

        # Add another cluster (should fail because we only have two nodes).
        cluster_config["cluster_name"] = "random_name_3"
        provider_config = cluster_config["provider"]
        node_provider_3 = _get_node_provider(
            provider_config, cluster_config["cluster_name"], use_cache=False)
        assert not node_provider_3.non_terminated_nodes(head_node_tags)
        head_node_tags[TAG_RAY_NODE_NAME] = "ray-{}-head".format(
            cluster_config["cluster_name"])
        node_provider_3.create_node(cluster_config["head_node"],
                                    head_node_tags, 1)
        assert not node_provider_3.non_terminated_nodes({})

        # Terminate all nodes.
        node_provider_1.terminate_node(self.list_of_node_ips[0])
        assert not node_provider_1.non_terminated_nodes({})
        node_provider_2.terminate_node(self.list_of_node_ips[1])
        assert not node_provider_2.non_terminated_nodes({})

        # Check if now we can create more clusters/nodes.
        node_provider_3.create_node(cluster_config["head_node"],
                                    head_node_tags, 1)
        worker_node_tags = {
            TAG_RAY_NODE_NAME: "ray-{}-worker".format(
                cluster_config["cluster_name"]),
            TAG_RAY_NODE_KIND: NODE_KIND_WORKER
        }
        node_provider_3.create_node(cluster_config["worker_nodes"],
                                    worker_node_tags, 1)
        assert node_provider_3.non_terminated_nodes(
            {}) == self.list_of_node_ips
        worker_filter = {TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
        assert node_provider_3.non_terminated_nodes(worker_filter) == [
            self.list_of_node_ips[1]
        ]
        head_filter = {TAG_RAY_NODE_KIND: NODE_KIND_HEAD}
        assert node_provider_3.non_terminated_nodes(head_filter) == [
            self.list_of_node_ips[0]
        ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
