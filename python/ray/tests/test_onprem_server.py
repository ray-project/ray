import os
import shutil
import tempfile
import unittest
import yaml

from ray.autoscaler.local.onprem_server import LocalNodeProviderServer
from ray.autoscaler.node_provider import NODE_PROVIDERS
from ray.autoscaler.tags import (
    TAG_RAY_NODE_TYPE,
    NODE_TYPE_WORKER,
    NODE_TYPE_HEAD,
)
import pytest


class LocalNodeProviderServerTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.server_config = {
            "server_address": "localhost:1234",
            "list_of_node_ips": ["0.0.0.0:1", "0.0.0.0:2"],
        }
        self.on_prem_server_config_path = self.write_config(self.server_config)
        self.server = LocalNodeProviderServer(self.on_prem_server_config_path)
        self.local_node_provider_cls = NODE_PROVIDERS.get("local")({})
        (
            self.host,
            self.port,
            self.node_ips,
        ) = LocalNodeProviderServer.get_and_validate_config(
            self.on_prem_server_config_path)
        self.server_address = self.host + ":" + str(self.port)
        self.request_get_status = {"request_type": "get_status"}

    def tearDown(self):
        self.server.shutdown()
        shutil.rmtree(self.tmpdir)

    def write_config(self, config):
        path = os.path.join(self.tmpdir, "simple.yaml")
        with open(path, "w") as f:
            f.write(yaml.dump(config))
        return path

    def testVerifyConfigs(self):
        """Verify we check the validity of the server config file."""

        on_prem_server_config = {
            "server_address": "localhost 1234",
            "list_of_node_ips": ["0.0.0.0:1", "0.0.0.0:2"],
            "unnecessary": "blabla",
        }
        on_prem_server_config_path = self.write_config(on_prem_server_config)

        with pytest.raises(ValueError):
            LocalNodeProviderServer.get_and_validate_config(
                on_prem_server_config_path)
        del on_prem_server_config["unnecessary"]
        on_prem_server_config_path = self.write_config(on_prem_server_config)
        with pytest.raises(ValueError):
            LocalNodeProviderServer.get_and_validate_config(
                on_prem_server_config_path)
        on_prem_server_config["server_address"] = "localhost:1235"
        on_prem_server_config_path = self.write_config(on_prem_server_config)
        host, port, node_ips = LocalNodeProviderServer.get_and_validate_config(
            on_prem_server_config_path)
        assert host == "localhost"
        assert port == 1235
        assert node_ips == ["0.0.0.0:1", "0.0.0.0:2"]

    def testGetStatus(self):
        """Test if get server status works."""

        (
            available_node_ips,
            total_node_ips,
            running_clusters,
        ) = self.local_node_provider_cls.get_http_response(
            self.request_get_status, self.server_address)
        assert available_node_ips == total_node_ips
        assert total_node_ips == ["0.0.0.0:1", "0.0.0.0:2"]
        assert not running_clusters

    def testGetNodeIps(self):
        """Test get_node_ips which allocates nodes for the clusters."""

        request_get_node_ips = {
            "request_type": "get_node_ips",
            "cluster_name": "random_name",
            "num_workers": 1,
        }
        requested_node_ips = self.local_node_provider_cls.get_http_response(
            request_get_node_ips, self.server_address)
        assert requested_node_ips["head_ip"] == "0.0.0.0:2"
        assert requested_node_ips["worker_ips"] == ["0.0.0.0:1"]
        (
            available_node_ips,
            total_node_ips,
            running_clusters,
        ) = self.local_node_provider_cls.get_http_response(
            self.request_get_status, self.server_address)
        assert not available_node_ips
        assert total_node_ips == ["0.0.0.0:1", "0.0.0.0:2"]
        expected_running_clusters = {"random_name": requested_node_ips}
        assert running_clusters == expected_running_clusters

        # Test to make sure we can't get more nodes than available
        request_get_node_ips = {
            "request_type": "get_node_ips",
            "cluster_name": "new_random_name",
            "num_workers": 0,
        }
        requested_node_ips = self.local_node_provider_cls.get_http_response(
            request_get_node_ips, self.server_address)
        assert not requested_node_ips
        (
            available_node_ips,
            total_node_ips,
            running_clusters,
        ) = self.local_node_provider_cls.get_http_response(
            self.request_get_status, self.server_address)
        assert not available_node_ips
        assert total_node_ips == ["0.0.0.0:1", "0.0.0.0:2"]
        assert running_clusters == expected_running_clusters

    def testConsecutiveRayUpWithLessWorkers(self):
        """Test ray up then remove workers then ray up without ray down."""

        request_get_node_ips = {
            "request_type": "get_node_ips",
            "cluster_name": "random_name",
            "num_workers": 1,
        }
        self.local_node_provider_cls.get_http_response(request_get_node_ips,
                                                       self.server_address)
        request_get_node_ips = {
            "request_type": "get_node_ips",
            "cluster_name": "random_name",
            "num_workers": 0,
        }
        requested_node_ips = self.local_node_provider_cls.get_http_response(
            request_get_node_ips, self.server_address)
        assert requested_node_ips["head_ip"] == "0.0.0.0:2"
        assert not requested_node_ips["worker_ips"]
        (
            available_node_ips,
            total_node_ips,
            running_clusters,
        ) = self.local_node_provider_cls.get_http_response(
            self.request_get_status, self.server_address)
        assert available_node_ips == ["0.0.0.0:1"]
        expected_running_clusters = {"random_name": requested_node_ips}
        assert running_clusters == expected_running_clusters

    def testConsecutiveRayUpWithMoreWorkers(self):
        """Test ray up then add workers then ray up without ray down."""

        request_get_node_ips = {
            "request_type": "get_node_ips",
            "cluster_name": "random_name",
            "num_workers": 0,
        }
        self.local_node_provider_cls.get_http_response(request_get_node_ips,
                                                       self.server_address)
        request_get_node_ips = {
            "request_type": "get_node_ips",
            "cluster_name": "random_name",
            "num_workers": 1,
        }
        requested_node_ips = self.local_node_provider_cls.get_http_response(
            request_get_node_ips, self.server_address)
        assert requested_node_ips["head_ip"] == "0.0.0.0:2"
        assert requested_node_ips["worker_ips"] == ["0.0.0.0:1"]
        (
            available_node_ips,
            total_node_ips,
            running_clusters,
        ) = self.local_node_provider_cls.get_http_response(
            self.request_get_status, self.server_address)
        assert not available_node_ips
        assert total_node_ips == ["0.0.0.0:1", "0.0.0.0:2"]
        expected_running_clusters = {"random_name": requested_node_ips}
        assert running_clusters == expected_running_clusters

    def testConsecutiveRayUpWithExceedingWorkers(self):
        """Test ray up, add workers > available then ray up w/o ray down."""

        # Ameer: here we do not expect the server to terminate the cluster.
        request_get_node_ips = {
            "request_type": "get_node_ips",
            "cluster_name": "random_name",
            "num_workers": 1,
        }
        self.local_node_provider_cls.get_http_response(request_get_node_ips,
                                                       self.server_address)
        request_get_node_ips = {
            "request_type": "get_node_ips",
            "cluster_name": "random_name",
            "num_workers": 2,
        }
        requested_node_ips = self.local_node_provider_cls.get_http_response(
            request_get_node_ips, self.server_address)
        assert not requested_node_ips
        (
            available_node_ips,
            total_node_ips,
            running_clusters,
        ) = self.local_node_provider_cls.get_http_response(
            self.request_get_status, self.server_address)
        assert not available_node_ips
        assert total_node_ips == ["0.0.0.0:1", "0.0.0.0:2"]
        expected_running_clusters = {
            "random_name": {
                "head_ip": "0.0.0.0:2",
                "worker_ips": ["0.0.0.0:1"]
            }
        }
        assert running_clusters == expected_running_clusters

    def testStillValidCluster(self):
        """Test still_valid_cluster server function."""

        request_get_node_ips = {
            "request_type": "get_node_ips",
            "cluster_name": "random_name",
            "num_workers": 1,
        }
        self.local_node_provider_cls.get_http_response(request_get_node_ips,
                                                       self.server_address)
        provider_config = {"head_ip": "0.0.0.0:2", "worker_ips": ["0.0.0.0:1"]}
        request_still_valid_cluster = {
            "request_type": "still_valid_cluster",
            "cluster_name": "random_name",
            "provider_config": provider_config,
        }
        is_valid = self.local_node_provider_cls.get_http_response(
            request_still_valid_cluster, self.server_address)
        assert is_valid

    def testReleaseCluster(self):
        """Test release_cluster server function."""

        request_get_node_ips = {
            "request_type": "get_node_ips",
            "cluster_name": "random_name",
            "num_workers": 1,
        }
        self.local_node_provider_cls.get_http_response(request_get_node_ips,
                                                       self.server_address)
        request_release_cluster = {
            "request_type": "release_cluster",
            "cluster_name": "random_name",
        }
        self.local_node_provider_cls.get_http_response(request_release_cluster,
                                                       self.server_address)
        # Make sure releasing a cluster twice is OK
        self.local_node_provider_cls.get_http_response(request_release_cluster,
                                                       self.server_address)
        (
            available_node_ips,
            total_node_ips,
            running_clusters,
        ) = self.local_node_provider_cls.get_http_response(
            self.request_get_status, self.server_address)
        assert available_node_ips == ["0.0.0.0:2", "0.0.0.0:1"]
        assert total_node_ips == ["0.0.0.0:1", "0.0.0.0:2"]
        assert not running_clusters

    def testRecoverRunningClusterAfterCrash(self):
        """Test if we can recover a running cluster after a crash in server."""

        provider_config = {"head_ip": "0.0.0.0:2", "worker_ips": ["0.0.0.0:1"]}
        request_still_valid_cluster = {
            "request_type": "still_valid_cluster",
            "cluster_name": "random_name",
            "provider_config": provider_config,
        }
        is_valid = self.local_node_provider_cls.get_http_response(
            request_still_valid_cluster, self.server_address)
        assert is_valid  # Make sure it is still valid.
        (
            available_node_ips,
            total_node_ips,
            running_clusters,
        ) = self.local_node_provider_cls.get_http_response(
            self.request_get_status, self.server_address)
        assert not available_node_ips
        assert total_node_ips == ["0.0.0.0:1", "0.0.0.0:2"]
        expected_running_clusters = {
            "random_name": {
                "head_ip": "0.0.0.0:2",
                "worker_ips": ["0.0.0.0:1"]
            }
        }
        assert running_clusters == expected_running_clusters

    def testSuccessfullyFailToRecoverBadClusterAfterCrash(self):
        """Test to make sure we can't recover something unrecoverable."""

        provider_config = {"head_ip": "BADIP:3", "worker_ips": ["0.0.0.0:1"]}
        request_still_valid_cluster = {
            "request_type": "still_valid_cluster",
            "cluster_name": "random_name",
            "provider_config": provider_config,
        }
        is_valid = self.local_node_provider_cls.get_http_response(
            request_still_valid_cluster, self.server_address)
        assert not is_valid  # Make sure it is not valid.

    def testCoexistingClusters(self):
        """Test starting multiple clusters from different users."""

        # create cluster #1.
        request_get_node_ips = {
            "request_type": "get_node_ips",
            "cluster_name": "random_name_1",
            "num_workers": 0,
        }
        requested_node_ips1 = self.local_node_provider_cls.get_http_response(
            request_get_node_ips, self.server_address)
        assert requested_node_ips1["head_ip"] == "0.0.0.0:2"
        assert not requested_node_ips1["worker_ips"]

        # create cluster #2.
        request_get_node_ips = {
            "request_type": "get_node_ips",
            "cluster_name": "random_name_2",
            "num_workers": 0,
        }
        requested_node_ips2 = self.local_node_provider_cls.get_http_response(
            request_get_node_ips, self.server_address)
        assert requested_node_ips2["head_ip"] == "0.0.0.0:1"
        assert not requested_node_ips2["worker_ips"]
        (
            available_node_ips,
            total_node_ips,
            running_clusters,
        ) = self.local_node_provider_cls.get_http_response(
            self.request_get_status, self.server_address)
        assert not available_node_ips
        assert total_node_ips == ["0.0.0.0:1", "0.0.0.0:2"]
        expected_running_clusters = {
            "random_name_1": requested_node_ips1,
            "random_name_2": requested_node_ips2,
        }
        assert running_clusters == expected_running_clusters

        # Try to create third.
        # No more available resources for this cluster.
        request_get_node_ips = {
            "request_type": "get_node_ips",
            "cluster_name": "random_name_3",
            "num_workers": 0,
        }
        requested_node_ips3 = self.local_node_provider_cls.get_http_response(
            request_get_node_ips, self.server_address)
        assert not requested_node_ips3
        (
            available_node_ips,
            total_node_ips,
            running_clusters,
        ) = self.local_node_provider_cls.get_http_response(
            self.request_get_status, self.server_address)
        assert not available_node_ips
        assert total_node_ips == ["0.0.0.0:1", "0.0.0.0:2"]
        assert running_clusters == expected_running_clusters

        # Release cluster #1
        request_release_cluster = {
            "request_type": "release_cluster",
            "cluster_name": "random_name_1",
        }
        self.local_node_provider_cls.get_http_response(request_release_cluster,
                                                       self.server_address)
        assert not requested_node_ips3
        (
            available_node_ips,
            total_node_ips,
            running_clusters,
        ) = self.local_node_provider_cls.get_http_response(
            self.request_get_status, self.server_address)
        assert available_node_ips == ["0.0.0.0:2"]
        assert total_node_ips == ["0.0.0.0:1", "0.0.0.0:2"]
        expected_running_clusters = {
            "random_name_2": {
                "head_ip": "0.0.0.0:1",
                "worker_ips": []
            }
        }
        assert running_clusters == expected_running_clusters

        # Release cluster #2
        request_release_cluster = {
            "request_type": "release_cluster",
            "cluster_name": "random_name_2",
        }
        self.local_node_provider_cls.get_http_response(request_release_cluster,
                                                       self.server_address)
        (
            available_node_ips,
            total_node_ips,
            running_clusters,
        ) = self.local_node_provider_cls.get_http_response(
            self.request_get_status, self.server_address)
        assert available_node_ips == ["0.0.0.0:2", "0.0.0.0:1"]
        assert total_node_ips == ["0.0.0.0:1", "0.0.0.0:2"]
        assert not running_clusters

    def testLocalNodeProviderAutoManaged(self):
        """Test functionality of LocalNodeProvider with the on prem server."""

        config = {
            "cluster_name": "random_name",
            "min_workers": 0,
            "max_workers": 1,
            "initial_workers": 1,
            "provider": {
                "type": "local",
                "server_address": self.server_address,
            },
        }
        # Check bootstrap_config.
        new_config = self.local_node_provider_cls.bootstrap_config(config)
        assert new_config["provider"]["max_workers"] == 1

        # Check if initializing the cluster works (LocalNodeProvider).
        node_provider = self.local_node_provider_cls(
            new_config["provider"], new_config["cluster_name"])
        (
            available_node_ips,
            total_node_ips,
            running_clusters,
        ) = self.local_node_provider_cls.get_http_response(
            self.request_get_status, self.server_address)
        assert not available_node_ips
        assert total_node_ips == ["0.0.0.0:1", "0.0.0.0:2"]
        cluster_name = list(running_clusters.keys())[0]
        expected_running_clusters = {
            cluster_name: {
                "head_ip": "0.0.0.0:2",
                "worker_ips": ["0.0.0.0:1"]
            }
        }
        assert running_clusters == expected_running_clusters

        # Check some functionality of LocalNodeProvider functions.
        assert node_provider.is_terminated("0.0.0.0:2")
        assert node_provider.is_terminated("0.0.0.0:1")
        assert not node_provider.non_terminated_nodes({})
        tags = {}
        tags[TAG_RAY_NODE_TYPE] = NODE_TYPE_HEAD
        node_provider.create_node(node_config={}, tags=tags, count=1)
        tags[TAG_RAY_NODE_TYPE] = NODE_TYPE_WORKER
        node_provider.create_node(node_config={}, tags=tags, count=1)
        assert node_provider.is_running("0.0.0.0:2")
        assert node_provider.is_running("0.0.0.0:1")
        node_provider.terminate_node("0.0.0.0:1")
        # Make sure that terminating a worker node does not release it.
        (
            available_node_ips,
            total_node_ips,
            running_clusters,
        ) = self.local_node_provider_cls.get_http_response(
            self.request_get_status, self.server_address)
        assert not available_node_ips
        assert total_node_ips == ["0.0.0.0:1", "0.0.0.0:2"]
        assert running_clusters == expected_running_clusters
        # Make sure that terminating the head node releases the cluster.
        node_provider.terminate_node("0.0.0.0:2")
        (
            available_node_ips,
            total_node_ips,
            running_clusters,
        ) = self.local_node_provider_cls.get_http_response(
            self.request_get_status, self.server_address)
        assert available_node_ips == ["0.0.0.0:2", "0.0.0.0:1"]
        assert total_node_ips == ["0.0.0.0:1", "0.0.0.0:2"]
        assert not running_clusters


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
