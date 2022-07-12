import copy
import os
import unittest
import sys

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import tempfile
import yaml

from ray.autoscaler.tags import TAG_RAY_NODE_KIND, NODE_KIND_HEAD
from ray.autoscaler.node_provider import NodeProvider
from ray._private.ray_constants import DEFAULT_PORT
from ray.ray_operator.operator_utils import cr_to_config
from ray.ray_operator.operator_utils import check_redis_password_not_specified
from ray.ray_operator.operator_utils import get_head_service
from ray.ray_operator.operator_utils import infer_head_port
from ray.autoscaler._private._kubernetes.node_provider import KubernetesNodeProvider
from ray.autoscaler._private.updater import NodeUpdaterThread
from ray.autoscaler._private.providers import _get_default_config

sys.modules["kopf"] = MagicMock()
from ray.ray_operator.operator import RayCluster  # noqa: E402

"""
Tests that, when the K8s operator launches a cluster, no files are mounted onto
the head node.
The main idea is to mock the NodeUpdaterThread to report if it received any
file mounts.
"""

# NodeUpdaterThread mock methods
START = "start"
JOIN = "join"


def mock_start(self):
    # Detects any file mounts passed in NodeUpdaterThread.__init__()
    if self.file_mounts:
        raise ValueError("File mounts in operator's code path.")


def mock_join(self):
    # Fake success
    self.exitcode = 0
    return


# RayCluster mock methods
SETUP_LOGGING = "setup_logging"
WRITE_CONFIG = "write_config"


def mock_setup_logging(self):
    return


def mock_write_config(self):
    # Use a named temporary file instead of a real one.
    self.config_file = tempfile.NamedTemporaryFile("w")
    self.config_path = self.config_file.name
    yaml.dump(self.config, self.config_file)
    self.config_file.flush()


# KubernetesNodeProvider mock methods
INIT = "__init__"
NON_TERMINATED_NODES = "non_terminated_nodes"
CREATE_NODE = "create_node"
BOOTSTRAP_CONFIG = "bootstrap_config"

HEAD_NODE_TAGS = {TAG_RAY_NODE_KIND: NODE_KIND_HEAD}


def mock_init(self, provider_config, cluster_name):
    # Adds an attribute to detect if the provider has created the head.
    NodeProvider.__init__(self, provider_config, cluster_name)
    self.cluster_name = cluster_name
    self.namespace = provider_config["namespace"]

    self._head_created = False


def mock_non_terminated_nodes(self, node_tags):
    # First time this is called, it returns an empty list.
    # Second time, returns a mock head node id.
    if HEAD_NODE_TAGS.items() <= node_tags.items() and self._head_created:
        # Second call.
        return ["HEAD"]
    elif node_tags == HEAD_NODE_TAGS:
        # First call.
        return []
    else:
        # Should not go here.
        raise ValueError("Test passed invalid parameters.")


def mock_create_node(self, node_config, tags, count):
    # Called during head node creation. Marks that a head node has been
    # created.
    if HEAD_NODE_TAGS.items() <= tags.items() and count == 1:
        self._head_created = True
    else:
        raise ValueError(f"Test passed invalid parameter {tags} {count}.")


def mock_bootstrap_config(cluster_config):
    # KubernetesNodeProvider.bootstrap_config has no side effects
    # on cluster_config -- the method just creates K8s API objects.
    # Thus it makes sense to dummy out the K8s API calls and return
    # the config.
    return cluster_config


def custom_resources():
    # K8s custom resources used in test.
    here = os.path.realpath(__file__)
    ray_python_root = os.path.dirname(os.path.dirname(here))
    ray_root = os.path.dirname(os.path.dirname(ray_python_root))
    relative_path = "deploy/components"
    abs_path = os.path.join(ray_root, relative_path)
    cluster = "example_cluster.yaml"
    path = os.path.join(abs_path, cluster)
    cr1 = yaml.safe_load(open(path).read())
    cr2 = copy.deepcopy(cr1)
    # Namespace uid and filled on resource creation in real life.
    cr1["metadata"]["uid"] = "abc"
    cr2["metadata"]["uid"] = "xyz"
    cr1["metadata"]["namespace"] = "ray"
    cr2["metadata"]["namespace"] = "ray"
    return cr1, cr2


class OperatorTest(unittest.TestCase):
    def test_no_file_mounts_k8s_operator_cluster_launch(self):
        with patch.object(NodeUpdaterThread, START, mock_start), patch.object(
            NodeUpdaterThread, JOIN, mock_join
        ), patch.object(RayCluster, SETUP_LOGGING, mock_setup_logging), patch.object(
            RayCluster, WRITE_CONFIG, mock_write_config
        ), patch.object(
            KubernetesNodeProvider, INIT, mock_init
        ), patch.object(
            KubernetesNodeProvider, NON_TERMINATED_NODES, mock_non_terminated_nodes
        ), patch.object(
            KubernetesNodeProvider, CREATE_NODE, mock_create_node
        ), patch.object(
            KubernetesNodeProvider, BOOTSTRAP_CONFIG, mock_bootstrap_config
        ), patch.object(
            os, "mkdir"
        ):

            cluster_cr1, cluster_cr2 = custom_resources()

            # Ensure that operator does not mount any files during cluster
            # launch.
            config1 = cr_to_config(cluster_cr1)
            config1["provider"]["namespace"] = "test"
            cluster1 = RayCluster(config1)
            cluster1.start_head()

            # Check that this test is working correctly by inserting extraneous
            # file mounts and confirming a ValueError from the mocked
            # NodeUpdater.
            config2 = cr_to_config(cluster_cr2)
            config2["provider"]["namespace"] = "test"
            # Note: There is no user interface for adding file mounts
            # to the config of a Ray cluster run via the operator.
            # This purely for purposes of testing this test.
            config2["file_mounts"] = {"remote_foo": os.path.abspath(__file__)}
            cluster2 = RayCluster(config2)
            with pytest.raises(ValueError):
                cluster2.start_head()

    def test_operator_redis_password(self):
        stop_cmd = "ray stop"
        start_cmd = (
            "ulimit -n 65536; ray start --head --no-monitor"
            " --dashboard-host 0.0.0.0 --redis-password 1234567"
        )
        cluster_config = {"head_start_ray_commands": [stop_cmd, start_cmd]}
        exception_message = (
            "name,namespace:The Ray Kubernetes Operator does"
            " not support setting a custom Redis password in"
            " Ray start commands."
        )
        with pytest.raises(ValueError, match=exception_message):
            check_redis_password_not_specified(cluster_config, "name", "namespace")
        start_cmd = (
            "ulimit -n 65536; ray start --head --no-monitor --dashboard-host 0.0.0.0"
        )
        cluster_config = {"head_start_ray_commands": [stop_cmd, start_cmd]}
        check_redis_password_not_specified(cluster_config, "name", "namespace")

    def test_operator_infer_port(self):
        stop_cmd = "ray stop"
        start_cmd = (
            "ulimit -n 65536; ray start --head --no-monitor"
            " --dashboard-host 0.0.0.0 --port 1234567"
        )
        cluster_config = {"head_start_ray_commands": [stop_cmd, start_cmd]}
        assert infer_head_port(cluster_config) == "1234567"
        # Use equals sign.
        start_cmd = (
            "ulimit -n 65536; ray start --head --no-monitor"
            " --dashboard-host 0.0.0.0 --port=1234567"
        )
        cluster_config = {"head_start_ray_commands": [stop_cmd, start_cmd]}
        assert infer_head_port(cluster_config) == "1234567"
        # Don't specify port
        start_cmd = (
            "ulimit -n 65536; ray start --head --no-monitor --dashboard-host 0.0.0.0"
        )
        cluster_config = {"head_start_ray_commands": [stop_cmd, start_cmd]}
        assert infer_head_port(cluster_config) == str(DEFAULT_PORT)

    def test_operator_configure_ports(self):
        cluster_name = "test-cluster"
        cluster_owner_reference = {}
        head_service_ports = [
            {"name": "test-port", "port": 123, "targetPort": 456},
            {"name": "client", "port": 555, "targetPort": 666},
        ]
        head_service = get_head_service(
            cluster_name, cluster_owner_reference, head_service_ports
        )
        expected_ports = [
            {"name": "client", "port": 555, "targetPort": 666},
            {"name": "dashboard", "protocol": "TCP", "port": 8265, "targetPort": 8265},
            {"name": "ray-serve", "protocol": "TCP", "port": 8000, "targetPort": 8000},
            {"name": "test-port", "port": 123, "targetPort": 456},
        ]
        assert head_service["spec"]["ports"] == expected_ports

        head_service = get_head_service(cluster_name, cluster_owner_reference, None)
        assert (
            head_service["spec"]["ports"]
            == _get_default_config({"type": "kubernetes"})["provider"]["services"][0][
                "spec"
            ]["ports"]
        )


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
