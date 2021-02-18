import os
import unittest
from unittest.mock import patch

import pytest
import tempfile
import yaml

from ray.autoscaler.tags import TAG_RAY_NODE_KIND, NODE_KIND_HEAD
from ray.autoscaler.node_provider import NodeProvider
from ray.ray_operator.operator import RayCluster
from ray.ray_operator.operator_utils import cr_to_config
from ray.autoscaler._private.kubernetes.node_provider import\
    KubernetesNodeProvider
from ray.autoscaler._private.updater import NodeUpdaterThread
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
    relative_path = "autoscaler/kubernetes/operator_configs"
    abs_path = os.path.join(ray_python_root, relative_path)
    cluster1, cluster2 = "example_cluster.yaml", "example_cluster2.yaml"
    path1, path2 = os.path.join(abs_path, cluster1), os.path.join(
        abs_path, cluster2)
    cr1, cr2 = (yaml.safe_load(open(path1).read()),
                yaml.safe_load(open(path2).read()))
    # Metadata and field is filled by K8s in real life.
    cr1["metadata"]["uid"] = "abc"
    cr2["metadata"]["uid"] = "xyz"
    return cr1, cr2


class OperatorTest(unittest.TestCase):
    def test_no_file_mounts_k8s_operator_cluster_launch(self):
        with patch.object(NodeUpdaterThread, START, mock_start),\
                patch.object(NodeUpdaterThread, JOIN, mock_join),\
                patch.object(RayCluster, SETUP_LOGGING, mock_setup_logging),\
                patch.object(RayCluster, WRITE_CONFIG, mock_write_config),\
                patch.object(KubernetesNodeProvider, INIT, mock_init),\
                patch.object(KubernetesNodeProvider, NON_TERMINATED_NODES,
                             mock_non_terminated_nodes),\
                patch.object(KubernetesNodeProvider, CREATE_NODE,
                             mock_create_node),\
                patch.object(KubernetesNodeProvider, BOOTSTRAP_CONFIG,
                             mock_bootstrap_config):

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


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
