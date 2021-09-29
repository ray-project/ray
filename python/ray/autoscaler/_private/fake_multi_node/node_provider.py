import logging
import json
from typing import Tuple, List
import subprocess

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (TAG_RAY_NODE_KIND, NODE_KIND_HEAD,
                                 NODE_KIND_WORKER, TAG_RAY_USER_NODE_TYPE,
                                 TAG_RAY_NODE_NAME, TAG_RAY_NODE_STATUS,
                                 STATUS_UP_TO_DATE)
from ray.autoscaler._private.util import format_readonly_node_type


logger = logging.getLogger(__name__)

# RAY_OVERRIDE_NODE_ID_FOR_TESTING=fffffffffffffffffffffffffffffffffffffffffffffffffff00000
# ray start \
#   --autoscaling-config=~/Desktop/foo.yaml --head --block
FAKE_HEAD_NODE_ID = "fffffffffffffffffffffffffffffffffffffffffffffffffff00000"
FAKE_HEAD_NODE_TYPE = "ray.head.default"


class FakeMultiNodeProvider(NodeProvider):
    """A node provider that implements multi-node on a single machine.

    This is used for laptop mode testing of autoscaling functionality."""

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self._nodes = {
            FAKE_HEAD_NODE_ID: {
                "tags": {
                    TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                    TAG_RAY_USER_NODE_TYPE: FAKE_HEAD_NODE_TYPE,
                    TAG_RAY_NODE_NAME: FAKE_HEAD_NODE_ID,
                    TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                }
            },
        }
        self._next_node_id = 0

    def _next_hex_node_id(self):
        self._next_node_id += 1
        base = "fffffffffffffffffffffffffffffffffffffffffffffffffff"
        return base + str(self._next_node_id).zfill(5)

    def non_terminated_nodes(self, tag_filters):
        return list(self._nodes.keys())

    def is_running(self, node_id):
        return node_id in self._nodes

    def is_terminated(self, node_id):
        return node_id not in self._nodes

    def node_tags(self, node_id):
        return self._nodes[node_id]["tags"]

    def external_ip(self, node_id):
        return node_id

    def internal_ip(self, node_id):
        return node_id

    def set_node_tags(self, node_id, tags):
        raise AssertionError("Readonly node provider cannot be updated")

    def create_node_with_resources(self, node_config, tags, count, resources):
        node_type = tags[TAG_RAY_USER_NODE_TYPE]
        next_id = self._next_hex_node_id()
        self._nodes[next_id] = {
            "tags": {
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
                TAG_RAY_USER_NODE_TYPE: node_type,
                TAG_RAY_NODE_NAME: next_id,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            }
        }
        cmd = "/home/eric/.local/bin/ray start --address=localhost:6379"
        env = {
            "RAY_OVERRIDE_NODE_ID_FOR_TESTING": next_id,
            "RAY_OVERRIDE_RESOURCES": json.dumps(resources),
        }
        try:
            output = subprocess.check_output(
                cmd, stderr=subprocess.STDOUT, shell=True, env=env,
                universal_newlines=True)
        except subprocess.CalledProcessError as e:
            logger.exception(
                "Failed: exit={}, output={}".format(e.returncode, e.output))
        else:
            logger.info("Output: \n{}".format(output))

    def terminate_node(self, node_id):
        raise AssertionError("Readonly node provider cannot be updated")

    @staticmethod
    def bootstrap_config(cluster_config):
        return cluster_config
