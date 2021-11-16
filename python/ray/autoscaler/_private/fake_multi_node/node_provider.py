import logging
import os
import json

import ray
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (TAG_RAY_NODE_KIND, NODE_KIND_HEAD,
                                 NODE_KIND_WORKER, TAG_RAY_USER_NODE_TYPE,
                                 TAG_RAY_NODE_NAME, TAG_RAY_NODE_STATUS,
                                 STATUS_UP_TO_DATE)

logger = logging.getLogger(__name__)

# We generate the node ids deterministically in the fake node provider, so that
# we can associate launched nodes with their resource reports. IDs increment
# starting with fffff*00000 for the head node, fffff*00001, etc. for workers.
FAKE_HEAD_NODE_ID = "fffffffffffffffffffffffffffffffffffffffffffffffffff00000"
FAKE_HEAD_NODE_TYPE = "ray.head.default"


class FakeMultiNodeProvider(NodeProvider):
    """A node provider that implements multi-node on a single machine.

    This is used for laptop mode testing of autoscaling functionality."""

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        if "RAY_FAKE_CLUSTER" not in os.environ:
            raise RuntimeError(
                "FakeMultiNodeProvider requires ray to be started with "
                "RAY_FAKE_CLUSTER=1 ray start ...")
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
        nodes = []
        for node_id in self._nodes:
            tags = self.node_tags(node_id)
            ok = True
            for k, v in tag_filters.items():
                if tags.get(k) != v:
                    ok = False
            if ok:
                nodes.append(node_id)
        return nodes

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
        ray_params = ray._private.parameter.RayParams(
            min_worker_port=0,
            max_worker_port=0,
            dashboard_port=None,
            num_cpus=resources.pop("CPU", 0),
            num_gpus=resources.pop("GPU", 0),
            object_store_memory=resources.pop("object_store_memory", None),
            resources=resources,
            redis_address="{}:6379".format(
                ray._private.services.get_node_ip_address()),
            env_vars={
                "RAY_OVERRIDE_NODE_ID_FOR_TESTING": next_id,
                "RAY_OVERRIDE_RESOURCES": json.dumps(resources),
            })
        node = ray.node.Node(
            ray_params, head=False, shutdown_at_exit=False, spawn_reaper=False)
        self._nodes[next_id] = {
            "tags": {
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
                TAG_RAY_USER_NODE_TYPE: node_type,
                TAG_RAY_NODE_NAME: next_id,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            },
            "node": node
        }

    def terminate_node(self, node_id):
        node = self._nodes.pop(node_id)["node"]
        self._kill_ray_processes(node)

    def _kill_ray_processes(self, node):
        node.kill_all_processes(check_alive=False, allow_graceful=True)

    @staticmethod
    def bootstrap_config(cluster_config):
        return cluster_config
