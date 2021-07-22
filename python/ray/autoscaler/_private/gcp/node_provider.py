from typing import Dict, Set
from functools import wraps
from uuid import uuid4
from threading import RLock
import time
import logging

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME
from ray.autoscaler._private.gcp.config import bootstrap_gcp
from ray.autoscaler._private.gcp.config import MAX_POLLS, POLL_INTERVAL, \
    construct_clients_from_provider_config

from ray.autoscaler._private.gcp.node import (
    GCPResource, GCPNode, GCPCompute, GCPComputeNode, GCPTPU, GCPTPUNode,
    GCPNodeType, INSTANCE_NAME_MAX_LEN, INSTANCE_NAME_UUID_LEN)

logger = logging.getLogger(__name__)


def _get_node_type(node: dict) -> GCPNodeType:
    """Returns True if node is a TPU, false if Compute"""
    if "machineType" not in node and "acceleratorType" not in node:
        raise ValueError(
            "Invalid node. For a Compute instance, 'machineType' is "
            "required."
            "For a TPU instance, 'acceleratorType' and no 'machineType' "
            "is required.")
    if "machineType" not in node and "acceleratorType" in node:
        return GCPNodeType.TPU
    return GCPNodeType.COMPUTE


def _retry(method, max_tries=5, backoff_s=1):
    """Retry decorator for methods of GCPNodeProvider.

    Upon catching BrokenPipeError, API clients are rebuilt and
    decorated methods are retried.

    Work-around for https://github.com/ray-project/ray/issues/16072.
    Based on https://github.com/kubeflow/pipelines/pull/5250/files.
    """

    @wraps(method)
    def method_with_retries(self, *args, **kwargs):
        try_count = 0
        while try_count < max_tries:
            try:
                return method(self, *args, **kwargs)
            except BrokenPipeError:
                logger.warning("Caught a BrokenPipeError. Retrying.")
                try_count += 1
                if try_count < max_tries:
                    self._construct_clients()
                    time.sleep(backoff_s)
                else:
                    raise

    return method_with_retries


class GCPNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.lock = RLock()
        self._construct_clients()

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_nodes: Dict[str, GCPNode] = {}

    def _construct_clients(self):
        _, _, compute, tpu = construct_clients_from_provider_config(
            self.provider_config)
        self.resources: Dict[GCPNodeType, GCPResource] = {}

        self.resources[GCPNodeType.COMPUTE] = GCPCompute(
            compute, self.provider_config["project_id"],
            self.provider_config["availability_zone"], self.cluster_name)
        self.resources[GCPNodeType.TPU] = GCPTPU(
            tpu, self.provider_config["project_id"],
            self.provider_config["availability_zone"], self.cluster_name)

    def _get_resource_depending_on_node_name(self,
                                             node_name: str) -> GCPResource:
        return self.resources[GCPNodeType.name_to_type(node_name)]

    @_retry
    def non_terminated_nodes(self, tag_filters):
        with self.lock:
            instances = []

            for resource in self.resources.values():
                node_instances = resource.list_instances(tag_filters)
                logger.info(f"{type(resource)}: {node_instances}")
                instances += node_instances

            # Note: All the operations use "name" as the unique instance id
            self.cached_nodes = {i["name"]: i for i in instances}
            return [i["name"] for i in instances]

    def is_running(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)
            return node.is_running()

    def is_terminated(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)
            return node.is_terminated()

    def node_tags(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)
            return node.get_tags()

    @_retry
    def set_node_tags(self, node_id, tags):
        with self.lock:
            labels = tags
            node = self._get_node(node_id)

            resource = self._get_resource_depending_on_node_name(node_id)

            result = resource.set_labels(node=node, labels=labels)

            return result

    def external_ip(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)

            ip = node.get_external_ip()
            if ip is None:
                node = self._get_node(node_id)
                ip = node.get_external_ip()

            return ip

    def internal_ip(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)

            ip = node.get_internal_ip()
            if ip is None:
                node = self._get_node(node_id)
                ip = node.get_internal_ip()

            return ip

    @_retry
    def create_node(self, base_config, tags, count) -> None:
        with self.lock:
            labels = tags  # gcp uses "labels" instead of aws "tags"

            node_type = _get_node_type(base_config)
            resource = self.resources[node_type]

            resource.create_instances(base_config, labels, count)

    @_retry
    def terminate_node(self, node_id):
        with self.lock:
            resource = self._get_resource_depending_on_node_name(node_id)
            result = resource.delete_instance(node_id=node_id, )
            return result

    @_retry
    def _get_node(self, node_id: str) -> GCPNode:
        self.non_terminated_nodes({})  # Side effect: updates cache

        with self.lock:
            if node_id in self.cached_nodes:
                return self.cached_nodes[node_id]

            resource = self._get_resource_depending_on_node_name(node_id)
            instance = resource.get_instance(node_id=node_id)

            return instance

    def _get_cached_node(self, node_id: str) -> GCPNode:
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        return self._get_node(node_id)

    @staticmethod
    def bootstrap_config(cluster_config):
        return bootstrap_gcp(cluster_config)
