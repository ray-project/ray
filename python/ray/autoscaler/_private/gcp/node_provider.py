import logging
import time
from functools import wraps
from threading import RLock
from typing import Dict, List, Tuple, Optional

import googleapiclient

from ray.autoscaler._private.gcp.config import (
    bootstrap_gcp,
    construct_clients_from_provider_config,
    get_node_type,
    num_tpus_from_accelerator_type,
    num_tpus_from_node_config,
)

# The logic has been abstracted away here to allow for different GCP resources
# (API endpoints), which can differ widely, making it impossible to use
# the same logic for everything.
from ray.autoscaler._private.gcp.node import GCPTPU  # noqa
from ray.autoscaler._private.gcp.node import (
    GCPCompute,
    GCPNode,
    GCPNodeType,
    GCPResource,
)
from ray.autoscaler.node_provider import NodeProvider

logger = logging.getLogger(__name__)

TPUCHIP = "TPUCHIP"


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
    def __init__(self, provider_config: dict, cluster_name: str):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.lock = RLock()
        self._construct_clients()

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_nodes: Dict[str, GCPNode] = {}

    def _construct_clients(self):
        _, _, compute, tpu = construct_clients_from_provider_config(
            self.provider_config
        )

        # Dict of different resources provided by GCP.
        # At this moment - Compute and TPUs
        self.resources: Dict[GCPNodeType, GCPResource] = {}

        # Compute is always required
        self.resources[GCPNodeType.COMPUTE] = GCPCompute(
            compute,
            self.provider_config["project_id"],
            self.provider_config["availability_zone"],
            self.cluster_name,
        )

        # if there are no TPU nodes defined in config, tpu will be None.
        if tpu is not None:
            self.resources[GCPNodeType.TPU] = GCPTPU(
                tpu,
                self.provider_config["project_id"],
                self.provider_config["availability_zone"],
                self.cluster_name,
            )

    def _get_resource_depending_on_node_name(self, node_name: str) -> GCPResource:
        """Return the resource responsible for the node, based on node_name.

        This expects the name to be in format '[NAME]-[UUID]-[TYPE]',
        where [TYPE] is either 'compute' or 'tpu' (see ``GCPNodeType``).
        """
        return self.resources[GCPNodeType.name_to_type(node_name)]

    @_retry
    def non_terminated_nodes(self, tag_filters: dict):
        with self.lock:
            instances = []

            for resource in self.resources.values():
                node_instances = resource.list_instances(tag_filters)
                instances += node_instances

            # Note: All the operations use "name" as the unique instance id
            self.cached_nodes = {i["name"]: i for i in instances}
            node_names = []
            for i in instances:
                instance_name = i["name"]
                num_tpus = self._num_tpus_from_instance(i)
                if num_tpus > 1:
                    for tpu_index in range(num_tpus):
                        tpu_node_name = self._add_tpu_chip_suffix(instance_name, tpu_index)
                        node_names.append(tpu_node_name)
                else:
                    node_names.append(instance_name)

            return [i["name"] for i in instances]

    def _add_tpu_chip_suffix(self, name, tpu_index) -> str:
        return f"{name}-{TPUCHIP}-{tpu_index}"

    def _name_and_tpu_index(self, suffixed_name) -> Tuple[str, int]:
        components = suffixed_name.strip("-")
        assert components[1] == TPUCHIP
        assert components[2].isnumeric()
        return components[0], int(components[2])

    def _is_tpu_chip(self, node_name):
        return TPUCHIP in node_name

    def _num_tpus_from_instance(self, instance) -> int:
        accelerators = instance.get("guestAccelerators", [])
        if accelerators:
            accelerator_type = accelerators[0].get("acceleratorType", "")
            return num_tpus_from_accelerator_type(accelerator_type)
        else:
            return 0

    def is_running(self, node_id: str):
        with self.lock:
            if self._is_tpu_chip(node_id):
                node_id, _ = self._name_and_tpu_index(node_id)
            node = self._get_cached_node(node_id)
            return node.is_running()

    def is_terminated(self, node_id: str):
        with self.lock:
            if self._is_tpu_chip(node_id):
                node_id, _ = self._name_and_tpu_index(node_id)
            node = self._get_cached_node(node_id)
            return node.is_terminated()

    def node_tags(self, node_id: str):
        with self.lock:
            if self._is_tpu_chip(node_id):
                node_id, tpu_index = self._name_and_tpu_index(node_id)
                node = self._get_cached_node(node_id)
                labels = node.get_labels()
                return self._get_tags_for_tpu_chip(labels, tpu_index)
            else:
                node = self._get_cached_node(node_id)
                return node.get_labels()

    def _get_tags_for_tpu_chip(self, labels: Dict[str, str], tpu_index) -> Dict[str, str]:
        tags: Dict[str, str] = {}
        for key in labels:
            tag_key, index = self._name_and_tpu_index(key)
            if index == tpu_index:
                tags[tag_key] = labels[key]
        return tags

    @_retry
    def set_node_tags(self, node_id: str, tags: dict):
        with self.lock:
            if self._is_tpu_chip(node_id):
                node_id, tpu_index = self._name_and_tpu_index(node_id)
                labels = {f"{self._add_tpu_chip_suffix(key, tpu_index)}": value for key, value in tags.items()}
            else:
                labels = tags

            node = self._get_node(node_id)
            resource = self._get_resource_depending_on_node_name(node_id)
            result = resource.set_labels(node=node, labels=labels)

            return result

    def external_ip(self, node_id: str) -> Optional[str]:
        # (Not necessary to add TPU host logic here.)
        with self.lock:
            node = self._get_cached_node(node_id)

            ip = node.get_external_ip()

            return ip

    def internal_ip(self, node_id: str) -> Optional[str]:
        with self.lock:
            ip_index = 0
            if self._is_tpu_chip(node_id):
                node_id, ip_index = self._name_and_tpu_index(node_id)
            node = self._get_cached_node(node_id)
            ip = node.get_internal_ip(ip_index)
            return ip

    @_retry
    def create_node(self, base_config: dict, tags: dict, count: int) -> Dict[str, dict]:
        """Creates instances.

        Returns dict mapping instance id to each create operation result for the created
        instances.
        """
        with self.lock:
            labels = tags  # gcp uses "labels" instead of aws "tags"
            num_tpus = num_tpus_from_node_config(base_config)
            if num_tpus > 1:
                labels = self._format_tpu_chip_labels(tags, num_tpus)
            else:
                labels = tags

            node_type = get_node_type(base_config)
            resource = self.resources[node_type]

            results = resource.create_instances(
                base_config, labels, count
            )  # type: List[Tuple[dict, str]]
            return {instance_id: result for result, instance_id in results}

    def _format_tpu_chip_labels(self, tags, num_tpus):
        """Represent each label key for tpu node in a pod as the usual
        tag key dash a numeric suffix.
        """
        return {
            f"{self._add_tpu_chip_suffix(key, tpu_index)}": value for key, value in tags.items() for tpu_index in range(num_tpus)
        }

    @_retry
    def terminate_node(self, node_id: str):
        with self.lock:
            resource = self._get_resource_depending_on_node_name(node_id)
            try:
                result = resource.delete_instance(
                    node_id=node_id,
                )
            except googleapiclient.errors.HttpError as http_error:
                if http_error.resp.status == 404:
                    logger.warning(
                        f"Tried to delete the node with id {node_id} "
                        "but it was already gone."
                    )
                else:
                    raise http_error from None
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