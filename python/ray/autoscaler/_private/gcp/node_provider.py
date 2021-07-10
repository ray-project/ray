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

logger = logging.getLogger(__name__)

INSTANCE_NAME_MAX_LEN = 64
INSTANCE_NAME_UUID_LEN = 8


def wait_for_compute_zone_operation(compute, project_name, operation, zone):
    """Poll for compute zone operation until finished."""
    logger.info("wait_for_compute_zone_operation: "
                "Waiting for operation {} to finish...".format(
                    operation["name"]))

    for _ in range(MAX_POLLS):
        result = compute.zoneOperations().get(
            project=project_name, operation=operation["name"],
            zone=zone).execute()
        if "error" in result:
            raise Exception(result["error"])

        if result["status"] == "DONE":
            logger.info("wait_for_compute_zone_operation: "
                        "Operation {} finished.".format(operation["name"]))
            break

        time.sleep(POLL_INTERVAL)

    return result


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
        self.cached_nodes = {}

    def _construct_clients(self):
        _, _, self.compute = construct_clients_from_provider_config(
            self.provider_config)

    @_retry
    def non_terminated_nodes(self, tag_filters):
        with self.lock:
            if tag_filters:
                label_filter_expr = "(" + " AND ".join([
                    "(labels.{key} = {value})".format(key=key, value=value)
                    for key, value in tag_filters.items()
                ]) + ")"
            else:
                label_filter_expr = ""

            instance_state_filter_expr = "(" + " OR ".join([
                "(status = {status})".format(status=status)
                for status in {"PROVISIONING", "STAGING", "RUNNING"}
            ]) + ")"

            cluster_name_filter_expr = ("(labels.{key} = {value})"
                                        "".format(
                                            key=TAG_RAY_CLUSTER_NAME,
                                            value=self.cluster_name))

            not_empty_filters = [
                f for f in [
                    label_filter_expr,
                    instance_state_filter_expr,
                    cluster_name_filter_expr,
                ] if f
            ]

            filter_expr = " AND ".join(not_empty_filters)

            response = self.compute.instances().list(
                project=self.provider_config["project_id"],
                zone=self.provider_config["availability_zone"],
                filter=filter_expr,
            ).execute()

            instances = response.get("items", [])
            # Note: All the operations use "name" as the unique instance id
            self.cached_nodes = {i["name"]: i for i in instances}

            return [i["name"] for i in instances]

    def is_running(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)
            return node["status"] == "RUNNING"

    def is_terminated(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)
            return node["status"] not in {"PROVISIONING", "STAGING", "RUNNING"}

    def node_tags(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)
            labels = node.get("labels", {})
            return labels

    @_retry
    def set_node_tags(self, node_id, tags):
        with self.lock:
            labels = tags
            project_id = self.provider_config["project_id"]
            availability_zone = self.provider_config["availability_zone"]

            node = self._get_node(node_id)
            operation = self.compute.instances().setLabels(
                project=project_id,
                zone=availability_zone,
                instance=node_id,
                body={
                    "labels": dict(node["labels"], **labels),
                    "labelFingerprint": node["labelFingerprint"]
                }).execute()

            result = wait_for_compute_zone_operation(
                self.compute, project_id, operation, availability_zone)

            return result

    def external_ip(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)

            def get_external_ip(node):
                return node.get("networkInterfaces", [{}])[0].get(
                    "accessConfigs", [{}])[0].get("natIP", None)

            ip = get_external_ip(node)
            if ip is None:
                node = self._get_node(node_id)
                ip = get_external_ip(node)

            return ip

    def internal_ip(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)

            def get_internal_ip(node):
                return node.get("networkInterfaces", [{}])[0].get("networkIP")

            ip = get_internal_ip(node)
            if ip is None:
                node = self._get_node(node_id)
                ip = get_internal_ip(node)

            return ip

    @_retry
    def create_node(self, base_config, tags, count) -> None:
        with self.lock:
            labels = tags  # gcp uses "labels" instead of aws "tags"
            project_id = self.provider_config["project_id"]
            availability_zone = self.provider_config["availability_zone"]

            config = base_config.copy()

            name_label = labels[TAG_RAY_NODE_NAME]
            assert (len(name_label) <=
                    (INSTANCE_NAME_MAX_LEN - INSTANCE_NAME_UUID_LEN - 1)), (
                        name_label, len(name_label))

            machine_type = ("zones/{zone}/machineTypes/{machine_type}"
                            "".format(
                                zone=availability_zone,
                                machine_type=base_config["machineType"]))
            labels = dict(config.get("labels", {}), **labels)

            config.update({
                "machineType": machine_type,
                "labels": dict(labels,
                               **{TAG_RAY_CLUSTER_NAME: self.cluster_name}),
            })

            operations = [
                self.compute.instances().insert(
                    project=project_id,
                    zone=availability_zone,
                    body=dict(
                        config, **{
                            "name": ("{name_label}-{uuid}".format(
                                name_label=name_label,
                                uuid=uuid4().hex[:INSTANCE_NAME_UUID_LEN]))
                        })).execute() for i in range(count)
            ]

            for operation in operations:
                wait_for_compute_zone_operation(self.compute, project_id,
                                                operation, availability_zone)

    @_retry
    def terminate_node(self, node_id):
        with self.lock:
            project_id = self.provider_config["project_id"]
            availability_zone = self.provider_config["availability_zone"]

            operation = self.compute.instances().delete(
                project=project_id,
                zone=availability_zone,
                instance=node_id,
            ).execute()

            result = wait_for_compute_zone_operation(
                self.compute, project_id, operation, availability_zone)

            return result

    @_retry
    def _get_node(self, node_id):
        self.non_terminated_nodes({})  # Side effect: updates cache

        with self.lock:
            if node_id in self.cached_nodes:
                return self.cached_nodes[node_id]

            instance = self.compute.instances().get(
                project=self.provider_config["project_id"],
                zone=self.provider_config["availability_zone"],
                instance=node_id,
            ).execute()

            return instance

    def _get_cached_node(self, node_id):
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        return self._get_node(node_id)

    @staticmethod
    def bootstrap_config(cluster_config):
        return bootstrap_gcp(cluster_config)
