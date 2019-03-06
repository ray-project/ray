from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from uuid import uuid4
from threading import RLock
import time
import logging

from googleapiclient import discovery

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME
from ray.autoscaler.gcp.config import MAX_POLLS, POLL_INTERVAL

logger = logging.getLogger(__name__)

INSTANCE_NAME_MAX_LEN = 64
INSTANCE_NAME_UUID_LEN = 8


def raise_for_errors(results):
    exceptions = [
        Exception(result["error"]) for result in results if "error" in result
    ]

    if exceptions:
        raise Exception(exceptions)


def wait_for_compute_zone_operations(compute, project_name, operations, zone):
    """Poll for a set of compute zone operations until finished."""
    logger.info("wait_for_compute_zone_operation: "
                "Waiting for {} {} to finish...".format(
                    len(operations),
                    "operations" if len(operations) > 1 else "operation"))

    for _ in range(MAX_POLLS):
        results = [
            compute.zoneOperations().get(
                project=project_name, operation=operation["name"],
                zone=zone).execute() for operation in operations
        ]

        raise_for_errors(results)

        if all(result["status"] == "DONE" for result in results):
            logger.info("wait_for_compute_zone_operation: "
                        "All {} operations finished.".format(len(operations)))
            return results

        time.sleep(POLL_INTERVAL)

    logger.info(
        "wait_for_compute_zone_operation: "
        "MAX_POLLS ({}) exceeded. Could not finish all the {} {}."
        "".format(MAX_POLLS, len(operations),
                  "operations" if len(operations) > 1 else "operation"))

    return results


def wait_for_compute_zone_operation(compute, project_name, operation, zone):
    """Poll for a compute zone operation until finished."""
    return wait_for_compute_zone_operations(compute, project_name, [operation],
                                            zone)[0]


class GCPNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)

        self.lock = RLock()
        self.compute = discovery.build("compute", "v1")

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_nodes = {}

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

    def create_nodes(self, base_config, tags, count):
        with self.lock:
            labels = tags  # gcp uses "labels" instead of aws "tags"
            project_id = self.provider_config["project_id"]
            availability_zone = self.provider_config["availability_zone"]

            config = base_config.copy()

            name_label = labels[TAG_RAY_NODE_NAME]
            assert (len(name_label) <=
                    (INSTANCE_NAME_MAX_LEN - INSTANCE_NAME_UUID_LEN - 3)), (
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

            results = wait_for_compute_zone_operations(
                self.compute, project_id, operations, availability_zone)

            return results

    def terminate_nodes(self, node_ids):
        with self.lock:
            project_id = self.provider_config["project_id"]
            availability_zone = self.provider_config["availability_zone"]

            operations = [
                self.compute.instances().delete(
                    project=project_id,
                    zone=availability_zone,
                    instance=node_id,
                ).execute() for node_id in node_ids
            ]

            results = wait_for_compute_zone_operations(
                self.compute, project_id, operations, availability_zone)

            return results

    def terminate_node(self, node_id):
        return self.terminate_nodes([node_id])[0]

    def _get_nodes(self, node_ids):
        with self.lock:
            self.non_terminated_nodes({})  # Side effect: updates cache

            expected_nodes = set(node_ids)
            found_nodes = set(self.cached_nodes.keys())
            missing_nodes = expected_nodes - found_nodes

            if not missing_nodes:
                return [self.cached_nodes[node_id] for node_id in node_ids]

            # Note: "name" is a unique id for the instance
            instance_name_filter_expr = "(" + " OR ".join([
                "(name = {name})".format(name=name) for name in missing_nodes
            ]) + ")"

            response = self.compute.instances().list(
                project=self.provider_config["project_id"],
                zone=self.provider_config["availability_zone"],
                filter=instance_name_filter_expr,
            ).execute()

            fetched_nodes = response.get("items", [])
            missing_nodes -= set(node["name"] for node in fetched_nodes)
            assert not missing_nodes, missing_nodes

            self.cached_nodes.update(
                {node["name"]: node
                 for node in fetched_nodes})

            result = [self.cached_nodes[node_id] for node_id in node_ids]

            return result

    def _get_node(self, node_id):
        return self._get_nodes([node_id])[0]

    def _get_cached_nodes(self, node_ids):
        nodes_by_id = {
            node_id: self.cached_nodes.get(node_id, None)
            for node_id in node_ids
        }

        non_cached_node_ids = [k for k, v in nodes_by_id.items() if v is None]

        if non_cached_node_ids:
            # Note: this fetches the nodes and saves them to self.cached_nodes
            self._get_nodes(non_cached_node_ids)

        result = [self.cached_nodes[node_id] for node_id in node_ids]

        return result

    def _get_cached_node(self, node_id):
        return self._get_cached_nodes([node_id])[0]
