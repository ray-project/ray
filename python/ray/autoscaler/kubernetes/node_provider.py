from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.kubernetes import RAY_NAMESPACE
from ray.autoscaler.kubernetes.pod import default_pod_config

logger = logging.getLogger(__name__)


def to_label_selector(tags):
    label_selector = ""
    for k, v in tags.items():
        if label_selector != "":
            label_selector += ","
        label_selector += "{}={}".format(k, v)
    return label_selector


class KubernetesNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        try:
            config.load_incluster_config()
        except ConfigException:
            config.load_kube_config()
        self.core_api = client.CoreV1Api()
        self.pod_spec = default_pod_config

    def non_terminated_nodes(self, tag_filters):
        # Match pods that are in the 'Pending' or 'Running' phase.
        # Unfortunately there is no OR operator in field selectors, so we
        # have to match on NOT any of the other phases.
        field_selector = ",".join([
            "status.phase!=Failed", "status.phase!=Unknown",
            "status.phase!=Succeeded",
        ])

        label_selector = to_label_selector(tag_filters)
        pod_list = self.core_api.list_namespaced_pod(
            RAY_NAMESPACE,
            field_selector=field_selector,
            label_selector=label_selector)
        return [pod.metadata.name for pod in pod_list.items]

    def is_running(self, node_id):
        pod = self.core_api.read_namespaced_pod_status(node_id,
                                                       RAY_NAMESPACE)
        return pod.status.phase == "Running"

    def is_terminated(self, node_id):
        pod = self.core_api.read_namespaced_pod_status(node_id,
                                                       RAY_NAMESPACE)
        return pod.status.phase not in ["Running", "Pending"]

    def node_tags(self, node_id):
        pod = self.core_api.read_namespaced_pod_status(node_id,
                                                       RAY_NAMESPACE)
        return pod.metadata.labels

    def external_ip(self, node_id):
        raise NotImplementedError("No external IPs for kubernetes pods")

    def internal_ip(self, node_id):
        pod = self.core_api.read_namespaced_pod_status(node_id,
                                                       RAY_NAMESPACE)
        return pod.status.pod_ip

    def set_node_tags(self, node_id, tags):
        body = {"metadata": {"labels": tags}}
        self.core_api.patch_namespaced_pod(node_id, RAY_NAMESPACE, body)

    def create_node(self, node_config, tags, count):
        conf = node_config.copy()

        # Delete unsupported keys from the node config
        try:
            del conf["Resources"]
        except KeyError:
            pass

        # Allow users to add tags and override values of existing
        # tags with their own. This only applies to the resource type
        # "instance". All other resource types are appended to the list of
        # tag specs.
        # TODO

        # TODO: batch?
        body = self.pod_spec.copy()
        body["metadata"]["labels"] = tags
        logger.info("NodeProvider: calling create_namespaced_pod "
                    "(count={}).".format(count))
        for _ in range(count):
            self.core_api.create_namespaced_pod(RAY_NAMESPACE, body)

    def terminate_node(self, node_id):
        self.core_api.delete_namespaced_pod(node_id, RAY_NAMESPACE)

    def terminate_nodes(self, node_ids):
        for node_id in node_ids:
            self.terminate_node(node_id)
