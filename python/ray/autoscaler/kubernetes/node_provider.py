import logging

from ray.autoscaler.kubernetes import core_api, log_prefix
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME
from ray.autoscaler.updater import KubernetesCommandRunner

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
        self.cluster_name = cluster_name
        self.namespace = provider_config["namespace"]

    def non_terminated_nodes(self, tag_filters):
        # Match pods that are in the 'Pending' or 'Running' phase.
        # Unfortunately there is no OR operator in field selectors, so we
        # have to match on NOT any of the other phases.
        field_selector = ",".join([
            "status.phase!=Failed",
            "status.phase!=Unknown",
            "status.phase!=Succeeded",
            "status.phase!=Terminating",
        ])

        tag_filters[TAG_RAY_CLUSTER_NAME] = self.cluster_name
        label_selector = to_label_selector(tag_filters)
        pod_list = core_api().list_namespaced_pod(
            self.namespace,
            field_selector=field_selector,
            label_selector=label_selector)

        return [pod.metadata.name for pod in pod_list.items]

    def is_running(self, node_id):
        pod = core_api().read_namespaced_pod_status(node_id, self.namespace)
        return pod.status.phase == "Running"

    def is_terminated(self, node_id):
        pod = core_api().read_namespaced_pod_status(node_id, self.namespace)
        return pod.status.phase not in ["Running", "Pending"]

    def node_tags(self, node_id):
        pod = core_api().read_namespaced_pod_status(node_id, self.namespace)
        return pod.metadata.labels

    def external_ip(self, node_id):
        raise NotImplementedError("Must use internal IPs with Kubernetes.")

    def internal_ip(self, node_id):
        pod = core_api().read_namespaced_pod_status(node_id, self.namespace)
        return pod.status.pod_ip

    def set_node_tags(self, node_id, tags):
        pod = core_api().read_namespaced_pod_status(node_id, self.namespace)
        pod.metadata.labels.update(tags)
        core_api().patch_namespaced_pod(node_id, self.namespace, pod)

    def create_node(self, node_config, tags, count):
        pod_spec = node_config.copy()
        tags[TAG_RAY_CLUSTER_NAME] = self.cluster_name
        pod_spec["metadata"]["namespace"] = self.namespace
        if "labels" in pod_spec["metadata"]:
            pod_spec["metadata"]["labels"].update(tags)
        else:
            pod_spec["metadata"]["labels"] = tags
        logger.info(log_prefix + "calling create_namespaced_pod "
                    "(count={}).".format(count))
        for _ in range(count):
            core_api().create_namespaced_pod(self.namespace, pod_spec)

    def terminate_node(self, node_id):
        core_api().delete_namespaced_pod(node_id, self.namespace)

    def terminate_nodes(self, node_ids):
        for node_id in node_ids:
            self.terminate_node(node_id)

    def get_command_runner(self, log_prefix, node_id, auth_config,
                           cluster_name, process_runner, use_internal_ip):
        return KubernetesCommandRunner(log_prefix, self.namespace, node_id,
                                       auth_config, process_runner)
