import logging
from uuid import uuid4
from kubernetes.client.rest import ApiException

from ray.autoscaler.command_runner import KubernetesCommandRunner
from ray.autoscaler.kubernetes import core_api, log_prefix, extensions_beta_api
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.kubernetes.config import bootstrap_kubernetes
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME

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
        conf = node_config.copy()
        pod_spec = conf.get("pod", conf)
        service_spec = conf.get("service")
        ingress_spec = conf.get("ingress")
        node_uuid = str(uuid4())
        tags[TAG_RAY_CLUSTER_NAME] = self.cluster_name
        tags["ray-node-uuid"] = node_uuid
        pod_spec["metadata"]["namespace"] = self.namespace
        if "labels" in pod_spec["metadata"]:
            pod_spec["metadata"]["labels"].update(tags)
        else:
            pod_spec["metadata"]["labels"] = tags
        logger.info(log_prefix + "calling create_namespaced_pod "
                    "(count={}).".format(count))
        new_nodes = []
        for _ in range(count):
            pod = core_api().create_namespaced_pod(self.namespace, pod_spec)
            new_nodes.append(pod)

        new_svcs = []
        if service_spec is not None:
            logger.info(log_prefix + "calling create_namespaced_service "
                        "(count={}).".format(count))

            for new_node in new_nodes:

                metadata = service_spec.get("metadata", {})
                metadata["name"] = new_node.metadata.name
                service_spec["metadata"] = metadata
                service_spec["spec"]["selector"] = {"ray-node-uuid": node_uuid}
                svc = core_api().create_namespaced_service(
                    self.namespace, service_spec)
                new_svcs.append(svc)

        if ingress_spec is not None:
            logger.info(log_prefix + "calling create_namespaced_ingress "
                        "(count={}).".format(count))
            for new_svc in new_svcs:
                metadata = ingress_spec.get("metadata", {})
                metadata["name"] = new_svc.metadata.name
                ingress_spec["metadata"] = metadata
                ingress_spec = _add_service_name_to_service_port(
                    ingress_spec, new_svc.metadata.name)
                extensions_beta_api().create_namespaced_ingress(
                    self.namespace, ingress_spec)

    def terminate_node(self, node_id):
        logger.info(log_prefix + "calling delete_namespaced_pod")
        core_api().delete_namespaced_pod(node_id, self.namespace)
        try:
            core_api().delete_namespaced_service(node_id, self.namespace)
        except ApiException:
            pass
        try:
            extensions_beta_api().delete_namespaced_ingress(
                node_id,
                self.namespace,
            )
        except ApiException:
            pass

    def terminate_nodes(self, node_ids):
        for node_id in node_ids:
            self.terminate_node(node_id)

    def get_command_runner(self,
                           log_prefix,
                           node_id,
                           auth_config,
                           cluster_name,
                           process_runner,
                           use_internal_ip,
                           docker_config=None):
        return KubernetesCommandRunner(log_prefix, self.namespace, node_id,
                                       auth_config, process_runner)

    @staticmethod
    def bootstrap_config(cluster_config):
        return bootstrap_kubernetes(cluster_config)


def _add_service_name_to_service_port(spec, svc_name):
    """Goes recursively through the ingress manifest and adds the
    right serviceName next to every servicePort definition.
    """
    if isinstance(spec, dict):
        dict_keys = list(spec.keys())
        for k in dict_keys:
            spec[k] = _add_service_name_to_service_port(spec[k], svc_name)

            # The magic string ${RAY_POD_NAME} is replaced with
            # the true service name, which is equal to the worker pod name.
            if k == "serviceName":
                if spec[k] != "${RAY_POD_NAME}":
                    raise ValueError(
                        "The value of serviceName must be set to "
                        "${RAY_POD_NAME}. It is automatically replaced "
                        "when using the autoscaler.")
                else:
                    spec["serviceName"] = svc_name

    elif isinstance(spec, list):
        spec = [
            _add_service_name_to_service_port(item, svc_name) for item in spec
        ]

    return spec
