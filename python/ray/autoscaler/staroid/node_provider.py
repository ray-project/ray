import os, logging, time, requests
from staroid import Staroid
from kubernetes import client, config
import socket
from contextlib import closing

from uuid import uuid4
from ray.autoscaler.command_runner import KubernetesCommandRunner
from ray.autoscaler.kubernetes import core_api, log_prefix, extensions_beta_api
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME

logger = logging.getLogger(__name__)


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('localhost', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]

def to_label_selector(tags):
    label_selector = ""
    for k, v in tags.items():
        if label_selector != "":
            label_selector += ","
        label_selector += "{}={}".format(k, v)
    return label_selector

class StaroidNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.__cached = {}

        self.__star = Staroid(
            access_token=provider_config["access_token"],
            account=provider_config["account"])

        self.__ske = self._get_config_or_env(provider_config, "ske", "STAROID_SKE")
        self.__ske_region = self._get_config_or_env(provider_config, "ske_regiono", "STAROID_SKE_REGION")

    def _get_config_or_env(self, config, config_key, env_name):
        value = None
        # check env first, so config can override env later
        if env_name in os.environ:
            value = os.environ[env_name]

        if config_key in config and config[config_key] != None:
            value = config[config_key]

        return value

    def _connect_kubeapi(self, ns_api, instance_name):
        if instance_name in self.__cached:
            return self.__cached[instance_name]["kube_client"]

        # start a shell service to create secure tunnel
        ns_api.shell_start(instance_name)

        local_port = find_free_port()
        remote_port = 57683 # fixed port number for kube api access through shell service in staroid

        # start a tunnel
        ns_api.start_tunnel(
            instance_name,
            ["{}:localhost:{}".format(local_port, remote_port)]
        )

        # wait for tunnel to be established by checking /version
        local_kube_api_addr = "http://localhost:{}".format(local_port)
        start_time = time.time()
        timeout = 300
        established = False
        while time.time() - start_time < timeout:
            try:
                r = requests.get("{}/version".format(local_kube_api_addr), timeout=(3, 5))
                if r.status_code == 200:
                    established = True
                    break
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(3)

        if established:
            kube_conf = client.Configuration()
            kube_conf.host = local_kube_api_addr
            kube_client = client.ApiClient(kube_conf)
            ns = ns_api.get(instance_name)
            self.__cached[instance_name] = {
                "kube_client": kube_client,
                "ns": ns,
                "api_server": local_kube_api_addr
            }
            self.namespace = ns.namespace()
            return kube_client
        else:
            self.__cached[instance_name] = None
            return None

    def non_terminated_nodes(self, tag_filters):
        print("non_terminated_nodes")
        instance_name = self.cluster_name

        kube_client = None
        ns_api = None
        if instance_name not in self.__cached:
            # check if ske exists
            cluster_api = self.__star.cluster()
            ske = cluster_api.get(self.__ske)
            if ske == None: # ske not exists
                return []

            # check if ray cluster instance exists
            ns_api = self.__star.namespace(ske)
            ns = ns_api.get(instance_name)
            if ns == None: # instance not exists
                return []

        kube_client = self._connect_kubeapi(ns_api, instance_name)
        core_api = client.CoreV1Api(kube_client)

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
        pod_list = core_api.list_namespaced_pod(
            self.namespace,
            field_selector=field_selector,
            label_selector=label_selector)

        return [pod.metadata.name for pod in pod_list.items]

    def is_running(self, node_id):
        kube_client = self.__cached[self.cluster_name]["kube_client"]
        core_api = client.CoreV1Api(kube_client)

        pod = core_api.read_namespaced_pod_status(node_id, self.namespace)
        return pod.status.phase == "Running"

    def is_terminated(self, node_id):
        kube_client = self.__cached[self.cluster_name]["kube_client"]
        core_api = client.CoreV1Api(kube_client)

        pod = core_api.read_namespaced_pod_status(node_id, self.namespace)
        return pod.status.phase not in ["Running", "Pending"]

    def node_tags(self, node_id):
        kube_client = self.__cached[self.cluster_name]["kube_client"]
        core_api = client.CoreV1Api(kube_client)

        pod = core_api.read_namespaced_pod_status(node_id, self.namespace)
        return pod.metadata.labels

    def external_ip(self, node_id):
        raise NotImplementedError("Must use internal IPs with Kubernetes.")

    def internal_ip(self, node_id):
        kube_client = self.__cached[self.cluster_name]["kube_client"]
        core_api = client.CoreV1Api(kube_client)

        pod = core_api.read_namespaced_pod_status(node_id, self.namespace)
        return pod.status.pod_ip

    def set_node_tags(self, node_id, tags):
        kube_client = self.__cached[self.cluster_name]["kube_client"]
        core_api = client.CoreV1Api(kube_client)

        pod = core_api.read_namespaced_pod_status(node_id, self.namespace)
        pod.metadata.labels.update(tags)
        core_api.patch_namespaced_pod(node_id, self.namespace, pod)

    def create_node(self, node_config, tags, count):
        print("node_config -- " + str(node_config))
        print("tags -- " + str(tags))
        print("count -- " + str(count))

        instance_name = self.cluster_name

        # get or create ske
        cluster_api = self.__star.cluster()
        ske = cluster_api.create(self.__ske, self.__ske_region)
        if ske == None:
            raise Exception("Failed to create an SKE '{}' in '{}' region".format(self.__ske, self.__ske_region))

        # create a namespace
        ns_api = self.__star.namespace(ske)
        ns = ns_api.create(instance_name, "GITHUB/staroids/namespace:master")
        if ns == None:
            raise Exception("Failed to create a cluster '{}' in SKE '{}'".format(instance_name, self.__ske))

        # kube client
        kube_client = self._connect_kubeapi(ns_api, instance_name)
        core_api = client.CoreV1Api(kube_client)

        # create head nodoe
        conf = node_config.copy()
        pod_spec = conf.get("pod", conf)
        service_spec = conf.get("service")
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
            pod = core_api.create_namespaced_pod(self.namespace, pod_spec)
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
                svc = core_api.create_namespaced_service(
                    self.namespace, service_spec)
                new_svcs.append(svc)

    def terminate_node(self, node_id):
        logger.info(log_prefix + "calling delete_namespaced_pod")
        kube_client = self.__cached[self.cluster_name]["kube_client"]
        core_api = client.CoreV1Api(kube_client)

        core_api.delete_namespaced_pod(node_id, self.namespace)
        try:
            core_api.delete_namespaced_service(node_id, self.namespace)
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
        command_runner = KubernetesCommandRunner(log_prefix, self.namespace, node_id,
                                       auth_config, process_runner)

        # set api server address
        command_runner.kubectl.extend([
            "--server",
            self.__cached[cluster_name]["api_server"]
        ])

        return command_runner

    @staticmethod
    def bootstrap_config(cluster_config):
        """Bootstraps the cluster config by adding env defaults if needed."""
        return cluster_config
