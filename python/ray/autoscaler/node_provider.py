import importlib
import logging
import json
import os
from typing import Any, Dict

import yaml

from ray.autoscaler._private.command_runner import \
    SSHCommandRunner, DockerCommandRunner

logger = logging.getLogger(__name__)

# For caching provider instantiations across API calls of one python session
_provider_instances = {}


def _import_aws(provider_config):
    from ray.autoscaler._private.aws.node_provider import AWSNodeProvider
    return AWSNodeProvider


def _import_gcp(provider_config):
    from ray.autoscaler._private.gcp.node_provider import GCPNodeProvider
    return GCPNodeProvider


def _import_azure(provider_config):
    from ray.autoscaler._private.azure.node_provider import AzureNodeProvider
    return AzureNodeProvider


def _import_local(provider_config):
    if "coordinator_address" in provider_config:
        from ray.autoscaler._private.local.coordinator_node_provider import (
            CoordinatorSenderNodeProvider)
        return CoordinatorSenderNodeProvider
    else:
        from ray.autoscaler._private.local.node_provider import \
            LocalNodeProvider
        return LocalNodeProvider


def _import_kubernetes(provider_config):
    from ray.autoscaler._private.kubernetes.node_provider import \
        KubernetesNodeProvider
    return KubernetesNodeProvider


def _import_staroid(provider_config):
    from ray.autoscaler._private.staroid.node_provider import \
        StaroidNodeProvider
    return StaroidNodeProvider


def _load_local_example_config():
    import ray.autoscaler.local as ray_local
    return os.path.join(
        os.path.dirname(ray_local.__file__), "example-full.yaml")


def _load_kubernetes_example_config():
    import ray.autoscaler.kubernetes as ray_kubernetes
    return os.path.join(
        os.path.dirname(ray_kubernetes.__file__), "example-full.yaml")


def _load_aws_example_config():
    import ray.autoscaler.aws as ray_aws
    return os.path.join(os.path.dirname(ray_aws.__file__), "example-full.yaml")


def _load_gcp_example_config():
    import ray.autoscaler.gcp as ray_gcp
    return os.path.join(os.path.dirname(ray_gcp.__file__), "example-full.yaml")


def _load_azure_example_config():
    import ray.autoscaler.azure as ray_azure
    return os.path.join(
        os.path.dirname(ray_azure.__file__), "example-full.yaml")


def _load_staroid_example_config():
    import ray.autoscaler.staroid as ray_staroid
    return os.path.join(
        os.path.dirname(ray_staroid.__file__), "example-full.yaml")


def _import_external(provider_config):
    provider_cls = _load_class(path=provider_config["module"])
    return provider_cls


_NODE_PROVIDERS = {
    "local": _import_local,
    "aws": _import_aws,
    "gcp": _import_gcp,
    "azure": _import_azure,
    "staroid": _import_staroid,
    "kubernetes": _import_kubernetes,
    "external": _import_external  # Import an external module
}

_PROVIDER_PRETTY_NAMES = {
    "local": "Local",
    "aws": "AWS",
    "gcp": "GCP",
    "azure": "Azure",
    "staroid": "Staroid",
    "kubernetes": "Kubernetes",
    "external": "External"
}

_DEFAULT_CONFIGS = {
    "local": _load_local_example_config,
    "aws": _load_aws_example_config,
    "gcp": _load_gcp_example_config,
    "azure": _load_azure_example_config,
    "staroid": _load_staroid_example_config,
    "kubernetes": _load_kubernetes_example_config,
}


def _load_class(path):
    """Load a class at runtime given a full path.

    Example of the path: mypkg.mysubpkg.myclass
    """
    class_data = path.split(".")
    if len(class_data) < 2:
        raise ValueError(
            "You need to pass a valid path like mymodule.provider_class")
    module_path = ".".join(class_data[:-1])
    class_str = class_data[-1]
    module = importlib.import_module(module_path)
    return getattr(module, class_str)


def _get_node_provider(provider_config: Dict[str, Any],
                       cluster_name: str,
                       use_cache: bool = True) -> Any:
    importer = _NODE_PROVIDERS.get(provider_config["type"])
    if importer is None:
        raise NotImplementedError("Unsupported node provider: {}".format(
            provider_config["type"]))
    provider_cls = importer(provider_config)
    provider_key = (json.dumps(provider_config, sort_keys=True), cluster_name)
    if use_cache and provider_key in _provider_instances:
        return _provider_instances[provider_key]

    new_provider = provider_cls(provider_config, cluster_name)

    if use_cache:
        _provider_instances[provider_key] = new_provider

    return new_provider


def _get_default_config(provider_config):
    if provider_config["type"] == "external":
        return {}
    load_config = _DEFAULT_CONFIGS.get(provider_config["type"])
    if load_config is None:
        raise NotImplementedError("Unsupported node provider: {}".format(
            provider_config["type"]))
    path_to_default = load_config()
    with open(path_to_default) as f:
        defaults = yaml.safe_load(f)

    return defaults


class NodeProvider:
    """Interface for getting and returning nodes from a Cloud.

    NodeProviders are namespaced by the `cluster_name` parameter; they only
    operate on nodes within that namespace.

    Nodes may be in one of three states: {pending, running, terminated}. Nodes
    appear immediately once started by `create_node`, and transition
    immediately to terminated when `terminate_node` is called.
    """

    def __init__(self, provider_config, cluster_name):
        self.provider_config = provider_config
        self.cluster_name = cluster_name
        self._internal_ip_cache = {}
        self._external_ip_cache = {}

    def non_terminated_nodes(self, tag_filters):
        """Return a list of node ids filtered by the specified tags dict.

        This list must not include terminated nodes. For performance reasons,
        providers are allowed to cache the result of a call to nodes() to
        serve single-node queries (e.g. is_running(node_id)). This means that
        nodes() must be called again to refresh results.

        Examples:
            >>> provider.non_terminated_nodes({TAG_RAY_NODE_KIND: "worker"})
            ["node-1", "node-2"]
        """
        raise NotImplementedError

    def is_running(self, node_id):
        """Return whether the specified node is running."""
        raise NotImplementedError

    def is_terminated(self, node_id):
        """Return whether the specified node is terminated."""
        raise NotImplementedError

    def node_tags(self, node_id):
        """Returns the tags of the given node (string dict)."""
        raise NotImplementedError

    def external_ip(self, node_id):
        """Returns the external ip of the given node."""
        raise NotImplementedError

    def internal_ip(self, node_id):
        """Returns the internal ip (Ray ip) of the given node."""
        raise NotImplementedError

    def get_node_id(self, ip_address, use_internal=False) -> str:
        """Returns the node_id given an IP address.

        Assumes ip-address is unique per node.

        Args:
            ip_address (str): Address of node.
            use_internal (bool): Whether the ip address is public or private.
        """
        def find_node_id():
            if use_internal:
                return self._internal_ip_cache.get(ip_address)
            else:
                return self._external_ip_cache.get(ip_address)

        if not find_node_id():
            all_nodes = self.non_terminated_nodes({})
            for node_id in all_nodes:
                self._external_ip_cache[self.external_ip(node_id)] = node_id
                self._internal_ip_cache[self.internal_ip(node_id)] = node_id

        if not find_node_id():
            if use_internal:
                known_msg = (
                    f"Known IP addresses: {list(self._internal_ip_cache)}")
            else:
                known_msg = (
                    f"Known IP addresses: {list(self._external_ip_cache)}")
            raise ValueError(f"ip {ip_address} not found. " + known_msg)

        return find_node_id()

    def create_node(self, node_config, tags, count):
        """Creates a number of nodes within the namespace."""
        raise NotImplementedError

    def set_node_tags(self, node_id, tags):
        """Sets the tag values (string dict) for the specified node."""
        raise NotImplementedError

    def terminate_node(self, node_id):
        """Terminates the specified node."""
        raise NotImplementedError

    def terminate_nodes(self, node_ids):
        """Terminates a set of nodes. May be overridden with a batch method."""
        for node_id in node_ids:
            logger.info("NodeProvider: "
                        "{}: Terminating node".format(node_id))
            self.terminate_node(node_id)

    def cleanup(self):
        """Clean-up when a Provider is no longer required."""
        pass

    @staticmethod
    def bootstrap_config(cluster_config):
        """Bootstraps the cluster config by adding env defaults if needed."""
        return cluster_config

    def get_command_runner(self,
                           log_prefix,
                           node_id,
                           auth_config,
                           cluster_name,
                           process_runner,
                           use_internal_ip,
                           docker_config=None) -> Any:
        """Returns the CommandRunner class used to perform SSH commands.

        Args:
        log_prefix(str): stores "NodeUpdater: {}: ".format(<node_id>). Used
            to print progress in the CommandRunner.
        node_id(str): the node ID.
        auth_config(dict): the authentication configs from the autoscaler
            yaml file.
        cluster_name(str): the name of the cluster.
        process_runner(module): the module to use to run the commands
            in the CommandRunner. E.g., subprocess.
        use_internal_ip(bool): whether the node_id belongs to an internal ip
            or external ip.
        docker_config(dict): If set, the docker information of the docker
            container that commands should be run on.
        """
        common_args = {
            "log_prefix": log_prefix,
            "node_id": node_id,
            "provider": self,
            "auth_config": auth_config,
            "cluster_name": cluster_name,
            "process_runner": process_runner,
            "use_internal_ip": use_internal_ip
        }
        if docker_config and docker_config["container_name"] != "":
            return DockerCommandRunner(docker_config, **common_args)
        else:
            return SSHCommandRunner(**common_args)

    def prepare_for_head_node(self, cluster_config):
        """Returns a new cluster config with custom configs for head node."""
        return cluster_config
