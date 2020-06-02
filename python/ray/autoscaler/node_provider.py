import importlib
import logging
import os
import yaml

from ray.autoscaler.updater import SSHCommandRunner

logger = logging.getLogger(__name__)


def import_aws():
    from ray.autoscaler.aws.config import bootstrap_aws
    from ray.autoscaler.aws.node_provider import AWSNodeProvider
    return bootstrap_aws, AWSNodeProvider


def import_gcp():
    from ray.autoscaler.gcp.config import bootstrap_gcp
    from ray.autoscaler.gcp.node_provider import GCPNodeProvider
    return bootstrap_gcp, GCPNodeProvider


def import_azure():
    from ray.autoscaler.azure.config import bootstrap_azure
    from ray.autoscaler.azure.node_provider import AzureNodeProvider
    return bootstrap_azure, AzureNodeProvider


def import_local():
    from ray.autoscaler.local.config import bootstrap_local
    from ray.autoscaler.local.node_provider import LocalNodeProvider
    return bootstrap_local, LocalNodeProvider


def import_kubernetes():
    from ray.autoscaler.kubernetes.config import bootstrap_kubernetes
    from ray.autoscaler.kubernetes.node_provider import KubernetesNodeProvider
    return bootstrap_kubernetes, KubernetesNodeProvider


def load_local_example_config():
    import ray.autoscaler.local as ray_local
    return os.path.join(
        os.path.dirname(ray_local.__file__), "example-full.yaml")


def load_kubernetes_example_config():
    import ray.autoscaler.kubernetes as ray_kubernetes
    return os.path.join(
        os.path.dirname(ray_kubernetes.__file__), "example-full.yaml")


def load_aws_example_config():
    import ray.autoscaler.aws as ray_aws
    return os.path.join(os.path.dirname(ray_aws.__file__), "example-full.yaml")


def load_gcp_example_config():
    import ray.autoscaler.gcp as ray_gcp
    return os.path.join(os.path.dirname(ray_gcp.__file__), "example-full.yaml")


def load_azure_example_config():
    import ray.autoscaler.azure as ray_azure
    return os.path.join(
        os.path.dirname(ray_azure.__file__), "example-full.yaml")


def import_external():
    """Mock a normal provider importer."""

    def return_it_back(config):
        return config

    return return_it_back, None


NODE_PROVIDERS = {
    "local": import_local,
    "aws": import_aws,
    "gcp": import_gcp,
    "azure": import_azure,
    "kubernetes": import_kubernetes,
    "docker": None,
    "external": import_external  # Import an external module
}

DEFAULT_CONFIGS = {
    "local": load_local_example_config,
    "aws": load_aws_example_config,
    "gcp": load_gcp_example_config,
    "azure": load_azure_example_config,
    "kubernetes": load_kubernetes_example_config,
    "docker": None,
}


def load_class(path):
    """
    Load a class at runtime given a full path.

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


def get_node_provider(provider_config, cluster_name):
    if provider_config["type"] == "external":
        provider_cls = load_class(path=provider_config["module"])
        return provider_cls(provider_config, cluster_name)

    importer = NODE_PROVIDERS.get(provider_config["type"])

    if importer is None:
        raise NotImplementedError("Unsupported node provider: {}".format(
            provider_config["type"]))
    _, provider_cls = importer()
    return provider_cls(provider_config, cluster_name)


def get_default_config(provider_config):
    if provider_config["type"] == "external":
        return {}
    load_config = DEFAULT_CONFIGS.get(provider_config["type"])
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

    def non_terminated_nodes(self, tag_filters):
        """Return a list of node ids filtered by the specified tags dict.

        This list must not include terminated nodes. For performance reasons,
        providers are allowed to cache the result of a call to nodes() to
        serve single-node queries (e.g. is_running(node_id)). This means that
        nodes() must be called again to refresh results.

        Examples:
            >>> provider.non_terminated_nodes({TAG_RAY_NODE_TYPE: "worker"})
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

    def get_command_runner(self, log_prefix, node_id, auth_config,
                           cluster_name, process_runner, use_internal_ip):
        """ Returns the CommandRunner class used to perform SSH commands.

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
        """

        return SSHCommandRunner(log_prefix, node_id, self, auth_config,
                                cluster_name, process_runner, use_internal_ip)
