from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


def import_aws():
    from ray.autoscaler.aws.commands import bootstrap_aws, teardown_aws
    from ray.autoscaler.aws.node_provider import AWSNodeProvider
    return bootstrap_aws, teardown_aws, AWSNodeProvider


NODE_PROVIDERS = {
    "aws": import_aws,
    "gce": None,  # TODO: support more node providers
    "azure": None,
    "kubernetes": None,
    "docker": None,
    "local_cluster": None,
}


def get_node_provider(provider_config, worker_group):
    importer = NODE_PROVIDERS.get(provider_config["type"])
    if importer is None:
        raise NotImplementedError(
            "Unsupported node provider: {}".format(provider_config["type"]))
    _, _, provider_cls = importer()
    return provider_cls(provider_config, worker_group)


class NodeProvider(object):
    """Interface for getting and returning nodes from a Cloud.

    NodeProviders are namespaced by the `worker_group` parameter; they only
    operate on nodes within that namespace.

    Nodes may be in one of three states: {pending, running, terminated}. Nodes
    appear immediately once started by `create_node`, and transition
    immediately to terminated when `terminate_node` is called.
    """

    def __init__(self, provider_config, worker_group):
        self.provider_config = provider_config
        self.worker_group = worker_group

    def nodes(self, tag_filters):
        """Return a list of nodes filtered by the specified tags."""
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

    def create_node(self, node_config, tags, count):
        """Creates a number of nodes within the namespace."""
        raise NotImplementedError

    def set_node_tags(self, node_id, tags):
        """Sets the tag values (string dict) for the specified node."""
        raise NotImplementedError

    def terminate_node(self, node_id):
        """Terminates the specified node."""
        raise NotImplementedError
