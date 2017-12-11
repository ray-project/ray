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
    def __init__(self, provider_config, worker_group):
        self.provider_config = provider_config
        self.worker_group = worker_group

    def nodes(self, tag_filters):
        raise NotImplementedError

    def is_running(self, node_id):
        raise NotImplementedError

    def is_terminated(self, node_id):
        raise NotImplementedError

    def node_tags(self, node_id):
        raise NotImplementedError

    def external_ip(self, node_id):
        raise NotImplementedError

    def create_node(self, node_config, tags, count):
        raise NotImplementedError

    def set_node_tags(self, node_id, tags):
        raise NotImplementedError

    def terminate_node(self, node_id):
        raise NotImplementedError
