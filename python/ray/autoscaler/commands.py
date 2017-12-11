from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.autoscaler.node_provider import NODE_PROVIDERS


def bootstrap_cluster(config):
    importer = NODE_PROVIDERS.get(config["provider"]["type"])
    if not importer:
        raise NotImplementedError(
            "Unsupported provider {}".format(config["provider"]))
    bootstrap, _, _ = importer()
    bootstrap(config)


def teardown_cluster(config):
    importer = NODE_PROVIDERS.get(config["provider"]["type"])
    if not importer:
        raise NotImplementedError(
            "Unsupported provider {}".format(config["provider"]))
    _, teardown, _ = importer()
    teardown(config)
