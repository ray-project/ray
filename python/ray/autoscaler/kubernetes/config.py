from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

logger = logging.getLogger(__name__)

DEFAULT_NAMESPACE = "ray"


def bootstrap_kubernetes(config):
    config["use_internal_ips"] = True
    config = _configure_namespace(config)

    # TODO: make cluster role if not set

    print(config)
    return config


def _configure_namespace(config):
    if "namespace" not in config:
        config["namespace"] = DEFAULT_NAMESPACE
    return config
