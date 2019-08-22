from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

logger = logging.getLogger(__name__)

DEFAULT_NAMESPACE = "ray"


def bootstrap_kubernetes(config):
    config["provider"]["use_internal_ips"] = True
    config = _configure_namespace(config)

    # TODO: make cluster role if not set

    return config


def _configure_namespace(config):
    if "namespace" not in config["provider"]:
        config["provider"]["namespace"] = DEFAULT_NAMESPACE
    return config
