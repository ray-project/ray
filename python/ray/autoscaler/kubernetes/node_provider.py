from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import importlib
import logging
import os
import yaml

from kubernetes import client, config


from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (TAG_RAY_LAUNCH_CONFIG, TAG_RAY_RUNTIME_CONFIG,
                                 TAG_RAY_NODE_STATUS, TAG_RAY_NODE_TYPE,
                                 TAG_RAY_NODE_NAME)

hard_coded_cluster_name = "sample-cluster"
hard_coded_namespace = "default"
logger = logging.getLogger(__name__)

class KubernetesNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        # TODO(gaocegege): Support custom kubeconfig in yaml.
        self.kubernetes_core_v1_client = client.CoreV1Api(
            config.new_client_from_config())

    @staticmethod
    def to_kubernetes_format(tag_filters):
        label_selector = ""
        for k, v in tag_filters.items():
            if k == TAG_RAY_NODE_TYPE:
                label_selector += "ray-{}=".format(v) + "{}-{}".format(hard_coded_cluster_name, v)
        return label_selector

    def non_terminated_nodes(self, tag_filters):
        label_selector = self.to_kubernetes_format(tag_filters)

        nodes = self.kubernetes_core_v1_client.list_namespaced_pod(hard_coded_namespace,label_selector=label_selector)
        logger.info("nodes: {}".format(nodes))
        return [node.id for node in nodes.items]
