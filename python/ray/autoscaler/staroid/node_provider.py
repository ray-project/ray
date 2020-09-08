import logging
import staroid

from ray.autoscaler.node_provider import NodeProvider

logger = logging.getLogger(__name__)

class StaroidNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)


    @staticmethod
    def bootstrap_config(cluster_config):
        """Bootstraps the cluster config by adding env defaults if needed."""
        return cluster_config
