import logging
import threading
from typing import Any, Dict

from ray.autoscaler._private.vsphere.cluster_operator_client import (
    ClusterOperatorClient,
)
from ray.autoscaler._private.vsphere.config import bootstrap_vsphere
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    STATUS_SETTING_UP,
    TAG_RAY_CLUSTER_NAME,
    TAG_RAY_NODE_NAME,
    TAG_RAY_NODE_STATUS,
)

logger = logging.getLogger(__name__)


class VsphereWcpNodeProvider(NodeProvider):
    max_terminate_nodes = 1000
    cluster_config = None

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.tag_cache = {}
        self.tag_cache_lock = threading.Lock()
        self.client = ClusterOperatorClient(
            cluster_name, provider_config, VsphereWcpNodeProvider.cluster_config
        )

    @staticmethod
    def bootstrap_config(cluster_config):
        config = bootstrap_vsphere(cluster_config)
        VsphereWcpNodeProvider.cluster_config = config
        return config

    def non_terminated_nodes(self, tag_filters):
        nodes, tag_cache = self.client.list_vms(tag_filters)
        with self.tag_cache_lock:
            for node_id in nodes:
                for k, v in tag_cache[node_id].items():
                    if node_id in self.tag_cache.keys():
                        self.tag_cache[node_id][k] = v
                    else:
                        self.tag_cache[node_id] = {}
                        self.tag_cache[node_id][k] = v
        logger.info(f"Non terminated nodes' tags are {self.tag_cache}")
        return nodes

    def is_running(self, node_id):
        return self.client.is_vm_power_on(node_id)

    def is_terminated(self, node_id):
        if self.client.is_vm_power_on(node_id):
            return False
        else:
            # If the node is not powered on but has the creating tag, then it could
            # be under reconfiguration, such as plugging the GPU. In this case we
            # should consider the node is not terminated, it will be turned on later
            return not self.client.is_vm_creating(node_id)

    def node_tags(self, node_id):
        with self.tag_cache_lock:
            return self.tag_cache[node_id]

    def external_ip(self, node_id):
        return self.client.get_vm_external_ip(node_id)

    def internal_ip(self, node_id):
        # Currently vSphere VMs do not show an internal IP. So we just return the
        # external IP
        return self.client.get_vm_external_ip(node_id)

    def set_node_tags(self, node_id, tags):
        # This method gets called from the Ray and it passes
        # node_id. It updates old tags (if present) with new values.
        with self.tag_cache_lock:
            for k, v in tags.items():
                # update tags for node_id
                self.tag_cache[node_id][k] = v
            logger.info(f"Updated tags for {node_id} to: {self.tag_cache[node_id]}")

    def create_node(self, node_config, tags, count) -> Dict[str, Any]:
        """Creates instances.

        Returns dict mapping instance id to VM object for the created
        instances.
        """
        to_be_launched_node_count = count
        created_nodes_dict = {}
        if to_be_launched_node_count > 0:
            created_nodes_dict = self.client.create_nodes(
                tags, to_be_launched_node_count, node_config
            )
        # make sure to mark newly created nodes as ready
        # so autoscaler shouldn't provision new ones
        with self.tag_cache_lock:
            for node_id in created_nodes_dict.keys():
                self.tag_cache[node_id] = tags.copy()
                self.tag_cache[node_id][TAG_RAY_NODE_STATUS] = STATUS_SETTING_UP
                self.tag_cache[node_id][TAG_RAY_NODE_NAME] = node_id
                self.tag_cache[node_id][TAG_RAY_CLUSTER_NAME] = self.cluster_name
                logger.info(
                    f"Node {node_id} created with tags: {self.tag_cache[node_id]}"
                )
        return created_nodes_dict

    def terminate_node(self, node_id):
        if not node_id or self.client.is_vm_creating(node_id):
            return
        # Delete node iff it is either in a running or a failure state
        self.client.delete_node(node_id)
        with self.tag_cache_lock:
            if node_id in self.tag_cache:
                self.tag_cache.pop(node_id)

    def terminate_nodes(self, node_ids):
        if not node_ids:
            return
        for node_id in node_ids:
            self.terminate_node(node_id)

    def safe_to_scale(self) -> bool:
        return self.client.safe_to_scale()
