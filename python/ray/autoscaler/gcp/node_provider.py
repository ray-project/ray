from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from googleapiclient import discovery
compute = discovery.build('compute', 'v1')

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME
from ray.ray_constants import BOTO_MAX_RETRIES


class GCPNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        config = Config(retries=dict(max_attempts=BOTO_MAX_RETRIES))

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_nodes = {}

        # Cache of ip lookups. We assume IPs never change once assigned.
        self.internal_ip_cache = {}
        self.external_ip_cache = {}

    def nodes(self, label_filters):
        # TODO: Add filters
        filter_expr = ''

        response = compute.instances().list(
            project=self.provider_config['project_id'],
            zone=self.provider_config['availability_zone'],
            filter=filter_expr,
        ).execute()

        instances = response.get('items', [])
        # Note: All the operations use 'name' as the unique instance identifier
        self.cached_nodes = {i['name']: i for i in instances}

        return [i['name'] for i in instances]

    def is_running(self, node_id):
        node = self._node(node_id)
        raise NotImplementedError('GCPNodeProvider.is_terminated')
        # TODO.gcp: return node.state == running
        return False

    def is_terminated(self, node_id):
        node = self._node(node_id)
        raise NotImplementedError('GCPNodeProvider.is_terminated')
        # TODO.gcp: state = node...
        return state not in ["running", "pending"]

    def node_tags(self, node_id):
        node = self._node(node_id)
        raise NotImplementedError('GCPNodeProvider.node_tags')
        # TODO.gcp: tags = node...
        return tags

    def external_ip(self, node_id):
        if node_id in self.external_ip_cache:
            return self.external_ip_cache[node_id]
        node = self._node(node_id)
        raise NotImplementedError('GCPNodeProvider.external_ip')
        # TODO.gcp: ip = node.public_ip_address
        if ip:
            self.external_ip_cache[node_id] = ip
        return ip

    def internal_ip(self, node_id):
        if node_id in self.internal_ip_cache:
            return self.internal_ip_cache[node_id]
        node = self._node(node_id)
        raise NotImplementedError('GCPNodeProvider.internal_ip')
        # TODO.gcp: ip = node.private_ip_address
        if ip:
            self.internal_ip_cache[node_id] = ip
        return ip

    def set_node_tags(self, node_id, tags):
        node = self._node(node_id)
        raise NotImplementedError('GCPNodeProvider.set_node_tags')
        # TODO.gcp: node.set_tags(tags)

    def create_node(self, node_config, tags, count):
        conf = node_config.copy()
        raise NotImplementedError('GCPNodeProvider.create_node')
        # TODO.gcp: create nodes with tags

    def terminate_node(self, node_id):
        node = self._node(node_id)
        raise NotImplementedError('GCPNodeProvider.terminate_node')
        # TODO.gcp: node.terminate

    def _node(self, node_id):
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]
        raise NotImplementedError('GCPNodeProvider.terminate_node')
        # TODO.gcp: instance = ...
        return None
