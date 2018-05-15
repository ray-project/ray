from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from googleapiclient import discovery
compute = discovery.build('compute', 'v1')

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_NAME
from ray.autoscaler.gcp.config import wait_for_compute_zone_operation
from ray.ray_constants import BOTO_MAX_RETRIES

TERMINATED_STATES = (
    'STOPPING',
    'STOPPED',
    'SUSPENDING',
    'SUSPENDED',
    'TERMINATED',
)

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
        return node['status'] == 'RUNNING'

    def is_terminated(self, node_id):
        node = self._node(node_id)
        return node['status'] in TERMINATED_STATES

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

    def create_node(self, base_config, labels, count):
        # NOTE: gcp uses 'labels' instead of aws 'tags'
        # https://cloud.google.com/compute/docs/instances/create-start-instance#startinginstancwithimage
        project_id = self.provider_config['project_id']
        availability_zone = self.provider_config['availability_zone']

        config = base_config.copy()
        config['name'] = labels[TAG_NAME]
        config['machineType'] = (
            'zones/{zone}/machineTypes/{machine_type}'
            ''.format(zone=availability_zone,
                      machine_type=base_config['machineType']))

        config['labels'] = dict(
            config.get('labels', {}),
            **labels,
            **{TAG_RAY_CLUSTER_NAME: self.cluster_name})

        if count != 1: raise NotImplementedError(count)

        operation = compute.instances().insert(
            project=project_id,
            zone=availability_zone,
            body=config
        ).execute()

        result = wait_for_compute_zone_operation(
            project_id, operation, availability_zone)

        return result

    def terminate_node(self, node_id):
        node = self._node(node_id)
        raise NotImplementedError('GCPNodeProvider.terminate_node')
        # TODO.gcp: node.terminate

    def _node(self, node_id):
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        instance = compute.instances().get(
            project=self.provider_config['project_id'],
            zone=self.provider_config['availability_zone'],
            instance=node_id,
        ).execute()

        return instance
