import logging
from threading import RLock
from uuid import uuid4

from azure.common.client_factory import get_client_from_auth_file
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
#from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME
#from ray.autoscaler.azure.config import INSTANCE_NAME_MAX_LEN, INSTANCE_NAME_UUID_LEN

TAG_RAY_CLUSTER_NAME="ray-cluster"
TAG_RAY_NODE_NAME="raynode"
INSTANCE_NAME_MAX_LEN = 16
INSTANCE_NAME_UUID_LEN = 4
IP_CONFIG_NAME = 'ray-ip-config'
NIC_NAME = 'ray-nic'

# FIXME: reset this after testing
#from ray.autoscaler.node_provider import NodeProvider
class NodeProvider():
    def __init__(self, provider_config, cluster_name):
        self.provider_config = provider_config
        self.cluster_name = cluster_name
        pass


logger = logging.getLogger(__name__)


def synchronized(f):
    def wrapper(self, *args, **kwargs):
        self.lock.acquire()
        try:
            return f(self, *args, **kwargs)
        finally:
            self.lock.release()
    return wrapper

class AzureNodeProvider(NodeProvider):
    """
    Azure Node Provider
    Assumes credentials are set by running `az login`
    and default subscription is configured through `az account`

    Nodes may be in one of three states: {pending, running, terminated}. Nodes
    appear immediately once started by `create_node`, and transition
    immediately to terminated when `terminate_node` is called.
    """

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.compute_client = get_client_from_auth_file(ComputeManagementClient, auth_path=provider_config['auth_path'])
        self.network_client = get_client_from_auth_file(NetworkManagementClient, auth_path=provider_config'auth_path'])
        self.lock = RLock()

        # cache of node objects from the last nodes() call to avoid repeating expensive queries
        self.cached_nodes = {}

    @synchronized
    def non_terminated_nodes(self, tag_filters):
        """Return a list of node ids filtered by the specified tags dict.

        This list must not include terminated nodes. For performance reasons,
        providers are allowed to cache the result of a call to nodes() to
        serve single-node queries (e.g. is_running(node_id)). This means that
        nodes() must be called again to refresh results.

        Examples:
            >>> provider.non_terminated_nodes({TAG_RAY_NODE_TYPE: "worker"})
            ["node-1", "node-2"]
        """
        resource_group = self.provider_config['resource_group']
        vms = self.compute_client.virtual_machines.list(resource_group_name=resource_group)

        def match_tags(vm):
            for k, v in tag_filters.items():
                if vm.tags.get(k) != v:
                    return False
            return True

        def extract_metadata(vm):
            print('em')
            # get tags
            metadata['tags'] = vm.tags

            # get status
            instance = self.compute_client.virtual_machines.instance_view(
                resource_group_name=resource_group,
                vm_name=vm.name
            ).as_dict()
            print(f'instance: {instance}')
            for status in instance['statuses']:
                code, state = status['code'].split('/')
                # skip provisioning status
                if code == 'PowerState':
                    metdata['status'] = state
                    break
            
            # TODO: get ip data
            metadata['external_ip'] = 0
            metadata['internal_ip'] = 0
            return metadata
        
        self.all_nodes = {vm.name: extract_metadata(vm) for vm in filter(match_tags, vms)}

        # remove terminated nodes from list
        self.live_nodes = {k: v for k, v in self.all_nodes.items() if v['status'] != 'deallocated'}
        return list(self.cached_nodes.keys())

    @synchronized
    def is_running(self, node_id):
        """Return whether the specified node is running."""
        return self._get_cached_node(node_id=node_id)["status"] == "running"

    @synchronized
    def is_terminated(self, node_id):
        """Return whether the specified node is terminated."""
        return self._get_cached_node(node_id=node_id)["status"] == 'deallocated'

    @synchronized
    def node_tags(self, node_id):
        """Returns the tags of the given node (string dict)."""
        return self._get_cached_node(node_id=node_id)["tags"]

    @synchronized
    def external_ip(self, node_id):
        """Returns the external ip of the given node."""
        return self._get_cached_node(node_id=node_id)["external_ip"]

    @synchronized
    def internal_ip(self, node_id):
        """Returns the internal ip (Ray ip) of the given node."""
        return self._get_cached_node(node_id=node_id)["internal_ip"]

    @synchronized
    def create_node(self, node_config, tags, count):
        """Creates a number of nodes within the namespace."""
        config = node_config.copy()

        name_tag = tags[TAG_RAY_NODE_NAME]
        assert name_tag

        vm_name = "{name}_{id}".format(name=name_tag,
                                       id=uuid4().hex[:INSTANCE_NAME_UUID_LEN])
        assert len(vm_name) <= INSTANCE_NAME_MAX_LEN

        config_tags = config.get("tags", {})
        config_tags[TAG_RAY_CLUSTER_NAME] = self.cluster_name

        config["tags"] = config_tags
        config["location"] = self.provider_config["location"]

        # get public ip address
        public_ip_addess_params = {
            'location': config['location'],
            'public_ip_allocation_method': 'Dynamic'
        }
        public_ip_address = self.network_client.public_ip_addresses.create_or_update(
            resource_group_name=resource_group,
            public_ip_address_name='{}-ip'.format(vm_name),
            parameters=public_ip_addess_params
        ).result()

        nic_params = {
            'location': config['location'],
            'ip_configurations': [{
                'name': IP_CONFIG_NAME,
                'public_ip_address': public_ip_address,
                'subnet': {'id': self.provider_config['subnet_id']}
            }]
        }
        nic = network_client.network_interfaces.create_or_update(resource_group_name=resource_group,
                                                                 network_interface_name=NIC_NAME,
                                                                 parameters=nic_params).result()

        # update config with nested ssh config parameters
        config.update({
            'network_profile': {
                'network_interfaces': [{'id': nic.id}]
            },
            'os_profile': {
                'computer_name': TAG_RAY_NODE_NAME,
                'linux_configuration': {
                    'ssh': {
                        'public_keys': [{
                            'key_data': self.provider_config['ssh_public_key_data'],
                            'path': '/home/ubuntu/.ssh/authorized_keys'
                        }]
                    }
                }
            }
        })

        operations = [
            self.compute_client.virtual_machines.create_or_update(
                resource_group_name=self.provider_config["resource_group"],
                vm_name=vm_name,
                parameters=config
            ) for _ in range(count)
        ]

        #TODO: return something?

    @synchronized
    def set_node_tags(self, node_id, tags):
        """Sets the tag values (string dict) for the specified node."""
        return self.compute_client.virtual_machines.update(
            resource_group_name=self.provider_config["resource_group"],
            vm_name=node_id,
            parameters={'tags': tags}
        ).result()

    @synchronized
    def terminate_node(self, node_id):
        """Terminates the specified node."""
        return self.compute_client.virtual_machines.deallocate(
            resource_group_name=self.provider_config["resource_group"],
            vm_name=node_id
        )

    @synchronized
    def cleanup(self):
        """Delete all created VM resources"""
        self.non_terminated_nodes({})
        for node_id in self.all_nodes.keys():
            self.compute_client.virtual_machines.delete(
                resource_group_name=self.provider_config["resource_group"],
                vm_name=node_id
            )

    @synchronized
    def _get_node(self, node_id):
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]
        elif node_id in self.all_nodes:
            return self.all_nodes[node_id]

        return self.compute_client.virtual_machines.get(
            resource_group_name=self.provider_config["resource_group"],
            vm_name=node_id
        ).result()

    def _get_cached_node(self, node_id):
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        self.non_terminated_nodes({})  # Side effect: updates cache
        return self._get_node(node_id)
