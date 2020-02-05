import logging
from threading import RLock
from uuid import uuid4

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.compute import ComputeManagementClient
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME
from ray.autoscaler.azure.config import INSTANCE_NAME_MAX_LEN, INSTANCE_NAME_UUID_LEN

logger = logging.getLogger(__name__)


def tags_match(vm, tags):
    vm_tags = vm.get("tags", {})
    for k, v in tags.items():
        if vm_tags.get(k) != v:
            return False
    return True


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
        credentials = ServicePrincipalCredentials(client_id=provider_config['client_id'],
                                                  secret=provider_config['secret'],
                                                  tenant=provider_config['tenant'])
        self.client = ComputeManagementClient(credentials=credentials,
                                              subscription_id=provider_config['subscription_id'])
        self.lock = RLock()

        # cache of node objects from the last nodes() call to avoid repeating expensive queries
        self.cached_nodes = {}

    @staticmethod
    def synchronized(f):
        def wrapper(self, *args, **kwargs):
            self.lock.acquire()
            try:
                return f(self, *args, **kwargs)
            finally:
                self.lock.release()

        return wrapper

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
        vms = self.client.virtual_machines.list(
            resource_group_name=self.provider_config["resource_group"]
        ).result()

        # TODO: ensure terminated nodes are not shown here
        self.cached_nodes = {vm["name"]: vm.as_dict()
                             for vm in vms
                             if tags_match(vm=vm, tags=tag_filters)}
        return self.cached_nodes.keys()

    @synchronized
    def is_running(self, node_id):
        """Return whether the specified node is running."""
        return self._get_cached_node(node_id=node_id)["status"] == "RUNNING"

    @synchronized
    def is_terminated(self, node_id):
        """Return whether the specified node is terminated."""
        return self._get_cached_node(node_id=node_id)["status"] not in {"PROVISIONING", "STAGING", "RUNNING"}

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
        config["tags"].update(config_tags)

        config["location"] = self.provider_config["location"]
        ssh_config = {"os_profile":
                          {"linux_configuration":
                               {"disable_password_authentication": True,
                                "ssh": {
                                    {"public_keys": [
                                        {"key_data": self.provider_config['ssh_public_key_data'],
                                         "path": self.provider_config['ssh_public_key_path']}
                                    ]}
                                }}
                           }
                      }
        config.update(ssh_config)

        operations = [
            self.client.virtual_machines.create_or_update(
                resource_group_name=self.provider_config["resource_group"],
                vm_name=vm_name,
                parameters=config
            ) for _ in range(count)
        ]

        return [operation.result() for operation in operations]

    @synchronized
    def set_node_tags(self, node_id, tags):
        """Sets the tag values (string dict) for the specified node."""
        return self.client.virtual_machines.update(
            resource_group_name=self.provider_config["resource_group"],
            vm_name=node_id,
            parameters=dict(tags=tags)
        ).result()

    @synchronized
    def terminate_node(self, node_id):
        """Terminates the specified node."""
        return self.client.virtual_machines.delete(
            resource_group_name=self.provider_config["resource_group"],
            vm_name=node_id
        ).result()

    @synchronized
    def _get_node(self, node_id):
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        return self.client.virtual_machines.get(
            resource_group_name=self.provider_config["resource_group"],
            vm_name=node_id
        ).result()

    def _get_cached_node(self, node_id):
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        self.non_terminated_nodes({})  # Side effect: updates cache
        return self._get_node(node_id)
