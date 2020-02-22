import logging
from threading import RLock
from uuid import uuid4

from azure.common.client_factory import get_client_from_cli_profile
from msrestazure.azure_active_directory import MSIAuthentication
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.compute.models import ResourceIdentityType

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME

INSTANCE_NAME_MAX_LEN = 64
INSTANCE_NAME_UUID_LEN = 4

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
        subscription_id = provider_config["subscription_id"]
        try:
            self.compute_client = get_client_from_cli_profile(
                client_class=ComputeManagementClient,
                subscription_id=subscription_id)
            self.network_client = get_client_from_cli_profile(
                client_class=NetworkManagementClient,
                subscription_id=subscription_id)
        except Exception:
            logger.info(
                "CLI profile authentication failed. Trying MSI", exc_info=True)

            credentials = MSIAuthentication()
            self.compute_client = ComputeManagementClient(
                credentials=credentials, subscription_id=subscription_id)
            self.network_client = NetworkManagementClient(
                credentials=credentials, subscription_id=subscription_id)

        self.lock = RLock()

        # cache node objects
        self.cached_nodes = {}

    def _get_filtered_nodes(self, tag_filters):
        def match_tags(vm):
            for k, v in tag_filters.items():
                if vm.tags.get(k) != v:
                    return False
            return True

        vms = self.compute_client.virtual_machines.list(
            resource_group_name=self.provider_config["resource_group"])

        nodes = [self._extract_metadata(vm) for vm in filter(match_tags, vms)]
        self.cached_nodes = {vm["name"]: vm for vm in nodes}
        return self.cached_nodes

    def _extract_metadata(self, vm):
        # get tags
        metadata = {"name": vm.name, "tags": vm.tags}

        # get status
        resource_group = self.provider_config["resource_group"]
        instance = self.compute_client.virtual_machines.instance_view(
            resource_group_name=resource_group, vm_name=vm.name).as_dict()
        for status in instance["statuses"]:
            code, state = status["code"].split("/")
            # skip provisioning status
            if code == "PowerState":
                metadata["status"] = state
                break

        # get ip data
        nic_id = vm.network_profile.network_interfaces[0].id
        metadata["nic_name"] = nic_id.split("/")[-1]
        nic = self.network_client.network_interfaces.get(
            resource_group_name=resource_group,
            network_interface_name=metadata["nic_name"])
        ip_config = nic.ip_configurations[0]
        public_ip_id = ip_config.public_ip_address.id
        metadata["public_ip_name"] = public_ip_id.split("/")[-1]
        public_ip = self.network_client.public_ip_addresses.get(
            resource_group_name=resource_group,
            public_ip_address_name=metadata["public_ip_name"])
        metadata["external_ip"] = public_ip.ip_address
        metadata["internal_ip"] = ip_config.private_ip_address

        return metadata

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
        nodes = self._get_filtered_nodes(tag_filters=tag_filters)
        return [
            k for k, v in nodes.items()
            if not v.get("status", "deallocat").startswith("deallocat")
        ]

    @synchronized
    def is_running(self, node_id):
        """Return whether the specified node is running."""
        return self._get_cached_node(node_id=node_id)["status"] == "running"

    @synchronized
    def is_terminated(self, node_id):
        """Return whether the specified node is terminated."""
        return self._get_cached_node(node_id=node_id)["status"] in [
            "deallocating", "deallocated"
        ]

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
        # TODO: restart deallocated nodes if possible
        location = self.provider_config["location"]
        resource_group = self.provider_config["resource_group"]
        subnet_id = self.provider_config["subnet_id"]

        config = node_config.copy()
        config_tags = config.get("tags", {})
        config_tags.update(tags)
        config_tags[TAG_RAY_CLUSTER_NAME] = self.cluster_name

        config["tags"] = config_tags
        config["location"] = location
        name_tag = config_tags.get(TAG_RAY_NODE_NAME, "node")

        for _ in range(count):
            unique_id = uuid4().hex[:INSTANCE_NAME_UUID_LEN]
            vm_name = "{name}-{id}".format(name=name_tag, id=unique_id)

            try:
                assert len(vm_name) <= INSTANCE_NAME_MAX_LEN
            except AssertionError as e:
                e.args += ("name", vm_name)
                raise

            # get public ip address
            public_ip_addess_params = {
                "location": location,
                "public_ip_allocation_method": "Dynamic"
            }
            public_ip_address = (
                self.network_client.public_ip_addresses.create_or_update(
                    resource_group_name=resource_group,
                    public_ip_address_name="{}-ip".format(vm_name),
                    parameters=public_ip_addess_params).result())

            nic_params = {
                "location": location,
                "ip_configurations": [{
                    "name": uuid4().hex,
                    "public_ip_address": public_ip_address,
                    "subnet": {
                        "id": subnet_id
                    }
                }]
            }
            nic = self.network_client.network_interfaces.create_or_update(
                resource_group_name=resource_group,
                network_interface_name="{}-nic".format(vm_name),
                parameters=nic_params).result()

            # update vm config with network parameters
            config.update({
                "network_profile": {
                    "network_interfaces": [{
                        "id": nic.id
                    }]
                }
            })

            config["identity"] = {
                "type": ResourceIdentityType.user_assigned,
                "user_assigned_identities": [{
                    # zero-documentation.. *sigh*
                    "key": self.provider_config["msi_identity_id"],
                    "value": {
                        "principal_id": self.provider_config[
                            "msi_identity_principal_id"],
                        "client_id": self.provider_config["msi_identity_id"]
                    }
                }]
            }

            # TODO: do we need to wait or fire and forget is fine?
            self.compute_client.virtual_machines.create_or_update(
                resource_group_name=self.provider_config["resource_group"],
                vm_name=vm_name,
                parameters=config)

    @synchronized
    def set_node_tags(self, node_id, tags):
        """Sets the tag values (string dict) for the specified node."""
        node_tags = self._get_cached_node(node_id)["tags"]
        node_tags.update(tags)
        self.cached_nodes[node_id]["tags"] = node_tags
        self.compute_client.virtual_machines.update(
            resource_group_name=self.provider_config["resource_group"],
            vm_name=node_id,
            parameters={"tags": node_tags})

    @synchronized
    def terminate_node(self, node_id):
        """Terminates the specified node."""
        # self.compute_client.virtual_machines.deallocate(
        # resource_group_name=self.provider_config["resource_group"],
        # vm_name=node_id)
        resource_group = self.provider_config["resource_group"]
        nodes = self._get_filtered_nodes(
            tag_filters={TAG_RAY_CLUSTER_NAME: self.cluster_name})
        for node, metadata in nodes.items():
            # gather disks to delete later
            vm = self.compute_client.virtual_machines.get(
                resource_group_name=resource_group, vm_name=node)
            disks = vm.storage_profile.data_disks
            disks.append(vm.storage_profile.os_disk)
            # delete machine
            self.compute_client.virtual_machines.delete(
                resource_group_name=resource_group, vm_name=node).wait()
            # delete nic
            self.network_client.network_interfaces.delete(
                resource_group_name=resource_group,
                network_interface_name=metadata["nic_name"])
            # delete ip address
            self.network_client.public_ip_addresses.delete(
                resource_group_name=resource_group,
                public_ip_address_name=metadata["public_ip_name"])
            # delete disks
            for disk in disks:
                self.compute_client.disks.delete(
                    resource_group_name=resource_group, disk_name=disk.name)

    @synchronized
    def _get_node(self, node_id):
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        vm = self.compute_client.virtual_machines.get(
            resource_group_name=self.provider_config["resource_group"],
            vm_name=node_id)
        return self._extract_metadata(vm)

    def _get_cached_node(self, node_id):
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        self._get_filtered_nodes({})  # Side effect: updates cache
        return self._get_node(node_id)
