import json
import logging
import os
from threading import RLock
from uuid import uuid4

from azure.common.client_factory import get_client_from_cli_profile
from msrestazure.azure_active_directory import MSIAuthentication
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode
from knack.util import CLIError

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME

VM_NAME_MAX_LEN = 64
VM_NAME_UUID_LEN = 8

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
    """Node Provider for Azure

    This provider assumes Azure credentials are set by running ``az login``
    and the default subscription is configured through ``az account``
    or set in the ``provider`` field of the autoscaler configuration.

    Nodes may be in one of three states: {pending, running, terminated}. Nodes
    appear immediately once started by ``create_node``, and transition
    immediately to terminated when ``terminate_node`` is called.
    """

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        kwargs = {}
        if "subscription_id" in provider_config:
            kwargs["subscription_id"] = provider_config["subscription_id"]
        try:
            self.compute_client = get_client_from_cli_profile(
                client_class=ComputeManagementClient, **kwargs)
            self.network_client = get_client_from_cli_profile(
                client_class=NetworkManagementClient, **kwargs)
            self.resource_client = get_client_from_cli_profile(
                client_class=ResourceManagementClient, **kwargs)
        except CLIError as e:
            if str(e) != "Please run 'az login' to setup account.":
                raise
            else:
                logger.info("CLI profile authentication failed. Trying MSI")

                credentials = MSIAuthentication()
                self.compute_client = ComputeManagementClient(
                    credentials=credentials, **kwargs)
                self.network_client = NetworkManagementClient(
                    credentials=credentials, **kwargs)
                self.resource_client = ResourceManagementClient(
                    credentials=credentials, **kwargs)

        self.lock = RLock()

        # cache node objects
        self.cached_nodes = {}

    @synchronized
    def _get_filtered_nodes(self, tag_filters):
        def match_tags(vm):
            for k, v in tag_filters.items():
                if vm.tags.get(k) != v:
                    return False
            return True

        vms = self.compute_client.virtual_machines.list(
            resource_group_name=self.provider_config["resource_group"])

        nodes = [self._extract_metadata(vm) for vm in filter(match_tags, vms)]
        self.cached_nodes = {node["name"]: node for node in nodes}
        return self.cached_nodes

    def _extract_metadata(self, vm):
        # get tags
        metadata = {"name": vm.name, "tags": vm.tags, "status": ""}

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

        if not self.provider_config.get("use_internal_ips", False):
            public_ip_id = ip_config.public_ip_address.id
            metadata["public_ip_name"] = public_ip_id.split("/")[-1]
            public_ip = self.network_client.public_ip_addresses.get(
                resource_group_name=resource_group,
                public_ip_address_name=metadata["public_ip_name"])
            metadata["external_ip"] = public_ip.ip_address

        metadata["internal_ip"] = ip_config.private_ip_address

        return metadata

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
            if not v["status"].startswith("deallocat")
        ]

    def is_running(self, node_id):
        """Return whether the specified node is running."""
        # always get current status
        node = self._get_node(node_id=node_id)
        return node["status"] == "running"

    def is_terminated(self, node_id):
        """Return whether the specified node is terminated."""
        # always get current status
        node = self._get_node(node_id=node_id)
        return node["status"].startswith("deallocat")

    def node_tags(self, node_id):
        """Returns the tags of the given node (string dict)."""
        return self._get_cached_node(node_id=node_id)["tags"]

    def external_ip(self, node_id):
        """Returns the external ip of the given node."""
        ip = (self._get_cached_node(node_id=node_id)["external_ip"]
              or self._get_node(node_id=node_id)["external_ip"])
        return ip

    def internal_ip(self, node_id):
        """Returns the internal ip (Ray ip) of the given node."""
        ip = (self._get_cached_node(node_id=node_id)["internal_ip"]
              or self._get_node(node_id=node_id)["internal_ip"])
        return ip

    def create_node(self, node_config, tags, count):
        """Creates a number of nodes within the namespace."""
        # TODO: restart deallocated nodes if possible
        resource_group = self.provider_config["resource_group"]

        # load the template file
        current_path = os.path.dirname(os.path.abspath(__file__))
        template_path = os.path.join(current_path, "azure-vm-template.json")
        with open(template_path, "r") as template_fp:
            template = json.load(template_fp)

        # get the tags
        config_tags = node_config.get("tags", {}).copy()
        config_tags.update(tags)
        config_tags[TAG_RAY_CLUSTER_NAME] = self.cluster_name

        name_tag = config_tags.get(TAG_RAY_NODE_NAME, "node")
        unique_id = uuid4().hex[:VM_NAME_UUID_LEN]
        vm_name = "{name}-{id}".format(name=name_tag, id=unique_id)
        use_internal_ips = self.provider_config.get("use_internal_ips", False)

        template_params = node_config["azure_arm_parameters"].copy()
        template_params["vmName"] = vm_name
        template_params["provisionPublicIp"] = not use_internal_ips
        template_params["vmTags"] = config_tags
        template_params["vmCount"] = count

        parameters = {
            "properties": {
                "mode": DeploymentMode.incremental,
                "template": template,
                "parameters": {
                    key: {
                        "value": value
                    }
                    for key, value in template_params.items()
                }
            }
        }

        # TODO: we could get the private/public ips back directly
        self.resource_client.deployments.create_or_update(
            resource_group_name=resource_group,
            deployment_name="ray-vm-{}".format(name_tag),
            parameters=parameters).wait()

    @synchronized
    def set_node_tags(self, node_id, tags):
        """Sets the tag values (string dict) for the specified node."""
        node_tags = self._get_cached_node(node_id)["tags"]
        node_tags.update(tags)
        self.compute_client.virtual_machines.update(
            resource_group_name=self.provider_config["resource_group"],
            vm_name=node_id,
            parameters={"tags": node_tags})
        self.cached_nodes[node_id]["tags"] = node_tags

    def terminate_node(self, node_id):
        """Terminates the specified node. This will delete the VM and
           associated resources (NIC, IP, Storage) for the specified node."""

        resource_group = self.provider_config["resource_group"]
        try:
            # get metadata for node
            metadata = self._get_node(node_id)
        except KeyError:
            # node no longer exists
            return

        # TODO: deallocate instead of delete to allow possible reuse
        # self.compute_client.virtual_machines.deallocate(
        #   resource_group_name=resource_group,
        #   vm_name=node_id)

        # gather disks to delete later
        vm = self.compute_client.virtual_machines.get(
            resource_group_name=resource_group, vm_name=node_id)
        disks = {d.name for d in vm.storage_profile.data_disks}
        disks.add(vm.storage_profile.os_disk.name)

        try:
            # delete machine, must wait for this to complete
            self.compute_client.virtual_machines.delete(
                resource_group_name=resource_group, vm_name=node_id).wait()
        except Exception as e:
            logger.warning("Failed to delete VM: {}".format(e))

        try:
            # delete nic
            self.network_client.network_interfaces.delete(
                resource_group_name=resource_group,
                network_interface_name=metadata["nic_name"])
        except Exception as e:
            logger.warning("Failed to delete nic: {}".format(e))

        # delete ip address
        if "public_ip_name" in metadata:
            try:
                self.network_client.public_ip_addresses.delete(
                    resource_group_name=resource_group,
                    public_ip_address_name=metadata["public_ip_name"])
            except Exception as e:
                logger.warning("Failed to delete public ip: {}".format(e))

        # delete disks
        for disk in disks:
            try:
                self.compute_client.disks.delete(
                    resource_group_name=resource_group, disk_name=disk)
            except Exception as e:
                logger.warning("Failed to delete disk: {}".format(e))

    def _get_node(self, node_id):
        self._get_filtered_nodes({})  # Side effect: updates cache
        return self.cached_nodes[node_id]

    def _get_cached_node(self, node_id):
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]
        return self._get_node(node_id=node_id)
