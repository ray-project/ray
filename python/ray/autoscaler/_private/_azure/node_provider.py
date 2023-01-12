import json
import logging
from pathlib import Path
from threading import RLock
from uuid import uuid4

from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode

from ray.autoscaler._private._azure.config import (
    bootstrap_azure,
    get_azure_sdk_function,
)
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    TAG_RAY_CLUSTER_NAME,
    TAG_RAY_LAUNCH_CONFIG,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_USER_NODE_TYPE,
)

VM_NAME_MAX_LEN = 64

logger = logging.getLogger(__name__)
azure_logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
azure_logger.setLevel(logging.WARNING)


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
        subscription_id = provider_config["subscription_id"]
        self.cache_stopped_nodes = provider_config.get("cache_stopped_nodes", True)
        credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
        self.compute_client = ComputeManagementClient(credential, subscription_id)
        self.network_client = NetworkManagementClient(credential, subscription_id)
        self.resource_client = ResourceManagementClient(credential, subscription_id)

        self.lock = RLock()

        # cache node objects
        self.cached_nodes = {}

    @synchronized
    def _get_filtered_nodes(self, tag_filters):
        # add cluster name filter to only get nodes from this cluster
        cluster_tag_filters = {**tag_filters, TAG_RAY_CLUSTER_NAME: self.cluster_name}

        def match_tags(vm):
            for k, v in cluster_tag_filters.items():
                if vm.tags.get(k) != v:
                    return False
            return True

        vms = self.compute_client.virtual_machines.list(
            resource_group_name=self.provider_config["resource_group"]
        )

        nodes = [self._extract_metadata(vm) for vm in filter(match_tags, vms)]
        self.cached_nodes = {node["name"]: node for node in nodes}
        return self.cached_nodes

    def _extract_metadata(self, vm):
        # get tags
        metadata = {"name": vm.name, "tags": vm.tags, "status": ""}

        # get status
        resource_group = self.provider_config["resource_group"]
        instance = self.compute_client.virtual_machines.instance_view(
            resource_group_name=resource_group, vm_name=vm.name
        ).as_dict()
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
            network_interface_name=metadata["nic_name"],
        )
        ip_config = nic.ip_configurations[0]

        if not self.provider_config.get("use_internal_ips", False):
            public_ip_id = ip_config.public_ip_address.id
            metadata["public_ip_name"] = public_ip_id.split("/")[-1]
            public_ip = self.network_client.public_ip_addresses.get(
                resource_group_name=resource_group,
                public_ip_address_name=metadata["public_ip_name"],
            )
            metadata["external_ip"] = public_ip.ip_address

        metadata["internal_ip"] = ip_config.private_ip_address

        return metadata

    def stopped_nodes(self, tag_filters):
        """Return a list of stopped node ids filtered by the specified tags dict."""
        nodes = self._get_filtered_nodes(tag_filters=tag_filters)
        return [k for k, v in nodes.items() if v["status"].startswith("deallocat")]

    def non_terminated_nodes(self, tag_filters):
        """Return a list of node ids filtered by the specified tags dict.

        This list must not include terminated nodes. For performance reasons,
        providers are allowed to cache the result of a call to nodes() to
        serve single-node queries (e.g. is_running(node_id)). This means that
        nodes() must be called again to refresh results.

        Examples:
            >>> from ray.autoscaler.tags import TAG_RAY_NODE_KIND
            >>> provider = ... # doctest: +SKIP
            >>> provider.non_terminated_nodes( # doctest: +SKIP
            ...     {TAG_RAY_NODE_KIND: "worker"})
            ["node-1", "node-2"]
        """
        nodes = self._get_filtered_nodes(tag_filters=tag_filters)
        return [k for k, v in nodes.items() if not v["status"].startswith("deallocat")]

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
        ip = (
            self._get_cached_node(node_id=node_id)["external_ip"]
            or self._get_node(node_id=node_id)["external_ip"]
        )
        return ip

    def internal_ip(self, node_id):
        """Returns the internal ip (Ray ip) of the given node."""
        ip = (
            self._get_cached_node(node_id=node_id)["internal_ip"]
            or self._get_node(node_id=node_id)["internal_ip"]
        )
        return ip

    def create_node(self, node_config, tags, count):
        resource_group = self.provider_config["resource_group"]

        if self.cache_stopped_nodes:
            VALIDITY_TAGS = [
                TAG_RAY_CLUSTER_NAME,
                TAG_RAY_NODE_KIND,
                TAG_RAY_LAUNCH_CONFIG,
                TAG_RAY_USER_NODE_TYPE,
            ]
            filters = {tag: tags[tag] for tag in VALIDITY_TAGS if tag in tags}
            reuse_nodes = self.stopped_nodes(filters)[:count]
            logger.info(
                f"Reusing nodes {list(reuse_nodes)}. "
                "To disable reuse, set `cache_stopped_nodes: False` "
                "under `provider` in the cluster configuration.",
            )
            start = get_azure_sdk_function(
                client=self.compute_client.virtual_machines, function_name="start"
            )
            for node_id in reuse_nodes:
                start(resource_group_name=resource_group, vm_name=node_id).wait()
                self.set_node_tags(node_id, tags)
            count -= len(reuse_nodes)

        if count:
            self._create_node(node_config, tags, count)

    def _create_node(self, node_config, tags, count):
        """Creates a number of nodes within the namespace."""
        resource_group = self.provider_config["resource_group"]

        # load the template file
        current_path = Path(__file__).parent
        template_path = current_path.joinpath("azure-vm-template.json")
        with open(template_path, "r") as template_fp:
            template = json.load(template_fp)

        # get the tags
        config_tags = node_config.get("tags", {}).copy()
        config_tags.update(tags)
        config_tags[TAG_RAY_CLUSTER_NAME] = self.cluster_name

        name_tag = config_tags.get(TAG_RAY_NODE_NAME, "node")
        vm_name = "{name}-{id}".format(name=name_tag, id=uuid4().hex)[:VM_NAME_MAX_LEN]
        use_internal_ips = self.provider_config.get("use_internal_ips", False)

        template_params = node_config["azure_arm_parameters"].copy()
        template_params["vmName"] = vm_name
        template_params["provisionPublicIp"] = not use_internal_ips
        template_params["vmTags"] = config_tags
        template_params["vmCount"] = count
        template_params["msi"] = self.provider_config["msi"]
        template_params["nsg"] = self.provider_config["nsg"]
        template_params["subnet"] = self.provider_config["subnet"]

        parameters = {
            "properties": {
                "mode": DeploymentMode.incremental,
                "template": template,
                "parameters": {
                    key: {"value": value} for key, value in template_params.items()
                },
            }
        }

        # TODO: we could get the private/public ips back directly
        create_or_update = get_azure_sdk_function(
            client=self.resource_client.deployments, function_name="create_or_update"
        )
        create_or_update(
            resource_group_name=resource_group,
            deployment_name="ray-vm-{}".format(name_tag),
            parameters=parameters,
        ).wait()

    @synchronized
    def set_node_tags(self, node_id, tags):
        """Sets the tag values (string dict) for the specified node."""
        node_tags = self._get_cached_node(node_id)["tags"]
        node_tags.update(tags)
        update = get_azure_sdk_function(
            client=self.compute_client.virtual_machines, function_name="update"
        )
        update(
            resource_group_name=self.provider_config["resource_group"],
            vm_name=node_id,
            parameters={"tags": node_tags},
        )
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

        if self.cache_stopped_nodes:
            try:
                # stop machine and leave all resources
                logger.info(
                    f"Stopping instance {node_id}"
                    "(to fully terminate instead, "
                    "set `cache_stopped_nodes: False` "
                    "under `provider` in the cluster configuration)"
                )
                stop = get_azure_sdk_function(
                    client=self.compute_client.virtual_machines,
                    function_name="deallocate",
                )
                stop(resource_group_name=resource_group, vm_name=node_id)
            except Exception as e:
                logger.warning("Failed to stop VM: {}".format(e))
        else:
            vm = self.compute_client.virtual_machines.get(
                resource_group_name=resource_group, vm_name=node_id
            )
            disks = {d.name for d in vm.storage_profile.data_disks}
            disks.add(vm.storage_profile.os_disk.name)

            try:
                # delete machine, must wait for this to complete
                delete = get_azure_sdk_function(
                    client=self.compute_client.virtual_machines, function_name="delete"
                )
                delete(resource_group_name=resource_group, vm_name=node_id)
            except Exception as e:
                logger.warning("Failed to delete VM: {}".format(e))

            try:
                # delete nic
                delete = get_azure_sdk_function(
                    client=self.network_client.network_interfaces,
                    function_name="delete",
                )
                delete(
                    resource_group_name=resource_group,
                    network_interface_name=metadata["nic_name"],
                )
            except Exception as e:
                logger.warning("Failed to delete nic: {}".format(e))

            # delete ip address
            if "public_ip_name" in metadata:
                try:
                    delete = get_azure_sdk_function(
                        client=self.network_client.public_ip_addresses,
                        function_name="delete",
                    )
                    delete(
                        resource_group_name=resource_group,
                        public_ip_address_name=metadata["public_ip_name"],
                    )
                except Exception as e:
                    logger.warning("Failed to delete public ip: {}".format(e))

            # delete disks
            for disk in disks:
                try:
                    delete = get_azure_sdk_function(
                        client=self.compute_client.disks, function_name="delete"
                    )
                    delete(resource_group_name=resource_group, disk_name=disk)
                except Exception as e:
                    logger.warning("Failed to delete disk: {}".format(e))

    def _get_node(self, node_id):
        self._get_filtered_nodes({})  # Side effect: updates cache
        return self.cached_nodes[node_id]

    def _get_cached_node(self, node_id):
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]
        return self._get_node(node_id=node_id)

    @staticmethod
    def bootstrap_config(cluster_config):
        return bootstrap_azure(cluster_config)
