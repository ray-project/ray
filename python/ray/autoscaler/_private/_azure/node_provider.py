import json
import logging
import os
import time
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from threading import RLock
from typing import List, Optional
from uuid import uuid4

from azure.common.credentials import get_cli_profile
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode

from ray._common.usage.usage_lib import get_cloud_from_metadata_requests
from ray.autoscaler._private._azure.config import (
    _delete_role_assignments_for_principal,
    _generate_arm_guid,
    bootstrap_azure,
    get_azure_sdk_function,
)
from ray.autoscaler._private.constants import (
    AUTOSCALER_NODE_START_WAIT_S,
    AUTOSCALER_NODE_TERMINATE_WAIT_S,
    MAX_PARALLEL_SHUTDOWN_WORKERS,
)
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    TAG_RAY_CLUSTER_NAME,
    TAG_RAY_LAUNCH_CONFIG,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_USER_NODE_TYPE,
)

VM_NAME_MAX_LEN = 64
UNIQUE_ID_LEN = 4

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
        subscription_id = provider_config.get("subscription_id")
        if subscription_id is None:
            # Get subscription from logged in azure profile
            # if it isn't provided in the provider_config
            # so operations like `get-head-ip` will work
            subscription_id = get_cli_profile().get_subscription_id()
            logger.info(
                "subscription_id not found in provider config, falling back "
                f"to subscription_id from the logged in azure profile: {subscription_id}"
            )
        self.cache_stopped_nodes = provider_config.get("cache_stopped_nodes", True)

        # Detect cloud environment to optimize Azure credential chain.
        # On non-Azure clouds (AWS, GCP), skip Azure-specific auth methods
        # (managed identity, workload identity) to avoid IMDS timeout delays / failures.
        detected_cloud = get_cloud_from_metadata_requests()
        on_azure = detected_cloud == "azure"

        if on_azure:
            logger.info(
                "Initializing Azure node provider for Azure infrastructure "
                "running on Azure cloud environment"
            )
        else:
            logger.info(
                f"Initializing Azure node provider for Azure infrastructure "
                f"but detected this is running on a '{detected_cloud}' environment. "
                f"Skipping Azure-specific authentication methods to avoid timeouts."
            )

        credential = DefaultAzureCredential(
            exclude_shared_token_cache_credential=True,
            exclude_managed_identity_credential=not on_azure,
            exclude_workload_identity_credential=not on_azure,
        )
        self.compute_client = ComputeManagementClient(credential, subscription_id)
        self.network_client = NetworkManagementClient(credential, subscription_id)
        self.resource_client = ResourceManagementClient(credential, subscription_id)

        self.lock = RLock()

        # cache node objects
        self.cached_nodes = {}

        # Cache terminating node operations
        self.terminating_nodes: dict[str, Future] = {}
        self.termination_executor = ThreadPoolExecutor(
            max_workers=MAX_PARALLEL_SHUTDOWN_WORKERS
        )

    @synchronized
    def _get_filtered_nodes(self, tag_filters):
        # add cluster name filter to only get nodes from this cluster
        cluster_tag_filters = {**tag_filters, TAG_RAY_CLUSTER_NAME: self.cluster_name}

        def match_tags(tags):
            for k, v in cluster_tag_filters.items():
                if tags.get(k) != v:
                    return False
            return True

        vms = self.compute_client.virtual_machines.list(
            resource_group_name=self.provider_config["resource_group"]
        )

        nodes = [self._extract_metadata(vm) for vm in vms]
        self.cached_nodes = {node["name"]: node for node in nodes}

        # Update terminating nodes list by removing nodes that
        # have finished termination.
        self.terminating_nodes = {
            k: v for k, v in self.terminating_nodes.items() if not v.done()
        }

        return {
            k: v
            for k, v in self.cached_nodes.items()
            if v["tags"] is not None and match_tags(v["tags"])
        }

    def _extract_metadata(self, vm):
        # get tags
        metadata = {"name": vm.name, "tags": vm.tags, "status": ""}

        # get status
        resource_group = self.provider_config["resource_group"]
        try:
            instance = self.compute_client.virtual_machines.instance_view(
                resource_group_name=resource_group, vm_name=vm.name
            ).as_dict()
        except ResourceNotFoundError:
            return metadata

        for status in instance["statuses"]:
            # If ProvisioningState is "failed" (e.g.,
            # ProvisioningState/failed/RetryableError), we can get a third
            # string here, so we need to limit to the first two outputs.
            code, state = status["code"].split("/")[:2]
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

        # Get public IP if not using internal IPs or if this is the
        # head node and use_external_head_ip is True
        if not self.provider_config.get("use_internal_ips", False) or (
            self.provider_config.get("use_external_head_ip", False)
            and metadata["tags"][TAG_RAY_NODE_KIND] == NODE_KIND_HEAD
        ):
            public_ip_id = ip_config.public_ip_address.id
            metadata["public_ip_name"] = public_ip_id.split("/")[-1]
            public_ip = self.network_client.public_ip_addresses.get(
                resource_group_name=resource_group,
                public_ip_address_name=metadata["public_ip_name"],
            )
            metadata["external_ip"] = public_ip.ip_address

        metadata["internal_ip"] = ip_config.private_ip_address

        return metadata

    def _get_zones_for_vm_size(self, vm_size, location):
        """Get usable availability zones for a given VM size in a specific location."""
        try:
            # Note: Azure ResourceSKUs API filters don't work reliably(?), so we query all SKUs
            # and filter in code. Each SKU object represents one location for the VM size.
            skus = self.compute_client.resource_skus.list()

            for sku in skus:
                if sku.name == vm_size and sku.location_info:
                    # Each SKU object represents one location, check if it matches our target
                    for location_info in sku.location_info:
                        if location_info.location.lower() == location.lower():
                            zones = location_info.zones if location_info.zones else []
                            logger.debug(
                                f"Found {vm_size} in {location} with zones: {zones}"
                            )
                            return sorted(zones)

            logger.warning(f"No zones found for {vm_size} in {location}")
            return []  # No zones available for this VM size
        except Exception as e:
            logger.warning(
                f"Failed to get zones for VM size {vm_size} in location {location}: {str(e)}"
            )
            return []

    def _parse_availability_zones(
        self, availability_zone_config: Optional[str]
    ) -> Optional[List[str]]:
        """Parse availability_zone configuration from comma-separated string format.

        Args:
            availability_zone_config: Can be:
                - String: comma-separated zones like "1,2,3"
                - "none": explicitly disable zones
                - "auto": let Azure automatically pick zones
                - None: no zones specified (defaults to letting Azure pick)

        Returns:
            List of zone strings, or None if zones explicitly disabled, or [] if auto/unspecified
        """
        if availability_zone_config is None:
            return []  # Auto - let Azure pick

        # Handle string format (AWS-style comma-separated)
        if isinstance(availability_zone_config, str):
            # Strip whitespace and split by comma
            zones = [zone.strip() for zone in availability_zone_config.split(",")]

            # Handle special cases (case-insensitive)
            if len(zones) == 1:
                zone_lower = zones[0].lower()
                if zone_lower in ["none", "null"]:
                    return None  # Explicitly disabled
                elif zone_lower == "auto":
                    return []  # Auto - let Azure pick

            # Handle empty string or whitespace-only
            if not zones or all(not zone for zone in zones):
                return []  # Auto - let Azure pick
            return zones

        # Unsupported format
        raise ValueError(
            f"availability_zone must be a string, got {type(availability_zone_config).__name__}: {availability_zone_config!r}"
        )

    def _validate_zones_for_node_pool(self, zones, location, vm_size):
        """Validate that the specified zones are available for the given VM size in the location."""
        # Special case: zones=None means explicitly disabled availability zones
        if zones is None:
            logger.info(
                "Zones explicitly disabled with 'none' - will create VM without an availability zone"
            )
            return None  # Special return value to indicate "no zones by choice"

        vm_zones = self._get_zones_for_vm_size(vm_size, location)

        available_zones = set(vm_zones)
        if not available_zones:
            logger.warning("No zones available for this VM size and location")
            return []

        if zones:
            requested_zones = {str(z) for z in zones}
            intersection = sorted(available_zones.intersection(requested_zones))
            return intersection

        return sorted(available_zones)

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
        return [
            k
            for k, v in nodes.items()
            if not v["status"].startswith("deallocat") or k in self.terminating_nodes
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
        location = self.provider_config["location"]
        vm_size = node_config["azure_arm_parameters"]["vmSize"]

        # Determine availability zones with precedence: node-level > provider-level
        # Check for "availability_zone" field in node config first
        node_availability_zone = node_config.get("azure_arm_parameters", {}).get(
            "availability_zone"
        )
        # Then check provider-level "availability_zone"
        provider_availability_zone = self.provider_config.get("availability_zone")

        requested_zones = []
        zone_source = "default"

        # Precedence: node availability_zone > provider availability_zone
        if node_availability_zone is not None:
            requested_zones = self._parse_availability_zones(node_availability_zone)
            zone_source = "node config availability_zone"
        elif provider_availability_zone is not None:
            requested_zones = self._parse_availability_zones(provider_availability_zone)
            zone_source = "provider availability_zone"

        logger.info(f"Requested zones from {zone_source}: {requested_zones}")

        # Get actually available zones for this VM size
        available_zones = self._validate_zones_for_node_pool(
            requested_zones, location, vm_size
        )

        # Handle explicit zone disabling
        zones_explicitly_disabled = available_zones is None

        if requested_zones and not zones_explicitly_disabled and not available_zones:
            raise ValueError(
                f"No available zones for VM size {vm_size} in {location}. "
                f"Requested: {requested_zones}, but none are available for this VM size."
            )

        # load the template file
        current_path = Path(__file__).parent
        template_path = current_path.joinpath("azure-vm-template.json")
        with open(template_path, "r") as template_fp:
            template = json.load(template_fp)

        # get the tags
        config_tags = node_config.get("tags", {}).copy()
        config_tags.update(tags)
        config_tags[TAG_RAY_CLUSTER_NAME] = self.cluster_name

        deployment_name = "{node}-{unique_id}-{vm_id}".format(
            node=config_tags.get(TAG_RAY_NODE_NAME, "node"),
            unique_id=self.provider_config["unique_id"],
            vm_id=uuid4().hex[:UNIQUE_ID_LEN],
        )[:VM_NAME_MAX_LEN]

        template_params = node_config["azure_arm_parameters"].copy()
        # Remove availability_zone from template params since ARM template expects "zones"
        template_params.pop("availability_zone", None)
        # Use deployment_name for the vmName template parameter since
        # the template will append copyIndex() for each VM that gets created
        # to guarantee uniqueness.
        template_params["vmName"] = deployment_name
        # Provision public IP if not using internal IPs or if this is the
        # head node and use_external_head_ip is True
        template_params["provisionPublicIp"] = not self.provider_config.get(
            "use_internal_ips", False
        ) or (
            self.provider_config.get("use_external_head_ip", False)
            and config_tags[TAG_RAY_NODE_KIND] == NODE_KIND_HEAD
        )
        template_params["vmTags"] = config_tags
        template_params["vmCount"] = count
        template_params["msi"] = self.provider_config["msi"]
        template_params["nsg"] = self.provider_config["nsg"]
        template_params["subnet"] = self.provider_config["subnet"]

        # Add zone information based on availability and requested zones
        if zones_explicitly_disabled:
            # User explicitly disabled zones with ["None"]
            template_params["zones"] = []
            logger.info(
                f"Creating {count} VMs with zones explicitly disabled (no availability zone)"
            )
        elif available_zones:
            # Pass the list of available zones to the template
            template_params["zones"] = available_zones
            logger.info(
                f"Creating {count} VMs, distributed across availability zones: {available_zones}"
            )
        else:
            # For non-zonal deployments (no zones available), use empty array
            template_params["zones"] = []
            logger.info(f"Creating {count} VMs without specific availability zone")

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
            client=self.resource_client.deployments,
            function_name="create_or_update",
        )
        create_or_update(
            resource_group_name=resource_group,
            deployment_name=deployment_name,
            parameters=parameters,
        ).wait(timeout=AUTOSCALER_NODE_START_WAIT_S)

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

        # If node_id is in terminating nodes dict, it's already terminating
        # Otherwise, kick off termination and add it to the dict
        elif node_id not in self.terminating_nodes:
            self.terminating_nodes[node_id] = self.termination_executor.submit(
                self._delete_node_and_resources, resource_group, node_id
            )

    def _delete_node_and_resources(self, resource_group, node_id):
        try:
            vm = self.compute_client.virtual_machines.get(
                resource_group_name=resource_group, vm_name=node_id
            )
        except ResourceNotFoundError as e:
            # Node no longer exists
            logger.warning("Failed to delete VM: {}".format(e))
            return

        # Gather dependent disks
        disks = set()
        if vm.storage_profile is not None and vm.storage_profile.data_disks is not None:
            for d in vm.storage_profile.data_disks:
                if d.name is not None:
                    disks.add(d.name)
        if (
            vm.storage_profile is not None
            and vm.storage_profile.os_disk is not None
            and vm.storage_profile.os_disk.name is not None
        ):
            disks.add(vm.storage_profile.os_disk.name)

        # Gather dependent NICs and public IPs
        nics = set()
        ips = set()
        if (
            vm.network_profile is not None
            and vm.network_profile.network_interfaces is not None
        ):
            for nint in vm.network_profile.network_interfaces:
                if nint.id is not None:
                    nic_name = nint.id.split("/")[-1]
                    nics.add(nic_name)
                    # Get public IP if not using internal IPs or if this is the
                    # head node and use_external_head_ip is True
                    if not self.provider_config.get("use_internal_ips", False) or (
                        self.provider_config.get("use_external_head_ip", False)
                        and vm.tags[TAG_RAY_NODE_KIND] == NODE_KIND_HEAD
                    ):
                        nic = self.network_client.network_interfaces.get(
                            resource_group_name=resource_group,
                            network_interface_name=nic_name,
                        )
                        if nic.ip_configurations is not None:
                            for ipc in nic.ip_configurations:
                                if ipc.public_ip_address.id is not None:
                                    ips.add(ipc.public_ip_address.id.split("/")[-1])

        # Delete VM
        st = time.monotonic()
        delete = get_azure_sdk_function(
            client=self.compute_client.virtual_machines,
            function_name="delete",
        )
        try:
            delete(resource_group_name=resource_group, vm_name=node_id).wait(
                timeout=AUTOSCALER_NODE_TERMINATE_WAIT_S
            )
        except Exception as e:
            logger.warning("Failed to delete VM: {}".format(e))

        # Delete disks (no need to wait for these, but gather the LROs for end)
        disk_lros = []
        delete = get_azure_sdk_function(
            client=self.compute_client.disks, function_name="delete"
        )
        for d in disks:
            try:
                disk_lros.append(
                    delete(
                        resource_group_name=resource_group,
                        disk_name=d,
                    )
                )
            except Exception as e:
                logger.warning("Failed to delete disk: {}".format(e))

        # Delete NICs
        nic_lros = []
        delete = get_azure_sdk_function(
            client=self.network_client.network_interfaces, function_name="delete"
        )
        for n in nics:
            try:
                nic_lros.append(
                    delete(
                        resource_group_name=resource_group,
                        network_interface_name=n,
                    )
                )
            except Exception as e:
                logger.warning("Failed to delete NIC: {}".format(e))

        while (
            not all(nlro.done() for nlro in nic_lros)
            and (time.monotonic() - st) < AUTOSCALER_NODE_TERMINATE_WAIT_S
        ):
            time.sleep(0.1)

        # Delete Public IPs
        delete = get_azure_sdk_function(
            client=self.network_client.public_ip_addresses,
            function_name="delete",
        )
        ip_lros = []
        for ip in ips:
            try:
                ip_lros.append(
                    delete(
                        resource_group_name=resource_group,
                        public_ip_address_name=ip,
                    )
                )
            except Exception as e:
                logger.warning("Failed to delete public IP: {}".format(e))

        while (
            not all(dlro.done() for dlro in disk_lros)
            and (time.monotonic() - st) < AUTOSCALER_NODE_TERMINATE_WAIT_S
        ):
            time.sleep(0.1)
        while (
            not all(iplro.done() for iplro in ip_lros)
            and (time.monotonic() - st) < AUTOSCALER_NODE_TERMINATE_WAIT_S
        ):
            time.sleep(0.1)

    def cleanup_cluster_resources(self):
        """Delete shared cluster infrastructure (MSI, NSG, Subnet, VNet)."""

        resource_group = self.provider_config["resource_group"]

        msi_principal_id = self._cleanup_managed_identity(
            resource_group, self.provider_config.get("msi")
        )

        subnet_id = self.provider_config.get("subnet")
        vnet_name = self._cleanup_subnet(resource_group, subnet_id)

        nsg_id = self.provider_config.get("nsg")
        self._cleanup_nsg(resource_group, nsg_id)

        self._cleanup_vnet(resource_group, subnet_id, vnet_name)

        self._cleanup_role_assignments(resource_group, msi_principal_id)

        self._prune_provider_config_entries()
        self._cleanup_config_cache()

    @staticmethod
    def _get_resource_name_from_id(resource_id: Optional[str]) -> Optional[str]:
        if resource_id:
            return resource_id.split("/")[-1]
        return None

    @staticmethod
    def _retry_delete(delete_fn, max_retries: int = 5, initial_delay: int = 2):
        """Retry a delete operation with exponential backoff."""

        delay = initial_delay
        for attempt in range(max_retries):
            try:
                return delete_fn()
            except Exception as exc:  # noqa: BLE001
                error_msg = str(exc)
                if "InUse" in error_msg and attempt < max_retries - 1:
                    logger.info(
                        "Resource still in use, retrying in %ss (attempt %s/%s)...",
                        delay,
                        attempt + 1,
                        max_retries,
                    )
                    time.sleep(delay)
                    delay *= 2
                else:
                    raise

    def _cleanup_managed_identity(
        self, resource_group: str, msi_id: Optional[str]
    ) -> Optional[str]:
        if not msi_id:
            return None

        msi_name = self._get_resource_name_from_id(msi_id)
        if not msi_name:
            return None

        msi_principal_id: Optional[str] = None
        try:
            get_identity = get_azure_sdk_function(
                client=self.resource_client.resources,
                function_name="get_by_id",
            )
            existing_msi = get_identity(msi_id, "2023-01-31")
            msi_principal_id = getattr(existing_msi, "properties", {}).get(
                "principalId"
            )
        except ResourceNotFoundError:
            msi_principal_id = None
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to query MSI %s for principal ID prior to deletion: %s",
                msi_name,
                exc,
            )

        try:
            logger.info("Deleting Managed Service Identity: %s", msi_name)
            delete = get_azure_sdk_function(
                client=self.resource_client.resources,
                function_name="delete_by_id",
            )
            delete(resource_id=msi_id, api_version="2023-01-31").wait()
            logger.info("Successfully deleted MSI: %s", msi_name)
        except ResourceNotFoundError:
            logger.info("MSI %s not found, may have been already deleted", msi_name)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to delete MSI %s: %s", msi_name, exc)

        return msi_principal_id

    def _cleanup_subnet(
        self, resource_group: str, subnet_id: Optional[str]
    ) -> Optional[str]:
        if not subnet_id:
            return None

        subnet_name = self._get_resource_name_from_id(subnet_id)
        vnet_name: Optional[str] = None

        if subnet_id and "/virtualNetworks/" in subnet_id:
            parts = subnet_id.split("/")
            vnet_idx = parts.index("virtualNetworks")
            if vnet_idx + 1 < len(parts):
                vnet_name = parts[vnet_idx + 1]

        if not subnet_name or not vnet_name:
            return None

        try:
            logger.info("Deleting Subnet: %s in VNet: %s", subnet_name, vnet_name)

            def delete_subnet():
                delete = get_azure_sdk_function(
                    client=self.network_client.subnets, function_name="delete"
                )
                delete(
                    resource_group_name=resource_group,
                    virtual_network_name=vnet_name,
                    subnet_name=subnet_name,
                ).wait()

            self._retry_delete(delete_subnet)
            logger.info("Successfully deleted Subnet: %s", subnet_name)
        except ResourceNotFoundError:
            logger.info(
                "Subnet %s not found, may have been already deleted", subnet_name
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to delete Subnet %s: %s", subnet_name, exc)

        return vnet_name

    def _cleanup_nsg(self, resource_group: str, nsg_id: Optional[str]) -> None:
        if not nsg_id:
            return

        nsg_name = self._get_resource_name_from_id(nsg_id)
        if not nsg_name:
            return

        try:
            logger.info("Deleting Network Security Group: %s", nsg_name)

            def delete_nsg():
                delete = get_azure_sdk_function(
                    client=self.network_client.network_security_groups,
                    function_name="delete",
                )
                delete(
                    resource_group_name=resource_group,
                    network_security_group_name=nsg_name,
                ).wait()

            self._retry_delete(delete_nsg)
            logger.info("Successfully deleted NSG: %s", nsg_name)
        except ResourceNotFoundError:
            logger.info("NSG %s not found, may have been already deleted", nsg_name)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to delete NSG %s: %s", nsg_name, exc)

    def _cleanup_vnet(
        self,
        resource_group: str,
        subnet_id: Optional[str],
        vnet_name: Optional[str],
    ) -> None:
        if not subnet_id or not vnet_name:
            return

        try:
            logger.info("Deleting Virtual Network: %s", vnet_name)

            def delete_vnet():
                delete = get_azure_sdk_function(
                    client=self.network_client.virtual_networks,
                    function_name="delete",
                )
                delete(
                    resource_group_name=resource_group,
                    virtual_network_name=vnet_name,
                ).wait()

            self._retry_delete(delete_vnet)
            logger.info("Successfully deleted VNet: %s", vnet_name)
        except ResourceNotFoundError:
            logger.info("VNet %s not found, may have been already deleted", vnet_name)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to delete VNet %s: %s", vnet_name, exc)

    def _cleanup_role_assignments(
        self, resource_group: str, msi_principal_id: Optional[str]
    ) -> None:
        subscription_id = self.provider_config.get("subscription_id")
        unique_id = self.provider_config.get("unique_id")
        if not subscription_id or not unique_id:
            return

        cluster_id = f"{self.cluster_name}-{unique_id}"
        role_assignment_name = f"ray-{cluster_id}-ra"
        role_assignment_guid = _generate_arm_guid(role_assignment_name)
        role_assignment_id = (
            f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers"
            f"/Microsoft.Authorization/roleAssignments/{role_assignment_guid}"
        )

        if msi_principal_id:
            _delete_role_assignments_for_principal(
                self.resource_client, resource_group, msi_principal_id
            )

        delete_role_assignment = get_azure_sdk_function(
            client=self.resource_client.resources, function_name="delete_by_id"
        )
        try:
            delete_lro = delete_role_assignment(
                resource_id=role_assignment_id,
                api_version="2022-04-01",
            )
            if hasattr(delete_lro, "wait"):
                delete_lro.wait()
            logger.info(
                "Deleted role assignment %s for cluster %s",
                role_assignment_guid,
                self.cluster_name,
            )
        except ResourceNotFoundError:
            logger.debug(
                "Role assignment %s not found during cleanup", role_assignment_guid
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to delete role assignment %s: %s",
                role_assignment_guid,
                exc,
            )

    def _prune_provider_config_entries(self) -> None:
        for key in ("msi", "nsg", "subnet"):
            self.provider_config.pop(key, None)

    def _cleanup_config_cache(self) -> None:
        cache_path = self.provider_config.get("_config_cache_path")
        if not cache_path:
            return

        try:
            if os.path.exists(cache_path):
                os.remove(cache_path)
                logger.info(
                    "Deleted cached Ray config at %s after resource cleanup",
                    cache_path,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to delete cached Ray config %s: %s", cache_path, exc)
        finally:
            self.provider_config.pop("_config_cache_path", None)

    def _get_node(self, node_id):
        self._get_filtered_nodes({})  # Side effect: updates cache
        return self.cached_nodes[node_id]

    def _get_cached_node(self, node_id):
        return self.cached_nodes.get(node_id) or self._get_node(node_id=node_id)

    @staticmethod
    def bootstrap_config(cluster_config):
        return bootstrap_azure(cluster_config)
