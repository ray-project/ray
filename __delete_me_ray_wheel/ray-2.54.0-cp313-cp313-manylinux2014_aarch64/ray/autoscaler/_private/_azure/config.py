import json
import logging
import os
import random
import time
from hashlib import md5, sha256
from pathlib import Path
from typing import Any, Callable
from uuid import UUID

from azure.common.credentials import get_cli_profile
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode

from ray.autoscaler._private.util import (
    generate_rsa_key_pair,
    generate_ssh_key_name,
    generate_ssh_key_paths,
)

# Built-in Azure Contributor role definition ID used for role assignments.
CONTRIBUTOR_ROLE_DEFINITION_ID = "b24988ac-6180-42a0-ab88-20f7382dd24c"

UNIQUE_ID_LEN = 4

logger = logging.getLogger(__name__)


def get_azure_sdk_function(client: Any, function_name: str) -> Callable:
    """Retrieve a callable function from Azure SDK client object.

    Newer versions of the various client SDKs renamed function names to
    have a begin_ prefix. This function supports both the old and new
    versions of the SDK by first trying the old name and falling back to
    the prefixed new name.
    """
    func = getattr(
        client, function_name, getattr(client, f"begin_{function_name}", None)
    )
    if func is None:
        raise AttributeError(
            "'{obj}' object has no {func} or begin_{func} attribute".format(
                obj={client.__name__}, func=function_name
            )
        )
    return func


def bootstrap_azure(config):
    config = _configure_key_pair(config)
    config = _configure_resource_group(config)
    return config


def _configure_resource_group(config):
    # TODO: look at availability sets
    # https://docs.microsoft.com/en-us/azure/virtual-machines/windows/tutorial-availability-sets
    subscription_id = config["provider"].get("subscription_id")
    if subscription_id is None:
        subscription_id = get_cli_profile().get_subscription_id()
    resource_client = ResourceManagementClient(AzureCliCredential(), subscription_id)
    config["provider"]["subscription_id"] = subscription_id
    logger.info("Using subscription id: %s", subscription_id)

    assert (
        "resource_group" in config["provider"]
    ), "Provider config must include resource_group field"
    resource_group = config["provider"]["resource_group"]

    assert (
        "location" in config["provider"]
    ), "Provider config must include location field"
    params = {"location": config["provider"]["location"]}

    if "tags" in config["provider"]:
        params["tags"] = config["provider"]["tags"]

    logger.info("Creating/Updating resource group: %s", resource_group)
    rg_create_or_update = get_azure_sdk_function(
        client=resource_client.resource_groups, function_name="create_or_update"
    )
    rg_create_or_update(resource_group_name=resource_group, parameters=params)

    # load the template file
    current_path = Path(__file__).parent
    template_path = current_path.joinpath("azure-config-template.json")
    with open(template_path, "r") as template_fp:
        template = json.load(template_fp)

    logger.info("Using cluster name: %s", config["cluster_name"])

    # set unique id for resources in this cluster
    unique_id = config["provider"].get("unique_id")
    if unique_id is None:
        hasher = sha256()
        hasher.update(config["provider"]["resource_group"].encode("utf-8"))
        unique_id = hasher.hexdigest()[:UNIQUE_ID_LEN]
    else:
        unique_id = str(unique_id)
    config["provider"]["unique_id"] = unique_id
    logger.info("Using unique id: %s", unique_id)
    cluster_id = "{}-{}".format(config["cluster_name"], unique_id)
    role_assignment_name = f"ray-{cluster_id}-ra"
    role_assignment_guid = _generate_arm_guid(role_assignment_name)
    role_assignment_resource_id = (
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers"
        f"/Microsoft.Authorization/roleAssignments/{role_assignment_guid}"
    )

    subnet_mask = config["provider"].get("subnet_mask")
    if subnet_mask is None:
        # choose a random subnet, skipping most common value of 0
        random.seed(unique_id)
        subnet_mask = "10.{}.0.0/16".format(random.randint(1, 254))
    logger.info("Using subnet mask: %s", subnet_mask)

    # Copy over properties from existing subnet.
    # Addresses issue (https://github.com/Azure/azure-quickstart-templates/issues/2786)
    # where existing subnet properties will get overwritten unless explicitly specified
    # during multiple deployments even if vnet/subnet do not change.
    # May eventually be fixed by passing empty subnet list if they already exist:
    # https://techcommunity.microsoft.com/t5/azure-networking-blog/azure-virtual-network-now-supports-updates-without-subnet/ba-p/4067952
    list_by_rg = get_azure_sdk_function(
        client=resource_client.resources, function_name="list_by_resource_group"
    )
    existing_vnets = list(
        list_by_rg(
            resource_group,
            f"substringof('{unique_id}', name) and "
            "resourceType eq 'Microsoft.Network/virtualNetworks'",
        )
    )
    if len(existing_vnets) > 0:
        vnid = existing_vnets[0].id
        get_by_id = get_azure_sdk_function(
            client=resource_client.resources, function_name="get_by_id"
        )

        # Query for supported API versions for Microsoft.Network/virtualNetworks
        # because resource_client.DEFAULT_API_VERSION is not always supported.
        # (Example: "2024-11-01" is the default at the time of this writing)
        # Use "2024-10-01" as a fallback if we can't determine the latest stable version.
        vnet_api_version = "2024-10-01"
        try:
            # Get supported API versions for Microsoft.Network provider
            providers = resource_client.providers.get("Microsoft.Network")
            vnet_resource_type = next(
                (
                    rt
                    for rt in providers.resource_types
                    if rt.resource_type == "virtualNetworks"
                ),
                None,
            )
            if vnet_resource_type and vnet_resource_type.api_versions:
                stable_versions = [
                    v for v in vnet_resource_type.api_versions if "preview" not in v
                ]
                versions_to_consider = (
                    stable_versions or vnet_resource_type.api_versions
                )
                vnet_api_version = sorted(versions_to_consider)[-1]
                logger.info(
                    "Using API version: %s for virtualNetworks", vnet_api_version
                )
            else:
                logger.warning(
                    "Could not determine supported API versions for virtualNetworks, using fallback version %s",
                    vnet_api_version,
                )
        except Exception as e:
            logger.warning(
                "Failed to query Microsoft.Network provider: %s. Using fallback API version 2024-10-01",
                str(e),
            )

        subnet = get_by_id(vnid, vnet_api_version).properties["subnets"][0]
        template_vnet = next(
            (
                rs
                for rs in template["resources"]
                if rs["type"] == "Microsoft.Network/virtualNetworks"
            ),
            None,
        )
        if template_vnet is not None:
            template_subnets = template_vnet["properties"].get("subnets")
            if template_subnets is not None:
                template_subnets[0]["properties"].update(subnet["properties"])

    # Get or create an MSI name and resource group.
    # Defaults to current resource group if not provided.
    use_existing_msi = (
        "msi_name" in config["provider"] and "msi_resource_group" in config["provider"]
    )
    msi_resource_group = config["provider"].get("msi_resource_group", resource_group)
    msi_name = config["provider"].get("msi_name", f"ray-{cluster_id}-msi")
    logger.info(
        "Using msi_name: %s from msi_resource_group: %s", msi_name, msi_resource_group
    )

    existing_principal_id = None
    if not use_existing_msi:
        # When creating a MSI for managing Azure resources, we first need to clean up
        # any role assignments from the previous MSI's principal ID to avoid
        # orphaned permissions when the MSI gets recreated with a new principal ID.
        # Role assignments cannot be updated, only created/removed, so cleanup is required.
        msi_resource_id = (
            f"/subscriptions/{subscription_id}/resourceGroups/{msi_resource_group}"
            f"/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{msi_name}"
        )
        try:
            get_identity = get_azure_sdk_function(
                client=resource_client.resources, function_name="get_by_id"
            )
            existing_msi = get_identity(msi_resource_id, "2023-01-31")
            existing_principal_id = getattr(existing_msi, "properties", {}).get(
                "principalId"
            )
        except ResourceNotFoundError:
            existing_principal_id = None
        except Exception as exc:
            logger.warning(
                "Failed to query MSI %s for existing principal: %s",
                msi_name,
                exc,
            )

        if existing_principal_id:
            logger.info(
                "Removing existing role assignments for MSI principal %s before recreation",
                existing_principal_id,
            )
            _delete_role_assignments_for_principal(
                resource_client,
                resource_group,
                existing_principal_id,
            )

        delete_role_assignment = get_azure_sdk_function(
            client=resource_client.resources, function_name="delete_by_id"
        )
        get_role_assignment = get_azure_sdk_function(
            client=resource_client.resources, function_name="get_by_id"
        )

        role_assignment_known_missing = False
        initial_query_failed = False
        try:
            get_role_assignment(
                role_assignment_resource_id,
                "2022-04-01",
            )
        except ResourceNotFoundError:
            role_assignment_known_missing = True
            logger.debug(
                "Role assignment %s not found before MSI creation; skipping deletion",
                role_assignment_guid,
            )
        except Exception as exc:
            logger.warning(
                "Failed to query role assignment %s before deletion: %s",
                role_assignment_guid,
                exc,
            )
            initial_query_failed = True

        if not role_assignment_known_missing:
            try:
                delete_lro = delete_role_assignment(
                    resource_id=role_assignment_resource_id,
                    api_version="2022-04-01",
                )
                if hasattr(delete_lro, "wait"):
                    delete_lro.wait()
                logger.info(
                    "Deleted existing role assignment %s before recreating MSI",
                    role_assignment_guid,
                )

                if initial_query_failed:
                    logger.debug(
                        "Retrying verification for role assignment %s",
                        role_assignment_guid,
                    )

                if not _wait_for_role_assignment_deletion(
                    get_role_assignment,
                    role_assignment_resource_id,
                    role_assignment_guid,
                ):
                    logger.warning(
                        "Role assignment %s not confirmed deleted",
                        role_assignment_guid,
                    )
            except ResourceNotFoundError:
                logger.debug(
                    "Role assignment %s disappeared before deletion attempt; continuing",
                    role_assignment_guid,
                )
            except Exception as e:
                logger.warning(
                    "Failed to delete role assignment %s before MSI creation: %s",
                    role_assignment_guid,
                    e,
                )

    parameters = {
        "properties": {
            "mode": DeploymentMode.incremental,
            "template": template,
            "parameters": {
                "subnet": {"value": subnet_mask},
                "clusterId": {"value": cluster_id},
                "msiName": {"value": msi_name},
                "msiResourceGroup": {"value": msi_resource_group},
                "createMsi": {"value": not use_existing_msi},
                "roleAssignmentGuid": {"value": role_assignment_guid},
            },
        }
    }

    create_or_update = get_azure_sdk_function(
        client=resource_client.deployments, function_name="create_or_update"
    )
    outputs = (
        create_or_update(
            resource_group_name=resource_group,
            deployment_name="ray-config",
            parameters=parameters,
        )
        .result()
        .properties.outputs
    )

    # append output resource ids to be used with vm creation
    config["provider"]["msi"] = outputs["msi"]["value"]
    config["provider"]["nsg"] = outputs["nsg"]["value"]
    config["provider"]["subnet"] = outputs["subnet"]["value"]

    return config


def _configure_key_pair(config):
    """
    Configure SSH keypair. Use user specified custom paths, otherwise,
    generate Ray-specific keypair in this format: "ray-autoscaler_azure_{region}_{resource_group}_{ssh_user}_{index}"
    """
    ssh_user = config["auth"]["ssh_user"]
    public_key = None

    # Check if user specified custom SSH key paths
    user_specified_private_key = "ssh_private_key" in config["auth"]
    user_specified_public_key = "ssh_public_key" in config["auth"]

    # Validate that the user either specfied both keys or none, but not just one
    if user_specified_private_key != user_specified_public_key:
        if user_specified_private_key:
            missing_key, specified_key = "ssh_public_key", "ssh_private_key"
        else:
            missing_key, specified_key = "ssh_private_key", "ssh_public_key"
        raise ValueError(
            f"{specified_key} is specified but {missing_key} is missing. "
            "Both SSH key paths must be specified together, or omit both from "
            "your config to use auto-generated keys."
        )

    if user_specified_private_key and user_specified_public_key:
        # User specified custom paths
        private_key_path = Path(config["auth"]["ssh_private_key"]).expanduser()
        public_key_path = Path(config["auth"]["ssh_public_key"]).expanduser()

        # Validate that user-specified keys exist
        missing_keys = []
        if not private_key_path.is_file():
            missing_keys.append(f"ssh_private_key: {private_key_path}")
        if not public_key_path.is_file():
            missing_keys.append(f"ssh_public_key: {public_key_path}")

        if missing_keys:
            raise ValueError(
                "SSH key files from config do not exist: {}. "
                "Please create the keys or remove the custom paths from your config "
                "to use auto-generated keys.".format(", ".join(missing_keys))
            )
        logger.info(
            "Using specified SSH keys from config: {} and {}".format(
                private_key_path, public_key_path
            )
        )

        with open(public_key_path, "r") as f:
            public_key = f.read()
    else:
        # Generate Ray-specific keys
        region = config["provider"]["location"]
        resource_group = config["provider"]["resource_group"]

        # Generate single deterministic key name for this configuration
        key_name = generate_ssh_key_name(
            "azure", None, region, resource_group, ssh_user
        )
        public_key_path, private_key_path = generate_ssh_key_paths(key_name)

        # Check if this key pair already exists
        if os.path.exists(private_key_path) and os.path.exists(public_key_path):
            logger.info(
                "Using existing Ray-specific SSH keys: {} and {}".format(
                    private_key_path, public_key_path
                )
            )
            with open(public_key_path, "r") as f:
                public_key = f.read()
        else:
            # Create a key pair since it doesn't exist locally
            logger.info(
                "Generating new Ray-specific SSH key pair at {} and {}".format(
                    private_key_path, public_key_path
                )
            )
            os.makedirs(os.path.dirname(private_key_path), exist_ok=True)
            public_key, private_key = generate_rsa_key_pair()
            with open(
                private_key_path,
                "w",
                opener=lambda path, flags: os.open(path, flags, 0o600),
            ) as f:
                f.write(private_key)
            with open(public_key_path, "w") as f:
                f.write(public_key)

        assert os.path.exists(
            private_key_path
        ), "Private key file {} not found for user {}".format(
            private_key_path, ssh_user
        )

    config["auth"]["ssh_private_key"] = str(private_key_path)
    # Remove public key path because bootstrap config must only contain paths that exist on head node
    config["auth"].pop("ssh_public_key", None)

    for node_type in config["available_node_types"].values():
        azure_arm_parameters = node_type["node_config"].setdefault(
            "azure_arm_parameters", {}
        )
        azure_arm_parameters["adminUsername"] = ssh_user
        azure_arm_parameters["publicKey"] = public_key

    return config


def _delete_role_assignments_for_principal(
    resource_client: ResourceManagementClient,
    resource_group: str,
    principal_id: str,
) -> None:
    """Delete all role assignments in the resource group for the given principal.

    Uses the generic ResourceManagementClient so we avoid depending on
    azure-mgmt-authorization. All role assignments associated with the
    provided principal ID are removed.
    """

    if not principal_id:
        return

    list_by_rg = get_azure_sdk_function(
        client=resource_client.resources, function_name="list_by_resource_group"
    )
    delete_role_assignment = get_azure_sdk_function(
        client=resource_client.resources, function_name="delete_by_id"
    )

    try:
        assignments = list(
            list_by_rg(
                resource_group,
                "resourceType eq 'Microsoft.Authorization/roleAssignments'",
            )
        )
        logger.debug(
            "Found %d role assignments in resource group %s while cleaning up principal %s",
            len(assignments),
            resource_group,
            principal_id,
        )
    except HttpResponseError as exc:
        logger.warning(
            "Failed to enumerate role assignments for resource group %s: %s",
            resource_group,
            exc,
        )
        return

    for assignment in assignments:
        properties = getattr(assignment, "properties", {}) or {}
        logger.debug(
            "Inspecting role assignment %s with principalId=%s roleDefinitionId=%s",
            getattr(assignment, "name", "<unknown>"),
            properties.get("principalId"),
            properties.get("roleDefinitionId"),
        )
        if properties.get("principalId") != principal_id:
            continue

        try:
            delete_lro = delete_role_assignment(
                resource_id=assignment.id,
                api_version="2022-04-01",
            )
            if hasattr(delete_lro, "wait"):
                delete_lro.wait()
            logger.info(
                "Deleted existing role assignment %s for principal %s",
                assignment.name,
                principal_id,
            )
        except ResourceNotFoundError:
            logger.debug(
                "Role assignment %s not found while processing principal %s",
                assignment.name,
                principal_id,
            )
        except Exception as exc:
            logger.warning(
                "Failed to delete role assignment %s for principal %s: %s",
                assignment.name,
                principal_id,
                exc,
            )


def _wait_for_role_assignment_deletion(
    get_role_assignment: Callable[..., Any],
    resource_id: str,
    role_assignment_guid: str,
    *,
    max_attempts: int = 10,
    delay_seconds: int = 2,
) -> bool:
    """Poll until a role assignment disappears after deletion.

    Returns True if the assignment is confirmed deleted, False otherwise.
    Logs detailed progress to aid troubleshooting when transient errors occur.
    """

    for attempt in range(1, max_attempts + 1):
        try:
            get_role_assignment(resource_id, "2022-04-01")
        except ResourceNotFoundError:
            return True
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "Attempt %d/%d to verify removal of role assignment %s failed: %s",
                attempt,
                max_attempts,
                role_assignment_guid,
                exc,
            )
        else:
            logger.debug(
                "Role assignment %s still present after deletion (attempt %d/%d)",
                role_assignment_guid,
                attempt,
                max_attempts,
            )

        if attempt < max_attempts:
            time.sleep(delay_seconds)

    return False


def _generate_arm_guid(*values: Any) -> str:
    """Replicates ARM template guid() function for creating deterministic IDs."""

    concatenated = "".join(str(v) for v in values)
    return str(UUID(md5(concatenated.encode("utf-8")).hexdigest()))
