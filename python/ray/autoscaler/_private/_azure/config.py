import json
import logging
import random
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable

from azure.common.credentials import get_cli_profile
from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode

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
        subnet = get_by_id(vnid, resource_client.DEFAULT_API_VERSION).properties[
            "subnets"
        ][0]
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
    ssh_user = config["auth"]["ssh_user"]
    public_key = None
    # search if the keys exist
    for key_type in ["ssh_private_key", "ssh_public_key"]:
        try:
            key_path = Path(config["auth"][key_type]).expanduser()
        except KeyError:
            raise Exception("Config must define {}".format(key_type))
        except TypeError:
            raise Exception("Invalid config value for {}".format(key_type))

        assert key_path.is_file(), "Could not find ssh key: {}".format(key_path)

        if key_type == "ssh_public_key":
            with open(key_path, "r") as f:
                public_key = f.read()

    for node_type in config["available_node_types"].values():
        azure_arm_parameters = node_type["node_config"].setdefault(
            "azure_arm_parameters", {}
        )
        azure_arm_parameters["adminUsername"] = ssh_user
        azure_arm_parameters["publicKey"] = public_key

    return config
