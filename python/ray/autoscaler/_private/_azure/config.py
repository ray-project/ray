import json
import logging
from pathlib import Path
import random
from typing import Any, Callable

from azure.common.credentials import get_cli_profile
from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode

RETRIES = 30
MSI_NAME = "ray-msi-user-identity"
NSG_NAME = "ray-nsg"
SUBNET_NAME = "ray-subnet"
VNET_NAME = "ray-vnet"

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

    logger.info("Creating/Updating Resource Group: %s", resource_group)
    rg_create_or_update = get_azure_sdk_function(
        client=resource_client.resource_groups, function_name="create_or_update"
    )
    rg_create_or_update(resource_group_name=resource_group, parameters=params)

    # load the template file
    current_path = Path(__file__).parent
    template_path = current_path.joinpath("azure-config-template.json")
    with open(template_path, "r") as template_fp:
        template = json.load(template_fp)

    # choose a random subnet, skipping most common value of 0
    random.seed(resource_group)
    subnet_mask = "10.{}.0.0/16".format(random.randint(1, 254))

    parameters = {
        "properties": {
            "mode": DeploymentMode.incremental,
            "template": template,
            "parameters": {"subnet": {"value": subnet_mask}},
        }
    }

    create_or_update = get_azure_sdk_function(
        client=resource_client.deployments, function_name="create_or_update"
    )
    create_or_update(
        resource_group_name=resource_group,
        deployment_name="ray-config",
        parameters=parameters,
    ).wait()

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
