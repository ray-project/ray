import logging
import os
import time
import uuid

from azure.common.exceptions import CloudError, AuthenticationError
from azure.common.client_factory import get_client_from_cli_profile
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.msi import ManagedServiceIdentityClient

RETRIES = 30
MSI_NAME = "ray-msi-user-identity"
NSG_NAME = "ray-nsg"
SUBNET_NAME = "ray-subnet"
VNET_NAME = "ray-vnet"

logger = logging.getLogger(__name__)


def bootstrap_azure(config):
    config = _configure_key_pair(config)
    config = _configure_resource_group(config)
    return config


def _get_client(client_class, config):
    kwargs = {}
    if "subscription_id" in config["provider"]:
        kwargs["subscription_id"] = config["provider"]["subscription_id"]

    return get_client_from_cli_profile(client_class=client_class, **kwargs)


def _configure_resource_group(config):
    # TODO: look at availability sets
    # https://docs.microsoft.com/en-us/azure/virtual-machines/windows/tutorial-availability-sets
    resource_client = _get_client(ResourceManagementClient, config)

    subscription_id = resource_client.config.subscription_id
    logger.info("Using subscription id: %s", subscription_id)
    config["provider"]["subscription_id"] = subscription_id

    assert "resource_group" in config["provider"], (
        "Provider config must include resource_group field")
    resource_group = config["provider"]["resource_group"]

    assert "location" in config["provider"], (
        "Provider config must include location field")
    params = {"location": config["provider"]["location"]}

    if "tags" in config["provider"]:
        params["tags"] = config["provider"]["tags"]

    logger.info("Creating/Updating Resource Group: %s", resource_group)
    resource_client.resource_groups.create_or_update(
    resource_group_name=resource_group, parameters=params)

    # load the template
    template_path = os.path.join(os.path.dirname(__file__), 'azure-config-template.json')
    with open(template_path, 'r') as template_file_fd:
        template = json.load(template_file_fd)

    # choose a random subnet
    random.seed(resource_group)
    parameters = {
        "subnet": "10.{}.0.0/16".format(random.randint(0, 254))
    }

    deployment_properties = {
        'mode': DeploymentMode.incremental,
        'template': template,
        'parameters': {k: {'value': v} for k, v in parameters.items()}
    }

    deployment_async_operation = resource_client.deployments.create_or_update(
        resource_group,
        'ray-config',
        deployment_properties
    )
    deployment_async_operation.wait()

    return config

def _configure_key_pair(config):
    ssh_user = config["auth"]["ssh_user"]
    private_key_path = config["auth"].get("ssh_private_key")
    if private_key_path:
        # skip key generation if it is manually specified
        assert os.path.exists(private_key_path), (
            "Could not find private ssh key: {}".format(private_key_path))

        # make sure public key also exists
        public_key_path = config["auth"]["ssh_public_key"]
        assert os.path.exists(public_key_path), (
            "Could not find public ssh key: {}".format(public_key_path))
    else:
        resource_group = config["provider"]["resource_group"]

        # look for an existing key pair
        key_name = "ray_azure_{}_{}".format(resource_group, ssh_user)
        public_key_path = os.path.expanduser("~/.ssh/{}.pub".format(key_name))
        private_key_path = os.path.expanduser("~/.ssh/{}.pem".format(key_name))

    logger.info("Found SSH key pair: %s", key_name)
    with open(public_key_path, "r") as f:
        public_key = f.read()

    config["auth"]["ssh_private_key"] = private_key_path

    for node_type in ["head_node", "worker_nodes"]:
        config[node_type]["azure_arm_parameters"]["adminUsername"] = ssh_user
        config[node_type]["azure_arm_parameters"]["publicKey"] = public_key

    return config

