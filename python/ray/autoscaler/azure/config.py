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
    config = _configure_resource_group(config)
    config = _configure_msi_user(config)
    config = _configure_key_pair(config)
    config = _configure_network(config)
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

    return config


def _configure_msi_user(config):
    msi_client = _get_client(ManagedServiceIdentityClient, config)
    resource_client = _get_client(ResourceManagementClient, config)
    auth_client = _get_client(AuthorizationManagementClient, config)

    resource_group = config["provider"]["resource_group"]
    location = config["provider"]["location"]

    resource_group_id = resource_client.resource_groups.get(resource_group).id
    try:
        identity = msi_client.user_assigned_identities.list_by_resource_group(
            resource_group_name=resource_group,
            filter="name eq '{}'".format(MSI_NAME)).next()
        logger.info("Found MSI User Assigned Identity: %s", MSI_NAME)
    except StopIteration:
        logger.info("Creating MSI User Assigned Identity: %s", MSI_NAME)
        identity = msi_client.user_assigned_identities.create_or_update(
            resource_group_name=resource_group,
            resource_name=MSI_NAME,
            location=location)

    identity_id = identity.id
    principal_id = identity.principal_id
    config["provider"]["msi_identity_id"] = identity_id
    config["provider"]["msi_identity_principal_id"] = principal_id

    # assign Contributor role for MSI User Identity to resource group
    role_id = auth_client.role_definitions.list(
        scope=resource_group_id, filter="roleName eq 'Contributor'").next().id
    role_params = {"role_definition_id": role_id, "principal_id": principal_id}

    for _ in range(RETRIES):
        try:
            filter_expr = "principalId eq '{}'".format(principal_id)
            assignments = auth_client.role_assignments.list_for_scope(
                scope=resource_group_id, filter=filter_expr)

            if any(a.role_definition_id == role_id for a in assignments):
                break

            auth_client.role_assignments.create(
                scope=resource_group_id,
                role_assignment_name=uuid.uuid4(),
                parameters=role_params)
            logger.info("Assigning Contributor Role to MSI User")
        except CloudError as ce:
            if ce.inner_exception.error == "PrincipalNotFound":
                time.sleep(5)
    else:
        raise Exception(
            "Failed to create contributor role assignment (timeout)")

    return config


def _configure_key_pair(config):
    ssh_user = config["auth"]["ssh_user"]

    for key_type in ["ssh_private_key", "ssh_public_key"]:
        try:
            key_path = os.path.expanduser(config["auth"][key_type])
        except KeyError:
            raise Exception("Config must define {}".format(key_type))
        except TypeError:
            raise Exception("Invalid config value for {}".format(key_type))

        assert os.path.exists(key_path), (
            "Could not find ssh key: {}".format(key_path))

        if key_type == "ssh_public_key":
            with open(key_path, "r") as f:
                public_key = f.read()

    os_profile = {
        "admin_username": ssh_user,
        "computer_name": None,
        "linux_configuration": {
            "disable_password_authentication": True,
            "ssh": {
                "public_keys": [{
                    "key_data": public_key,
                    "path": "/home/{}/.ssh/authorized_keys".format(ssh_user)
                }]
            }
        }
    }
    for node_type in ["head_node", "worker_nodes"]:
        config[node_type]["os_profile"] = os_profile

    return config


def _configure_network(config):
    # skip this if subnet is manually set in configuration yaml
    if "subnet_id" in config["provider"]:
        return config

    location = config["provider"]["location"]
    resource_group = config["provider"]["resource_group"]
    network_client = _get_client(NetworkManagementClient, config)

    vnets = []
    for _ in range(RETRIES):
        try:
            vnets = list(
                network_client.virtual_networks.list(
                    resource_group_name=resource_group,
                    filter="name eq '{}'".format(VNET_NAME)))
            break
        except CloudError:
            time.sleep(1)
        except AuthenticationError:
            # wait for service principal authorization to populate
            time.sleep(1)

    # can't update vnet if subnet already exists
    if not vnets:
        # create vnet
        logger.info("Creating/Updating VNet: %s", VNET_NAME)
        vnet_params = {
            "location": location,
            "address_space": {
                "address_prefixes": ["10.0.0.0/16"]
            }
        }
        network_client.virtual_networks.create_or_update(
            resource_group_name=resource_group,
            virtual_network_name=VNET_NAME,
            parameters=vnet_params).wait()

    # create subnet
    logger.info("Creating/Updating Subnet: %s", SUBNET_NAME)
    subnet_params = {"address_prefix": "10.0.0.0/24"}
    subnet = network_client.subnets.create_or_update(
        resource_group_name=resource_group,
        virtual_network_name=VNET_NAME,
        subnet_name=SUBNET_NAME,
        subnet_parameters=subnet_params).result()

    config["provider"]["subnet_id"] = subnet.id

    # create network security group
    logger.info("Creating/Updating Network Security Group: %s", NSG_NAME)
    nsg_params = {
        "location": location,
        "security_rules": [{
            "protocol": "Tcp",
            "source_port_range": "*",
            "source_address_prefix": "*",
            "destination_port_range": "22",
            "destination_address_prefix": "*",
            "access": "Allow",
            "priority": 300,
            "direction": "Inbound",
            "name": "ssh_rule"
        }]
    }
    network_client.network_security_groups.create_or_update(
        resource_group_name=resource_group,
        network_security_group_name=NSG_NAME,
        parameters=nsg_params).wait()

    return config
