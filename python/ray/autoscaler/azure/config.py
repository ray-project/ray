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
import paramiko
from ray.autoscaler.tags import TAG_RAY_NODE_NAME

RAY = "ray-autoscaler"
PASSWORD_MIN_LENGTH = 16
RETRIES = 10
SUBNET_NAME = "ray-subnet"
NSG_NAME = "ray-nsg"
VNET_NAME = "ray-vnet"
AUTH_ENDPOINTS = {
    "activeDirectoryEndpointUrl": "https://login.microsoftonline.com",
    "resourceManagerEndpointUrl": "https://management.azure.com/",
    "activeDirectoryGraphResourceId": "https://graph.windows.net/",
    "sqlManagementEndpointUrl": "https://management.core.windows.net:8443/",
    "galleryEndpointUrl": "https://gallery.azure.com/",
    "managementEndpointUrl": "https://management.core.windows.net/"
}
DEFAULT_NODE_CONFIG = {
    "hardware_profile": {
        "vm_size": "Standard_D2s_v3"
    },
    "storage_profile": {
        "os_disk": {
            "create_option": "FromImage",
            "caching": "ReadWrite"
        },
        "image_reference": {
            "publisher": "microsoft-dsvm",
            "offer": "linux-data-science-vm-ubuntu",
            "sku": "linuxdsvmubuntu",
            "version": "latest"
        }
    },
    "os_profile": {
        "admin_username": "ubuntu",
        "computer_name": TAG_RAY_NODE_NAME,
        "linux_configuration": {
            "disable_password_authentication": True,
            "ssh": {
                "public_keys": None  # populated by _configure_key_pair
            }
        }
    }
}

logger = logging.getLogger(__name__)


def bootstrap_azure(config):
    config = _configure_resource_group(config)
    config = _configure_msi_user(config)
    config = _configure_key_pair(config)
    config = _configure_network(config)
    config = _configure_nodes(config)
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

    logger.info("Creating resource group: %s", resource_group)
    resource_client.resource_groups.create_or_update(
        resource_group_name=resource_group, parameters=params)

    return config


def _configure_msi_user(config):
    msi_client = _get_client(ManagedServiceIdentityClient, config)
    resource_client = _get_client(ResourceManagementClient, config)
    auth_client = _get_client(AuthorizationManagementClient, config)

    resource_group = config["provider"]["resource_group"]
    location = config["provider"]["location"]

    logger.info("Creating MSI user assigned identity")

    resource_group_id = resource_client.resource_groups.get(resource_group).id

    identities = list(
        msi_client.user_assigned_identities.list_by_resource_group(
            resource_group))
    if len(identities) > 0:
        identity = identities[0]
    else:
        identity = msi_client.user_assigned_identities.create_or_update(
            resource_group,
            str(uuid.uuid4()),  # Any name, just a human readable ID
            location)

    identity_id = identity.id
    principal_id = identity.principal_id
    config["provider"]["msi_identity_id"] = identity_id
    config["provider"]["msi_identity_principal_id"] = principal_id

    def assign_role():
        for _ in range(RETRIES):
            try:
                role_id = auth_client.role_definitions.list(
                    resource_group_id,
                    filter="roleName eq 'Contributor'").next().id
                role_params = {
                    "role_definition_id": role_id,
                    "principal_id": principal_id
                }

                filter_expr = "principalId eq '{}'".format(principal_id)
                assignments = auth_client.role_assignments.list_for_scope(
                    resource_group_id, filter=filter_expr)

                for assignment in assignments:
                    if assignment.role_definition_id == role_id:
                        return

                auth_client.role_assignments.create(
                    scope=resource_group_id,
                    role_assignment_name=uuid.uuid4(),
                    parameters=role_params)
                logger.info("Creating contributor role assignment")
                return
            except CloudError as ce:
                if str(ce.error).startswith("Azure Error: PrincipalNotFound"):
                    time.sleep(3)
                else:
                    raise Exception(
                        "Failed to create contributor role assignment")

            logger.info("Creating contributor role assignment")
            auth_client.role_assignments.create(
                scope=resource_group_id,
                role_assignment_name=uuid.uuid4(),
                parameters=role_params)
            break

    assign_role()
    return config


def _configure_key_pair(config):
    # skip key generation if it is manually specified
    ssh_private_key = config["auth"].get("ssh_private_key")
    if ssh_private_key:
        assert os.path.exists(ssh_private_key)
        # make sure public key configuration also exists
        for node_type in ["head_node", "worker_nodes"]:
            os_profile = config[node_type]["os_profile"]
            assert os_profile["linux_configuration"]["ssh"]["public_keys"]
        return config

    location = config["provider"]["location"]
    resource_group = config["provider"]["resource_group"]
    ssh_user = config["auth"]["ssh_user"]

    # look for an existing key pair
    key_name = "{}_azure_{}_{}".format(RAY, location, resource_group, ssh_user)
    public_key_path = os.path.expanduser("~/.ssh/{}.pub".format(key_name))
    private_key_path = os.path.expanduser("~/.ssh/{}.pem".format(key_name))
    if os.path.exists(public_key_path) and os.path.exists(private_key_path):
        logger.info("SSH key pair found: %s", key_name)
        with open(public_key_path, "r") as f:
            public_key = f.read()
    else:
        public_key, private_key_path = _generate_ssh_keys(key_name)
        logger.info("SSH key pair created: %s", key_name)

    config["auth"]["ssh_private_key"] = private_key_path

    public_keys = [{
        "key_data": public_key,
        "path": "/home/{}/.ssh/authorized_keys".format(
            config["auth"]["ssh_user"])
    }]
    for node_type in ["head_node", "worker_nodes"]:
        os_config = DEFAULT_NODE_CONFIG["os_profile"].copy()
        os_config["linux_configuration"]["ssh"]["public_keys"] = public_keys
        config_type = config.get(node_type, {})
        config_type.update({"os_profile": os_config})
        config[node_type] = config_type

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
        logger.info("Creating VNet: %s", VNET_NAME)
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
    logger.info("Creating Subnet: %s", SUBNET_NAME)
    subnet_params = {"address_prefix": "10.0.0.0/24"}
    subnet = network_client.subnets.create_or_update(
        resource_group_name=resource_group,
        virtual_network_name=VNET_NAME,
        subnet_name=SUBNET_NAME,
        subnet_parameters=subnet_params).result()

    config["provider"]["subnet_id"] = subnet.id

    # create network security group
    logger.info("Creating Network Security Group: %s", NSG_NAME)
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
        parameters=nsg_params)

    return config


def _generate_ssh_keys(key_name):
    """Generate and store public and private keys"""
    public_key_path = os.path.expanduser("~/.ssh/{}.pub".format(key_name))
    private_key_path = os.path.expanduser("~/.ssh/{}.pem".format(key_name))

    ssh_dir, _ = os.path.split(private_key_path)
    if not os.path.exists(ssh_dir):
        os.makedirs(ssh_dir)
        os.chmod(ssh_dir, 0o700)

    key = paramiko.RSAKey.generate(2048)
    key.write_private_key_file(private_key_path)
    os.chmod(private_key_path, 0o600)

    with open(public_key_path, "w") as public_key_file:
        # TODO: check if this is the necessary format
        public_key = "%s %s" % (key.get_name(), key.get_base64())
        public_key_file.write(public_key)
    os.chmod(public_key_path, 0o644)

    return public_key, private_key_path


def _configure_nodes(config):
    """Add default node configuration if not provided"""
    for node_type in ["head_node", "worker_nodes"]:
        node_config = DEFAULT_NODE_CONFIG.copy()
        node_config.update(config.get(node_type, {}))
        config[node_type] = node_config
    return config
