import datetime
import logging
import os
import uuid

from azure.common.client_factory import get_client_from_cli_profile
from azure.common.credentials import ServicePrincipalCredentials
from azure.graphrbac import GraphRbacManagementClient
from azure.graphrbac.models import PasswordCredential
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
import paramiko


RAY = "ray-autoscaler"
INSTANCE_NAME_MAX_LEN = 64
INSTANCE_NAME_UUID_LEN = 8
SSH_KEYS_MAX_COUNT = 10
IP_CONFIG_NAME = 'ray-ip-config'
SUBNET_NAME = 'ray-subnet'
NIC_NAME = 'ray-nic'
VNET_NAME = 'ray-vnet'

logger = logging.getLogger(__name__)


def bootstrap_azure(config):
    config = _configure_resource_group(config)
    config = _configure_service_principal(config)
    config = _configure_key_pair(config)
    config = _configure_nic(config)
    return config


def _configure_resource_group(config):
    kwargs = dict()
    if 'subscription_id' in config['provider']:
        kwargs['subscription_id'] = config['provider']['subscription_id']
    resource_client = get_client_from_cli_profile(client_class=ResourceManagementClient, **kwargs)
    logger.info("Using subscription id: %s", resource_client.config.subscription_id)
    config['provider']['subscription_id'] = resource_client.config.subscription_id

    resource_group_name = config['provider']['resource_group']
    logger.info("Creating resource group: %s", resource_group_name)
    resource_client.resource_groups.create_or_update(resource_group_name=resource_group_name).wait()

    return config


# Modeled after create_service_principal_for_rbac in
#  https://github.com/Azure/azure-cli/blob/dev/src/azure-cli/azure/cli/command_modules/role/custom.py
def _configure_service_principal(config):
    graph_client = get_client_from_cli_profile(GraphRbacManagementClient)

    sp_name = config['provider']['service_principal']
    if '://' not in sp_name:
        app_name = sp_name
        sp_name = 'http://' + sp_name
    else:
        app_name = sp_name.split('://', 1)[-1]

    password = os.environ.get('AZURE_CLIENT_SECRET')
    if password is None:
        logger.info('AZURE_CLIENT_SECRET environment variable not set, generating new password')
        password = uuid.uuid4().hex

    try:
        # find existing application
        app = graph_client.applications.list(filter="displayName eq '{}'".format(app_name)).next()
        logger.info("Found Application: %s", app_name)
    except StopIteration:
        # create new application
        logger.info("Creating Application: %s", app_name)
        app_start_date = datetime.datetime.utcnow()
        app_end_date = app_start_date.replace(year=app_start_date.year + 1)

        password_creds = PasswordCredential(start_date=app_start_date,
                                            end_date=app_end_date,
                                            key_id=uuid.uuid4().hex,
                                            value=password)

        app_params = dict(display_name=app_name,
                          identifier_uris=[sp_name],
                          password_credentials=password_creds)
        app = graph_client.applications.create(parameters=app_params)

    try:
        query_exp = "servicePrincipalNames/any(x:x eq '{}')".format(sp_name)
        sp = graph_client.service_principals.list(filter=query_exp).next()
        logger.info("Found Service Principal: %s", sp_name)
    except StopIteration:
        # create new service principal
        logger.info("Creating Service Principal: %s", sp_name)
        sp_params = dict(app_id=app.app_id)
        sp = graph_client.service_principals.create(parameters=sp_params)

        # TODO: set role / scope for service principal

    config['provider']['client_id'] = sp.app_id
    config['provider']['secret'] = password
    config['provider']['tenant'] = graph_client.config.tenant_id

    return config


def _configure_key_pair(config):
    # skip key generation if it is manually specified
    ssh_private_key = config['auth'].get('ssh_private_key')
    if ssh_private_key:
        assert os.path.exists(ssh_private_key)
        return config

    provider = config['provider']['type']
    location = config['provider']['location']
    resource_group = config['provider']['resource_group']
    ssh_user = config['auth']['ssh_user']

    # look for an existing key pair
    key_name = "{}_{}_{}_{}".format(RAY, provider, location, resource_group, ssh_user)
    public_key_path = os.path.expanduser("~/.ssh/{}.pub".format(key_name))
    private_key_path = os.path.expanduser("~/.ssh/{}.pem".format(key_name))
    if os.path.exists(public_key_path) and os.path.exists(private_key_path):
        logger.info('SSH key pair found: %s', key_name)
        with open(public_key_path, 'r') as f:
            public_key = f.read()
    else:
        public_key, public_key_path, private_key_path = generate_ssh_keys(key_name)
        logger.info('SSH key pair created: %s', key_name)

    config["auth"]["ssh_private_key"] = private_key_path
    config['provider']['ssh_public_key_data'] = public_key
    config['provider']['ssh_public_key_path'] = public_key_path

    return config


def _configure_nic(config):
    # skip this if nic is manually set in configuration yaml
    head_node_nic = config['head_node'].get('network_profile', {}).get('network_interfaces', [])
    worker_node_nic = config['worker_node'].get('network_profile', {}).get('network_interfaces', [])
    if head_node_nic and worker_node_nic:
        return config

    location = config['provider']['location']
    resource_group = config['provider']['resource_group']
    credentials = ServicePrincipalCredentials(client_id=config['provider']['client_id'],
                                              secret=config['provider']['secret'],
                                              tenant=config['provider']['tenant'])
    network_client = NetworkManagementClient(credentials=credentials,
                                             subscription_id=config['provider']['subscription_id'])

    # create VNet
    logger.info('Creating VNet: %s', VNET_NAME)
    vnet_params = dict(location=location,
                       address_space=dict(address_prefixes=['10.0.0.0/16']))
    network_client.virtual_networks.create_or_update(resource_group_name=resource_group,
                                                     virtual_network_name=VNET_NAME,
                                                     parameters=vnet_params).wait()

    # create Subnet
    logger.info('Creating Subnet: %s', SUBNET_NAME)
    subnet_params = dict(address_prefix='10.0.0.0/24')
    subnet = network_client.subnets.create_or_update(resource_group_name=resource_group,
                                                     virtual_network_name=VNET_NAME,
                                                     subnet_name=SUBNET_NAME,
                                                     subnet_parameters=subnet_params).result()

    # create NIC
    logger.info('Creating NIC: %s', NIC_NAME)
    nic_params = dict(location=location,
                      ip_configuration=[dict(name=IP_CONFIG_NAME, subnet=dict(id=subnet.id))])
    nic = network_client.network_interfaces.create_or_update(resource_group_name=resource_group,
                                                             network_interface_name=NIC_NAME,
                                                             parameters=nic_params).result()

    config['head_node']['network_profile'] = dict(network_interfaces=[dict(id=nic.id)])
    config['worker_node']['network_profile'] = dict(network_interfaces=[dict(id=nic.id)])

    return config


def generate_ssh_keys(key_name):
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

    with open(public_key_path, 'w') as public_key_file:
        public_key = '%s %s' % (key.get_name(), key.get_base64())
        public_key_file.write(public_key)
    os.chmod(public_key_path, 0o644)

    return private_key_path