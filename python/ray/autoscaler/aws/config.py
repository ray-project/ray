from distutils.version import StrictVersion
from functools import lru_cache
from functools import partial
import itertools
import json
import os
import time
import logging

import boto3
from botocore.config import Config
import botocore

from ray.ray_constants import BOTO_MAX_RETRIES
from ray.autoscaler.tags import NODE_TYPE_WORKER, NODE_TYPE_HEAD
from ray.autoscaler.aws.utils import LazyDefaultDict

logger = logging.getLogger(__name__)

RAY = "ray-autoscaler"
DEFAULT_RAY_INSTANCE_PROFILE = RAY + "-v1"
DEFAULT_RAY_IAM_ROLE = RAY + "-v1"
SECURITY_GROUP_TEMPLATE = RAY + "-{}"

# Mapping from the node type tag to the section of the autoscaler yaml that
# contains the config for the node type.
NODE_TYPE_CONFIG_KEYS = {
    NODE_TYPE_WORKER: "worker_nodes",
    NODE_TYPE_HEAD: "head_node",
}

DEFAULT_AMI_NAME = "AWS Deep Learning AMI (Ubuntu 18.04) V26.0"

# Obtained from https://aws.amazon.com/marketplace/pp/B07Y43P7X5 on 4/25/2020.
DEFAULT_AMI = {
    "us-east-1": "ami-0dbb717f493016a1a",  # US East (N. Virginia)
    "us-east-2": "ami-0d6808451e868a339",  # US East (Ohio)
    "us-west-1": "ami-09f2f73141c83d4fe",  # US West (N. California)
    "us-west-2": "ami-008d8ed4bd7dc2485",  # US West (Oregon)
    "ca-central-1": "ami-0142046278ba71f25",  # Canada (Central)
    "eu-central-1": "ami-09633db638556dc39",  # EU (Frankfurt)
    "eu-west-1": "ami-0c265211f000802b0",  # EU (Ireland)
    "eu-west-2": "ami-0f532395ff8544941",  # EU (London)
    "eu-west-3": "ami-03dbbdf69bbb06b29",  # EU (Paris)
    "sa-east-1": "ami-0da2c49fe75e7e5ed",  # SA (Sao Paulo)
}


assert StrictVersion(boto3.__version__) >= StrictVersion("1.4.8"), \
    "Boto3 version >= 1.4.8 required, try `pip install -U boto3`"


def key_pair(i, region, key_name):
    """
    If key_name is not None, key_pair will be named after key_name.
    Returns the ith default (aws_key_pair_name, key_pair_path).
    """
    if i == 0:
        key_pair_name = ("{}_{}".format(RAY, region)
                         if key_name is None else key_name)
        return (key_pair_name,
                os.path.expanduser("~/.ssh/{}.pem".format(key_pair_name)))

    key_pair_name = ("{}_{}_{}".format(RAY, i, region)
                     if key_name is None else key_name + "_key-{}".format(i))
    return (key_pair_name,
            os.path.expanduser("~/.ssh/{}.pem".format(key_pair_name)))


# Suppress excessive connection dropped logs from boto
logging.getLogger("botocore").setLevel(logging.WARNING)


def bootstrap_aws(config):
    # The head node needs to have an IAM role that allows it to create further
    # EC2 instances.
    config = _configure_iam_role(config)

    # Configure SSH access, using an existing key pair if possible.
    config = _configure_key_pair(config)

    # Pick a reasonable subnet if not specified by the user.
    config = _configure_subnet(config)

    # Cluster workers should be in a security group that permits traffic within
    # the group, and also SSH access from outside.
    config = _configure_security_group(config)

    # Provide a helpful message for missing AMI.
    _check_ami(config)

    return config


def _configure_iam_role(config):
    if "IamInstanceProfile" in config["head_node"]:
        return config

    profile = _get_instance_profile(DEFAULT_RAY_INSTANCE_PROFILE, config)

    if profile is None:
        logger.info("_configure_iam_role: "
                    "Creating new instance profile {}".format(
                        DEFAULT_RAY_INSTANCE_PROFILE))
        client = _client("iam", config)
        client.create_instance_profile(
            InstanceProfileName=DEFAULT_RAY_INSTANCE_PROFILE)
        profile = _get_instance_profile(DEFAULT_RAY_INSTANCE_PROFILE, config)
        time.sleep(15)  # wait for propagation

    assert profile is not None, "Failed to create instance profile"

    if not profile.roles:
        role = _get_role(DEFAULT_RAY_IAM_ROLE, config)
        if role is None:
            logger.info("_configure_iam_role: "
                        "Creating new role {}".format(DEFAULT_RAY_IAM_ROLE))
            iam = _resource("iam", config)
            iam.create_role(
                RoleName=DEFAULT_RAY_IAM_ROLE,
                AssumeRolePolicyDocument=json.dumps({
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "ec2.amazonaws.com"
                            },
                            "Action": "sts:AssumeRole",
                        },
                    ],
                }))
            role = _get_role(DEFAULT_RAY_IAM_ROLE, config)
            assert role is not None, "Failed to create role"
        role.attach_policy(
            PolicyArn="arn:aws:iam::aws:policy/AmazonEC2FullAccess")
        role.attach_policy(
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess")
        profile.add_role(RoleName=role.name)
        time.sleep(15)  # wait for propagation

    logger.info("_configure_iam_role: "
                "Role not specified for head node, using {}".format(
                    profile.arn))
    config["head_node"]["IamInstanceProfile"] = {"Arn": profile.arn}

    return config


def _configure_key_pair(config):
    if "ssh_private_key" in config["auth"]:
        assert "KeyName" in config["head_node"]
        assert "KeyName" in config["worker_nodes"]
        return config

    ec2 = _resource("ec2", config)

    # Try a few times to get or create a good key pair.
    MAX_NUM_KEYS = 30
    for i in range(MAX_NUM_KEYS):

        key_name = config["provider"].get("key_pair", {}).get("key_name")

        key_name, key_path = key_pair(i, config["provider"]["region"],
                                      key_name)
        key = _get_key(key_name, config)

        # Found a good key.
        if key and os.path.exists(key_path):
            break

        # We can safely create a new key.
        if not key and not os.path.exists(key_path):
            logger.info("_configure_key_pair: "
                        "Creating new key pair {}".format(key_name))
            key = ec2.create_key_pair(KeyName=key_name)

            # We need to make sure to _create_ the file with the right
            # permissions. In order to do that we need to change the default
            # os.open behavior to include the mode we want.
            with open(key_path, "w", opener=partial(os.open, mode=0o600)) as f:
                f.write(key.key_material)
            break

    if not key:
        raise ValueError(
            "No matching local key file for any of the key pairs in this "
            "account with ids from 0..{}. ".format(key_name) +
            "Consider deleting some unused keys pairs from your account.")

    assert os.path.exists(key_path), \
        "Private key file {} not found for {}".format(key_path, key_name)

    logger.info("_configure_key_pair: "
                "KeyName not specified for nodes, using {}".format(key_name))

    config["auth"]["ssh_private_key"] = key_path
    config["head_node"]["KeyName"] = key_name
    config["worker_nodes"]["KeyName"] = key_name

    return config


def _configure_subnet(config):
    ec2 = _resource("ec2", config)
    use_internal_ips = config["provider"].get("use_internal_ips", False)
    subnets = sorted(
        (s for s in ec2.subnets.all() if s.state == "available" and (
            use_internal_ips or s.map_public_ip_on_launch)),
        reverse=True,  # sort from Z-A
        key=lambda subnet: subnet.availability_zone)
    if not subnets:
        raise Exception(
            "No usable subnets found, try manually creating an instance in "
            "your specified region to populate the list of subnets "
            "and trying this again. Note that the subnet must map public IPs "
            "on instance launch unless you set 'use_internal_ips': True in "
            "the 'provider' config.")
    if "availability_zone" in config["provider"]:
        azs = config["provider"]["availability_zone"].split(",")
        subnets = [s for s in subnets if s.availability_zone in azs]
        if not subnets:
            raise Exception(
                "No usable subnets matching availability zone {} "
                "found. Choose a different availability zone or try "
                "manually creating an instance in your specified region "
                "to populate the list of subnets and trying this again.".
                format(config["provider"]["availability_zone"]))

    subnet_ids = [s.subnet_id for s in subnets]
    subnet_descr = [(s.subnet_id, s.availability_zone) for s in subnets]
    if "SubnetIds" not in config["head_node"]:
        config["head_node"]["SubnetIds"] = subnet_ids
        logger.info("_configure_subnet: "
                    "SubnetIds not specified for head node, using {}".format(
                        subnet_descr))

    if "SubnetIds" not in config["worker_nodes"]:
        config["worker_nodes"]["SubnetIds"] = subnet_ids
        logger.info("_configure_subnet: "
                    "SubnetId not specified for workers,"
                    " using {}".format(subnet_descr))

    return config


def _configure_security_group(config):
    node_types_to_configure = [
        node_type for node_type, config_key in NODE_TYPE_CONFIG_KEYS.items()
        if "SecurityGroupIds" not in config[NODE_TYPE_CONFIG_KEYS[node_type]]
    ]
    if not node_types_to_configure:
        return config  # have user-defined groups

    security_groups = _upsert_security_groups(config, node_types_to_configure)

    if NODE_TYPE_HEAD in node_types_to_configure:
        head_sg = security_groups[NODE_TYPE_HEAD]
        logger.info(
            "_configure_security_group: "
            "SecurityGroupIds not specified for head node, using {} ({})"
            .format(head_sg.group_name, head_sg.id))
        config["head_node"]["SecurityGroupIds"] = [head_sg.id]

    if NODE_TYPE_WORKER in node_types_to_configure:
        workers_sg = security_groups[NODE_TYPE_WORKER]
        logger.info("_configure_security_group: "
                    "SecurityGroupIds not specified for workers, using {} ({})"
                    .format(workers_sg.group_name, workers_sg.id))
        config["worker_nodes"]["SecurityGroupIds"] = [workers_sg.id]

    return config


def _check_ami(config):
    """Provide helpful message for missing ImageId for node configuration."""

    region = config["provider"]["region"]
    default_ami = DEFAULT_AMI.get(region)
    if not default_ami:
        # If we do not provide a default AMI for the given region, noop.
        return

    if config["head_node"].get("ImageId", "").lower() == "latest_dlami":
        config["head_node"]["ImageId"] = default_ami
        logger.info("_check_ami: head node ImageId is 'latest_dlami'. "
                    "Using '{ami_id}', which is the default {ami_name} "
                    "for your region ({region}).".format(
                        ami_id=default_ami,
                        ami_name=DEFAULT_AMI_NAME,
                        region=region))

    if config["worker_nodes"].get("ImageId", "").lower() == "latest_dlami":
        config["worker_nodes"]["ImageId"] = default_ami
        logger.info("_check_ami: worker nodes ImageId is 'latest_dlami'. "
                    "Using '{ami_id}', which is the default {ami_name} "
                    "for your region ({region}).".format(
                        ami_id=default_ami,
                        ami_name=DEFAULT_AMI_NAME,
                        region=region))


def _upsert_security_groups(config, node_types):
    security_groups = _get_or_create_vpc_security_groups(config, node_types)
    _upsert_security_group_rules(config, security_groups)

    return security_groups


def _get_or_create_vpc_security_groups(conf, node_types):
    # Figure out which VPC each node_type is in...
    ec2 = _resource("ec2", conf)
    node_type_to_vpc = {
        node_type: _get_vpc_id_or_die(
            ec2,
            conf[NODE_TYPE_CONFIG_KEYS[node_type]]["SubnetIds"][0],
        )
        for node_type in node_types
    }

    # Generate the name of the security group we're looking for...
    expected_sg_name = SECURITY_GROUP_TEMPLATE.format(conf["cluster_name"])

    # Figure out which security groups with this name exist for each VPC...
    vpc_to_existing_sg = {
        sg.vpc_id: sg
        for sg in _get_security_groups(
            conf,
            node_type_to_vpc.values(),
            [expected_sg_name],
        )
    }

    # Lazily create any security group we're missing for each VPC...
    vpc_to_sg = LazyDefaultDict(
        partial(_create_security_group, conf, group_name=expected_sg_name),
        vpc_to_existing_sg,
    )

    # Then return a mapping from each node_type to its security group...
    return {
        node_type: vpc_to_sg[vpc_id]
        for node_type, vpc_id in node_type_to_vpc.items()
    }


@lru_cache()
def _get_vpc_id_or_die(ec2, subnet_id):
    subnet = list(
        ec2.subnets.filter(Filters=[{
            "Name": "subnet-id",
            "Values": [subnet_id]
        }]))
    assert len(subnet) == 1, "Subnet ID not found: {}".format(subnet_id)
    subnet = subnet[0]
    return subnet.vpc_id


def _get_security_group(config, vpc_id, group_name):
    security_group = _get_security_groups(config, [vpc_id], [group_name])
    return None if not security_group else security_group[0]


def _get_security_groups(config, vpc_ids, group_names):
    unique_vpc_ids = list(set(vpc_ids))
    unique_group_names = set(group_names)

    ec2 = _resource("ec2", config)
    existing_groups = list(
        ec2.security_groups.filter(Filters=[{
            "Name": "vpc-id",
            "Values": unique_vpc_ids
        }]))
    filtered_groups = [
        sg for sg in existing_groups if sg.group_name in unique_group_names
    ]
    return filtered_groups


def _create_security_group(config, vpc_id, group_name):
    client = _client("ec2", config)
    client.create_security_group(
        Description="Auto-created security group for Ray workers",
        GroupName=group_name,
        VpcId=vpc_id)
    security_group = _get_security_group(config, vpc_id, group_name)
    logger.info("_create_security_group: Created new security group {} ({})"
                .format(security_group.group_name, security_group.id))
    assert security_group, "Failed to create security group"
    return security_group


def _upsert_security_group_rules(conf, security_groups):
    sgids = {sg.id for sg in security_groups.values()}
    # sort security group items for deterministic inbound rule config order
    # (mainly supports more precise stub-based boto3 unit testing)
    for node_type, sg in sorted(security_groups.items()):
        sg = security_groups[node_type]
        if not sg.ip_permissions:
            _update_inbound_rules(sg, sgids, conf)


def _update_inbound_rules(target_security_group, sgids, config):
    extended_rules = config["provider"] \
        .get("security_group", {}) \
        .get("IpPermissions", [])
    ip_permissions = _create_default_inbound_rules(sgids, extended_rules)
    target_security_group.authorize_ingress(IpPermissions=ip_permissions)


def _create_default_inbound_rules(sgids, extended_rules=[]):
    intracluster_rules = _create_default_instracluster_inbound_rules(sgids)
    ssh_rules = _create_default_ssh_inbound_rules()
    merged_rules = itertools.chain(
        intracluster_rules,
        ssh_rules,
        extended_rules,
    )
    return list(merged_rules)


def _create_default_instracluster_inbound_rules(intracluster_sgids):
    return [{
        "FromPort": -1,
        "ToPort": -1,
        "IpProtocol": "-1",
        "UserIdGroupPairs": [
            {
                "GroupId": security_group_id
            } for security_group_id in sorted(intracluster_sgids)
            # sort security group IDs for deterministic IpPermission models
            # (mainly supports more precise stub-based boto3 unit testing)
        ]
    }]


def _create_default_ssh_inbound_rules():
    return [{
        "FromPort": 22,
        "ToPort": 22,
        "IpProtocol": "tcp",
        "IpRanges": [{
            "CidrIp": "0.0.0.0/0"
        }]
    }]


def _get_role(role_name, config):
    iam = _resource("iam", config)
    role = iam.Role(role_name)
    try:
        role.load()
        return role
    except botocore.exceptions.ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "NoSuchEntity":
            return None
        else:
            raise exc


def _get_instance_profile(profile_name, config):
    iam = _resource("iam", config)
    profile = iam.InstanceProfile(profile_name)
    try:
        profile.load()
        return profile
    except botocore.exceptions.ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "NoSuchEntity":
            return None
        else:
            raise exc


def _get_key(key_name, config):
    ec2 = _resource("ec2", config)
    for key in ec2.key_pairs.filter(Filters=[{
            "Name": "key-name",
            "Values": [key_name]
    }]):
        if key.name == key_name:
            return key


def _client(name, config):
    return _resource(name, config).meta.client


def _resource(name, config):
    region = config["provider"]["region"]
    aws_credentials = config["provider"].get("aws_credentials", {})
    return _resource_cache(name, region, **aws_credentials)


@lru_cache()
def _resource_cache(name, region, **kwargs):
    boto_config = Config(retries={"max_attempts": BOTO_MAX_RETRIES})
    return boto3.resource(
        name,
        region,
        config=boto_config,
        **kwargs,
    )
