from distutils.version import StrictVersion
import json
import os
import time
import logging

import boto3
from botocore.config import Config
import botocore

from ray.ray_constants import BOTO_MAX_RETRIES

logger = logging.getLogger(__name__)

RAY = "ray-autoscaler"
DEFAULT_RAY_INSTANCE_PROFILE = RAY + "-v1"
DEFAULT_RAY_IAM_ROLE = RAY + "-v1"
SECURITY_GROUP_TEMPLATE = RAY + "-{}"

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
            with open(key_path, "w") as f:
                f.write(key.key_material)
            os.chmod(key_path, 0o600)
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
    if "SecurityGroupIds" in config["head_node"] and \
            "SecurityGroupIds" in config["worker_nodes"]:
        return config  # have user-defined groups

    group_name = SECURITY_GROUP_TEMPLATE.format(config["cluster_name"])
    vpc_id = _get_vpc_id_or_die(config, config["worker_nodes"]["SubnetIds"][0])
    security_group = _get_security_group(config, vpc_id, group_name)

    if security_group is None:
        logger.info("_configure_security_group: "
                    "Creating new security group {}".format(group_name))
        client = _client("ec2", config)
        client.create_security_group(
            Description="Auto-created security group for Ray workers",
            GroupName=group_name,
            VpcId=vpc_id)
        security_group = _get_security_group(config, vpc_id, group_name)
        assert security_group, "Failed to create security group"

    if not security_group.ip_permissions:
        IpPermissions = [{
            "FromPort": -1,
            "ToPort": -1,
            "IpProtocol": "-1",
            "UserIdGroupPairs": [{
                "GroupId": security_group.id
            }]
        }, {
            "FromPort": 22,
            "ToPort": 22,
            "IpProtocol": "TCP",
            "IpRanges": [{
                "CidrIp": "0.0.0.0/0"
            }]
        }]

        additional_IpPermissions = config["provider"].get(
            "security_group", {}).get("IpPermissions", [])
        IpPermissions.extend(additional_IpPermissions)

        security_group.authorize_ingress(IpPermissions=IpPermissions)

    if "SecurityGroupIds" not in config["head_node"]:
        logger.info(
            "_configure_security_group: "
            "SecurityGroupIds not specified for head node, using {}".format(
                security_group.group_name))
        config["head_node"]["SecurityGroupIds"] = [security_group.id]

    if "SecurityGroupIds" not in config["worker_nodes"]:
        logger.info(
            "_configure_security_group: "
            "SecurityGroupIds not specified for workers, using {}".format(
                security_group.group_name))
        config["worker_nodes"]["SecurityGroupIds"] = [security_group.id]

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


def _get_vpc_id_or_die(config, subnet_id):
    ec2 = _resource("ec2", config)
    subnet = list(
        ec2.subnets.filter(Filters=[{
            "Name": "subnet-id",
            "Values": [subnet_id]
        }]))
    assert len(subnet) == 1, "Subnet not found"
    subnet = subnet[0]
    return subnet.vpc_id


def _get_security_group(config, vpc_id, group_name):
    ec2 = _resource("ec2", config)
    existing_groups = list(
        ec2.security_groups.filter(Filters=[{
            "Name": "vpc-id",
            "Values": [vpc_id]
        }]))
    for sg in existing_groups:
        if sg.group_name == group_name:
            return sg


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
    boto_config = Config(retries={"max_attempts": BOTO_MAX_RETRIES})
    aws_credentials = config["provider"].get("aws_credentials", {})
    return boto3.client(
        name,
        config["provider"]["region"],
        config=boto_config,
        **aws_credentials)


def _resource(name, config):
    boto_config = Config(retries={"max_attempts": BOTO_MAX_RETRIES})
    aws_credentials = config["provider"].get("aws_credentials", {})
    return boto3.resource(
        name,
        config["provider"]["region"],
        config=boto_config,
        **aws_credentials)
