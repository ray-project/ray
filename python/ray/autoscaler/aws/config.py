from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import os
import time

import boto3

RAY = "ray-autoscaler"
DEFAULT_RAY_INSTANCE_PROFILE = RAY
DEFAULT_RAY_IAM_ROLE = RAY
DEFAULT_RAY_KEY_PAIR = RAY
DEFAULT_RAY_KEY_PAIR_PATH = os.path.expanduser("~/.ssh/{}.pem".format(RAY))
SECURITY_GROUP_TEMPLATE = RAY + "-{}"

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

    return config


def _configure_iam_role(config):
    if "IamInstanceProfile" in config["head_node"]:
        return config

    profile = _get_instance_profile(DEFAULT_RAY_INSTANCE_PROFILE, config)

    if profile is None:
        print("Creating new instance profile {}".format(
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
            print("Creating new role {}".format(DEFAULT_RAY_IAM_ROLE))
            iam = _resource("iam", config)
            iam.create_role(
                RoleName=DEFAULT_RAY_IAM_ROLE,
                AssumeRolePolicyDocument=json.dumps({
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "ec2.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        },
                    ],
                }))
            role = _get_role(DEFAULT_RAY_IAM_ROLE, config)
            assert role is not None, "Failed to create role"
        role.attach_policy(
            PolicyArn="arn:aws:iam::aws:policy/AmazonEC2FullAccess")
        profile.add_role(RoleName=role.name)
        time.sleep(15)  # wait for propagation

    print("Role not specified for head node, using {}".format(
        profile.arn))
    config["head_node"]["IamInstanceProfile"] = {"Arn": profile.arn}

    return config


def _configure_key_pair(config):
    if "ssh_private_key" in config["auth"]:
        assert "KeyName" in config["head_node"]
        assert "KeyName" in config["worker_nodes"]
        return config

    ec2 = _resource("ec2", config)
    key = _get_key(DEFAULT_RAY_KEY_PAIR, config)
    if not key:
        assert not os.path.exists(DEFAULT_RAY_KEY_PAIR_PATH), \
            "Private key file {} already exists, not overwriting.".format(
                DEFAULT_RAY_KEY_PAIR_PATH)
        print("Creating new key pair {}".format(DEFAULT_RAY_KEY_PAIR))
        key = ec2.create_key_pair(KeyName=DEFAULT_RAY_KEY_PAIR)
        with open(DEFAULT_RAY_KEY_PAIR_PATH, "w") as f:
            f.write(key.key_material)
        os.chmod(DEFAULT_RAY_KEY_PAIR_PATH, 0o600)

    assert os.path.exists(DEFAULT_RAY_KEY_PAIR_PATH), \
        "Private key file {} not found for {}".format(
            DEFAULT_RAY_KEY_PAIR_PATH, DEFAULT_RAY_KEY_PAIR)

    print("KeyName not specified for nodes, using {}".format(
        DEFAULT_RAY_KEY_PAIR))

    config["auth"]["ssh_private_key"] = DEFAULT_RAY_KEY_PAIR_PATH
    config["head_node"]["KeyName"] = DEFAULT_RAY_KEY_PAIR
    config["worker_nodes"]["KeyName"] = DEFAULT_RAY_KEY_PAIR

    return config


def _configure_subnet(config):
    ec2 = _resource("ec2", config)
    subnets = sorted(
        [s for s in ec2.subnets.all() if s.state == "available"],
        reverse=True,  # sort from Z-A
        key=lambda subnet: subnet.availability_zone)
    default_subnet = subnets[0]

    if "SubnetId" not in config["head_node"]:
        config["head_node"]["SubnetId"] = default_subnet.id
        print("SubnetId not specified for head node, using {} in {}".format(
            default_subnet.id, default_subnet.availability_zone))

    if "SubnetId" not in config["worker_nodes"]:
        config["worker_nodes"]["SubnetId"] = default_subnet.id
        print("SubnetId not specified for workers, using {} in {}".format(
            default_subnet.id, default_subnet.availability_zone))

    return config


def _configure_security_group(config):
    if "SecurityGroupIds" in config["head_node"] and \
            "SecurityGroupIds" in config["worker_nodes"]:
        return config  # have user-defined groups

    group_name = SECURITY_GROUP_TEMPLATE.format(config["cluster_name"])
    subnet = _get_subnet_or_die(config, config["worker_nodes"]["SubnetId"])
    security_group = _get_security_group(config, subnet.vpc_id, group_name)

    if security_group is None:
        print("Creating new security group {}".format(group_name))
        client = _client("ec2", config)
        client.create_security_group(
            Description="Auto-created security group for Ray workers",
            GroupName=group_name,
            VpcId=subnet.vpc_id)
        security_group = _get_security_group(config, subnet.vpc_id, group_name)
        assert security_group, "Failed to create security group"

    if not security_group.ip_permissions:
        security_group.authorize_ingress(
            IpPermissions=[
                {"FromPort": -1, "ToPort": -1, "IpProtocol": "-1",
                 "UserIdGroupPairs": [{"GroupId": security_group.id}]},
                {"FromPort": 22, "ToPort": 22, "IpProtocol": "TCP",
                 "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}])

    if "SecurityGroupIds" not in config["head_node"]:
        print("SecurityGroupIds not specified for head node, using {}".format(
            security_group.group_name))
        config["head_node"]["SecurityGroupIds"] = [security_group.id]

    if "SecurityGroupIds" not in config["worker_nodes"]:
        print("SecurityGroupIds not specified for workers, using {}".format(
            security_group.group_name))
        config["worker_nodes"]["SecurityGroupIds"] = [security_group.id]

    return config


def _get_subnet_or_die(config, subnet_id):
    ec2 = _resource("ec2", config)
    subnet = list(
        ec2.subnets.filter(Filters=[
            {"Name": "subnet-id", "Values": [subnet_id]}]))
    assert len(subnet) == 1, "Subnet not found"
    subnet = subnet[0]
    return subnet


def _get_security_group(config, vpc_id, group_name):
    ec2 = _resource("ec2", config)
    existing_groups = list(
        ec2.security_groups.filter(Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]}]))
    for sg in existing_groups:
        if sg.group_name == group_name:
            return sg


def _get_role(role_name, config):
    iam = _resource("iam", config)
    role = iam.Role(role_name)
    try:
        role.load()
        return role
    except Exception:
        return None


def _get_instance_profile(profile_name, config):
    iam = _resource("iam", config)
    profile = iam.InstanceProfile(profile_name)
    try:
        profile.load()
        return profile
    except Exception:
        return None


def _get_key(key_name, config):
    ec2 = _resource("ec2", config)
    for key in ec2.key_pairs.filter(
            Filters=[{"Name": "key-name", "Values": [key_name]}]):
        if key.name == key_name:
            return key


def _client(name, config):
    return boto3.client(name, config["provider"]["region"])


def _resource(name, config):
    return boto3.resource(name, config["provider"]["region"])
