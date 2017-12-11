from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json

import boto3

DEFAULT_RAY_INSTANCE_PROFILE = "ray-autoscaler"
DEFAULT_RAY_IAM_ROLE = "ray-autoscaler"
SECURITY_GROUP_TEMPLATE = "ray-autoscaler-{}"


def bootstrap_aws(config):
    # The head node needs to have an IAM role that allows it to create further
    # EC2 instances.
    config = _configure_iam_role(config)

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

    config["head_node"]["IamInstanceProfile"] = {"Arn": profile.arn}

    return config


def _configure_subnet(config):
    ec2 = _resource("ec2", config)
    subnets = sorted(
        list(ec2.subnets.all()),
        key=lambda subnet:
            (subnet.state != "available", subnet.availability_zone))
    default_subnet = subnets[0]

    if "SubnetId" not in config["head_node"]:
        config["head_node"]["SubnetId"] = default_subnet.id
        print("SubnetId not specified for head node, using {} in {}".format(
            default_subnet.id, default_subnet.availability_zone))

    if "SubnetId" not in config["node"]:
        config["node"]["SubnetId"] = default_subnet.id
        print("SubnetId not specified for workers, using {} in {}".format(
            default_subnet.id, default_subnet.availability_zone))

    return config


def _configure_security_group(config):
    if "SecurityGroupIds" in config["head_node"] and \
            "SecurityGroupIds" in config["node"]:
        return config  # have user-defined groups

    group_name = SECURITY_GROUP_TEMPLATE.format(config["worker_group"])
    subnet = _get_subnet_or_die(config, config["node"]["SubnetId"])
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

    if "SecurityGroupIds" not in config["node"]:
        print("SecurityGroupIds not specified for workers, using {}".format(
            security_group.group_name))
        config["node"]["SecurityGroupIds"] = [security_group.id]

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


def _get_role(DEFAULT_RAY_IAM_ROLE, config):
    iam = _resource("iam", config)
    role = iam.Role(DEFAULT_RAY_IAM_ROLE)
    try:
        role.load()
        return role
    except Exception:
        return None


def _get_instance_profile(DEFAULT_RAY_INSTANCE_PROFILE, config):
    iam = _resource("iam", config)
    profile = iam.InstanceProfile(DEFAULT_RAY_INSTANCE_PROFILE)
    try:
        profile.load()
        return profile
    except Exception:
        return None


def _client(name, config):
    return boto3.client(name, config["provider"]["region"])


def _resource(name, config):
    return boto3.resource(name, config["provider"]["region"])
