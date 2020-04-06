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

# Obtained from https://aws.amazon.com/marketplace/pp/B07Y43P7X5 on 1/20/2020.
DEFAULT_AMI = {
    "us-east-2": "ami-0bb99846db2df6e38",  # US East (Ohio)
    "us-east-1": "ami-0698bcaf8bd9ef56d",  # US East (N. Virginia)
    "us-west-1": "ami-074c29e29c500f623",  # US West (N. California)
    "us-west-2": "ami-010a96c958f9ee5cf",  # US West (Oregon)
    "ca-central-1": "ami-086b864eabf2da9b1",  # Canada (Central)
    "eu-central-1": "ami-0dcdcc4bc9e75005f",  # EU (Frankfurt)
    "eu-west-1": "ami-071e6d171b20431fb",  # EU (Ireland)
    "eu-west-2": "ami-0470e741c969b62fc",  # EU (London)
    "eu-west-3": "ami-064f884d98b90a453",  # EU (Paris)
    "sa-east-1": "ami-054e94fd9b444491d",  # SA (Sao Paulo)
}

FROM_PORT = 0
TO_PORT = 1
IP_PROTOCOL = 2

ALL_TRAFFIC_CHANNEL = (-1, -1, "-1")
SSH_CHANNEL = (22, 22, "tcp")
DEFAULT_INBOUND_CHANNELS = [SSH_CHANNEL]

RESOURCE_CACHE = {}


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

    security_groups = _upsert_security_groups(config)
    head_sg = security_groups["head"]
    workers_sg = security_groups["workers"]

    if "SecurityGroupIds" not in config["head_node"]:
        logger.info(
            "_configure_security_group: "
            "SecurityGroupIds not specified for head node, using {} ({})"
            .format(head_sg.group_name, head_sg.id))
        config["head_node"]["SecurityGroupIds"] = [head_sg.id]

    if "SecurityGroupIds" not in config["worker_nodes"]:
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


def _upsert_security_groups(config):
    vpc_group_name = SECURITY_GROUP_TEMPLATE.format(config["cluster_name"])
    rules_group_name = vpc_group_name + "-aux"

    security_groups = _get_or_create_vpc_security_groups(
        config, {vpc_group_name, rules_group_name}, vpc_group_name)
    _upsert_security_group_rules(config, security_groups, rules_group_name)

    return security_groups


def _get_or_create_vpc_security_groups(conf, all_group_names, vpc_group_name):
    worker_subnet_id = conf["worker_nodes"]["SubnetIds"][0]
    head_subnet_id = conf["head_node"]["SubnetIds"][0]

    worker_vpc_id = _get_vpc_id_or_die(conf, worker_subnet_id)
    head_vpc_id = _get_vpc_id_or_die(conf, head_subnet_id) \
        if head_subnet_id != worker_subnet_id else worker_vpc_id

    vpc_ids = [worker_vpc_id, head_vpc_id] \
        if head_vpc_id != worker_vpc_id else [worker_vpc_id]
    all_sgs = _get_security_groups(conf, vpc_ids, all_group_names)

    worker_sg = next((_ for _ in all_sgs if _.vpc_id == worker_vpc_id), None)
    if worker_sg is None:
        worker_sg = _create_security_group(conf, worker_vpc_id, vpc_group_name)
        all_sgs.append(worker_sg)

    head_sg = next((_ for _ in all_sgs if _.vpc_id == head_vpc_id), None)
    if head_sg is None:
        head_sg = _create_security_group(conf, head_vpc_id, vpc_group_name)

    return {"head": head_sg, "workers": worker_sg}


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


def _get_or_create_security_group(config, vpc_id, group_name):
    return _get_security_group(config, vpc_id, group_name) or \
           _create_security_group(config, vpc_id, group_name)


def _get_security_group(config, vpc_id, group_name):
    security_group = _get_security_groups(config, [vpc_id], [group_name])
    return None if not security_group else security_group[0]


def _get_security_groups(config, vpc_ids, group_names):
    ec2 = _resource("ec2", config)
    existing_groups = list(
        ec2.security_groups.filter(Filters=[{
            "Name": "vpc-id",
            "Values": vpc_ids
        }]))
    matching_groups = []
    for sg in existing_groups:
        if sg.group_name in group_names:
            matching_groups.append(sg)
    return matching_groups


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


def _upsert_security_group_rules(config, security_groups, rules_group_name):
    head_rules = config["provider"].get("head_inbound_rules", {})
    worker_rules = config["provider"].get("worker_inbound_rules", {})

    head_security_group = security_groups["head"]
    worker_security_group = security_groups["workers"]

    if head_rules != worker_rules:
        if head_security_group.id == worker_security_group.id:
            worker_security_group = _get_or_create_security_group(
                config, worker_security_group.vpc_id, rules_group_name)
            security_groups["workers"] = worker_security_group

    sgids = {head_security_group.id, worker_security_group.id}

    head_channel_rules = _group_rules_by_channel(config, head_rules)
    _update_inbound_rules(head_security_group, head_channel_rules, sgids)
    worker_channel_rules = _group_rules_by_channel(config, worker_rules)
    _update_inbound_rules(worker_security_group, worker_channel_rules, sgids)


def _group_rules_by_channel(config, rules):
    """Group rules by network "channel" (from_port, to_port, ip_protocol)"""
    channel_rules = {}

    for inbound_rule in rules:
        rule_channel = inbound_rule["channel"]
        channel_group_key = (rule_channel["from_port"],
                             rule_channel["to_port"],
                             rule_channel["ip_protocol"].lower())
        _union_targets(
            inbound_rule.get("targets", {}),
            channel_rules.setdefault(channel_group_key, {}))

    default_sources = config["provider"].get("default_inbound_sources", {})
    if default_sources:
        valid_source = _target_exists(default_sources)
        assert valid_source, "No targets specified for default inbound sources"
        for default_channel in DEFAULT_INBOUND_CHANNELS:
            if not channel_rules.setdefault(default_channel, {}):
                _union_targets(default_sources, channel_rules[default_channel])

    return channel_rules


def _union_targets(rule_targets, channel_targets):
    cidr_ips = set(rule_targets.get("cidr_ips", []))
    cidr_ipv6s = {_.lower() for _ in rule_targets.get("cidr_ipv6s", [])}
    plids = {_.lower() for _ in rule_targets.get("prefix_list_ids", [])}
    sgids = {_.lower() for _ in rule_targets.get("security_group_ids", [])}

    channel_targets["cidr_ips"] = channel_targets.get(
        "cidr_ips", set(cidr_ips)).union(cidr_ips)
    channel_targets["cidr_ipv6s"] = channel_targets.get(
        "cidr_ipv6s", set(cidr_ipv6s)).union(cidr_ipv6s)
    channel_targets["prefix_list_ids"] = channel_targets.get(
        "prefix_list_ids", set(plids)).union(plids)
    channel_targets["security_group_ids"] = channel_targets.get(
        "security_group_ids", set(sgids)).union(sgids)


def _update_inbound_rules(sg, channel_rules, sgids):
    _update_security_group_ingress(sg, channel_rules, sgids, True)
    req_channel_rules = _get_required_channel_rules(sg, channel_rules, sgids)
    _update_security_group_ingress(sg, req_channel_rules, sgids, False)


def _update_security_group_ingress(sg, channel_rules, required_sgids, revoke):
    ipps_to_revoke = []
    channel_rules_to_add = {}

    for channel, inbound_sources in channel_rules.items():
        cidr_ips = inbound_sources.get("cidr_ips", [])
        cidr_ipv6s = inbound_sources.get("cidr_ipv6s", [])
        plids = inbound_sources.get("prefix_list_ids", [])
        sgids = inbound_sources.get("security_group_ids", [])
        if channel == ALL_TRAFFIC_CHANNEL:
            sgids = sgids.union(required_sgids)

        src_to_add = channel_rules_to_add.setdefault(
            channel, {
                "cidr_ips": set(cidr_ips),
                "cidr_ipv6s": set(cidr_ipv6s),
                "prefix_list_ids": set(plids),
                "security_group_ids": set(sgids)
            })

        sg_ipps = sg.ip_permissions
        for ipp in (_ for _ in sg_ipps if _match_ipp_channel(channel, _)):
            _revoke_or_add(ipp, "IpRanges", "CidrIp", ipps_to_revoke, cidr_ips,
                           src_to_add["cidr_ips"])
            _revoke_or_add(ipp, "Ipv6Ranges", "CidrIpv6", ipps_to_revoke,
                           cidr_ipv6s, src_to_add["cidr_ipv6s"])
            _revoke_or_add(ipp, "PrefixListIds", "PrefixListId",
                           ipps_to_revoke, plids,
                           src_to_add["prefix_list_ids"])
            _revoke_or_add(ipp, "UserIdGroupPairs", "GroupId", ipps_to_revoke,
                           sgids, src_to_add["security_group_ids"])

    if ipps_to_revoke and revoke:
        logger.info("_update_inbound_rules: "
                    "Revoking inbound rules from {} ({}) -- {}".format(
                        sg.group_name, sg.id, ipps_to_revoke))
        sg.revoke_ingress(IpPermissions=ipps_to_revoke)

    if any(_target_exists(_) for _ in channel_rules_to_add.values()):
        ipps_to_authorize = _create_ip_permissions(channel_rules_to_add)
        logger.info("_update_inbound_rules: "
                    "Adding inbound rules to {} ({}) -- {}".format(
                        sg.group_name, sg.id, ipps_to_authorize))
        sg.authorize_ingress(IpPermissions=ipps_to_authorize)


def _match_ipp_channel(channel, ip_permission):
    return channel[FROM_PORT] == ip_permission.get("FromPort", -1) and \
           channel[TO_PORT] == ip_permission.get("ToPort", -1) and \
           channel[IP_PROTOCOL] == ip_permission.get("IpProtocol")


def _revoke_or_add(ipp, srcs_key, src_key, ipp_revokes, whitelist, add_srcs):
    for ipp_srcs in (_ for _ in ipp.get(srcs_key, []) if _.get(src_key)):
        ipp_revokes.append({
            "FromPort": ipp.get("FromPort", -1),
            "ToPort": ipp.get("ToPort", -1),
            "IpProtocol": ipp.get("IpProtocol"),
            srcs_key: [{src_key: ipp_srcs[src_key]}]
        }) if ipp_srcs[src_key] not in whitelist \
           else add_srcs.remove(ipp_srcs[src_key])


def _target_exists(inbound_sources):
    return bool(
        inbound_sources.get("cidr_ips") or inbound_sources.get("cidr_ipv6s")
        or inbound_sources.get("prefix_list_ids")
        or inbound_sources.get("security_group_ids"))


def _get_required_channel_rules(sg, channel_rules, sgids):
    required_channel_rules = {
        ALL_TRAFFIC_CHANNEL: {
            "security_group_ids": set(sgids)
        }
    }
    for channel in DEFAULT_INBOUND_CHANNELS:
        rules_exist = _target_exists(channel_rules.get(channel, {})) or \
            any(_ for _ in sg.ip_permissions if _match_ipp_channel(channel, _))
        if not rules_exist:
            default_targets = {"cidr_ips": ["0.0.0.0/0"]}
            required_channel_rules[channel] = default_targets
    return required_channel_rules


def _create_ip_permissions(channel_rules):
    ip_permissions = []
    for channel in sorted(channel_rules.keys()):
        src = channel_rules[channel]
        ipp = {
            "FromPort": channel[FROM_PORT],
            "ToPort": channel[TO_PORT],
            "IpProtocol": channel[IP_PROTOCOL],
        }
        _append_ipp_sources(src.get("cidr_ips"), ipp, "IpRanges", "CidrIp")
        _append_ipp_sources(
            src.get("cidr_ipv6s"), ipp, "Ipv6Ranges", "CidrIpv6")
        _append_ipp_sources(
            src.get("security_group_ids"), ipp, "UserIdGroupPairs", "GroupId")
        _append_ipp_sources(
            src.get("prefix_list_ids"), ipp, "PrefixListIds", "PrefixListId")
        ip_permissions.append(ipp)

    return ip_permissions


def _append_ipp_sources(sources_to_add, ipp, ipp_sources_key, src_key):
    if sources_to_add:
        ipp[ipp_sources_key] = [{src_key: _} for _ in sorted(sources_to_add)]


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
    boto_config = Config(retries={"max_attempts": BOTO_MAX_RETRIES})
    aws_credentials = config["provider"].get("aws_credentials", {})
    return RESOURCE_CACHE.setdefault(
        name,
        boto3.resource(
            name,
            config["provider"]["region"],
            config=boto_config,
            **aws_credentials))
