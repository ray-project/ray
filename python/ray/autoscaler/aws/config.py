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
from ray.autoscaler.aws.utils import LazyDefaultDict, handle_boto_error
from ray.autoscaler.node_provider import PROVIDER_PRETTY_NAMES

from ray.autoscaler.cli_logger import cli_logger
import colorful as cf

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

# todo: cli_logger should handle this assert properly
# this should probably also happens somewhere else
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

_log_info = {}


def reload_log_state(override_log_info):
    _log_info.update(override_log_info)


def get_log_state():
    return _log_info.copy()


def _set_config_info(**kwargs):
    """Record configuration artifacts useful for logging."""

    # todo: this is technically fragile iff we ever use multiple configs

    for k, v in kwargs.items():
        _log_info[k] = v


def _arn_to_name(arn):
    return arn.split(":")[-1].split("/")[-1]


def log_to_cli(config):
    provider_name = PROVIDER_PRETTY_NAMES.get("aws", None)

    cli_logger.doassert(provider_name is not None,
                        "Could not find a pretty name for the AWS provider.")

    with cli_logger.group("{} config", provider_name):

        def same_everywhere(key):
            return config["head_node"][key] == config["worker_nodes"][key]

        def print_info(resource_string,
                       key,
                       head_src_key,
                       workers_src_key,
                       allowed_tags=["default"],
                       list_value=False):

            head_tags = {}
            workers_tags = {}

            if _log_info[head_src_key] in allowed_tags:
                head_tags[_log_info[head_src_key]] = True
            if _log_info[workers_src_key] in allowed_tags:
                workers_tags[_log_info[workers_src_key]] = True

            head_value_str = config["head_node"][key]
            if list_value:
                head_value_str = cli_logger.render_list(head_value_str)

            if same_everywhere(key):
                cli_logger.labeled_value(  # todo: handle plural vs singular?
                    resource_string + " (head & workers)",
                    "{}",
                    head_value_str,
                    _tags=head_tags)
            else:
                workers_value_str = config["worker_nodes"][key]
                if list_value:
                    workers_value_str = cli_logger.render_list(
                        workers_value_str)

                cli_logger.labeled_value(
                    resource_string + " (head)",
                    "{}",
                    head_value_str,
                    _tags=head_tags)
                cli_logger.labeled_value(
                    resource_string + " (workers)",
                    "{}",
                    workers_value_str,
                    _tags=workers_tags)

        tags = {"default": _log_info["head_instance_profile_src"] == "default"}
        cli_logger.labeled_value(
            "IAM Profile",
            "{}",
            _arn_to_name(config["head_node"]["IamInstanceProfile"]["Arn"]),
            _tags=tags)

        print_info("EC2 Key pair", "KeyName", "keypair_src", "keypair_src")
        print_info(
            "VPC Subnets",
            "SubnetIds",
            "head_subnet_src",
            "workers_subnet_src",
            list_value=True)
        print_info(
            "EC2 Security groups",
            "SecurityGroupIds",
            "head_security_group_src",
            "workers_security_group_src",
            list_value=True)
        print_info(
            "EC2 AMI",
            "ImageId",
            "head_ami_src",
            "workers_ami_src",
            allowed_tags=["dlami"])

    cli_logger.newline()


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
        _set_config_info(head_instance_profile_src="config")
        return config
    _set_config_info(head_instance_profile_src="default")

    profile = _get_instance_profile(DEFAULT_RAY_INSTANCE_PROFILE, config)

    if profile is None:
        cli_logger.verbose(
            "Creating new IAM instance profile {} for use as the default.",
            cf.bold(DEFAULT_RAY_INSTANCE_PROFILE))
        cli_logger.old_info(
            logger, "_configure_iam_role: "
            "Creating new instance profile {}", DEFAULT_RAY_INSTANCE_PROFILE)
        client = _client("iam", config)
        client.create_instance_profile(
            InstanceProfileName=DEFAULT_RAY_INSTANCE_PROFILE)
        profile = _get_instance_profile(DEFAULT_RAY_INSTANCE_PROFILE, config)
        time.sleep(15)  # wait for propagation

    cli_logger.doassert(profile is not None,
                        "Failed to create instance profile.")  # todo: err msg
    assert profile is not None, "Failed to create instance profile"

    if not profile.roles:
        role = _get_role(DEFAULT_RAY_IAM_ROLE, config)
        if role is None:
            cli_logger.verbose(
                "Creating new IAM role {} for "
                "use as the default instance role.",
                cf.bold(DEFAULT_RAY_IAM_ROLE))
            cli_logger.old_info(logger, "_configure_iam_role: "
                                "Creating new role {}", DEFAULT_RAY_IAM_ROLE)
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

            cli_logger.doassert(role is not None,
                                "Failed to create role.")  # todo: err msg
            assert role is not None, "Failed to create role"
        role.attach_policy(
            PolicyArn="arn:aws:iam::aws:policy/AmazonEC2FullAccess")
        role.attach_policy(
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess")
        profile.add_role(RoleName=role.name)
        time.sleep(15)  # wait for propagation

    cli_logger.old_info(
        logger, "_configure_iam_role: "
        "Role not specified for head node, using {}", profile.arn)
    config["head_node"]["IamInstanceProfile"] = {"Arn": profile.arn}

    return config


def _configure_key_pair(config):
    if "ssh_private_key" in config["auth"]:
        _set_config_info(keypair_src="config")

        cli_logger.doassert(  # todo: verify schema beforehand?
            "KeyName" in config["head_node"],
            "`KeyName` missing for head node.")  # todo: err msg
        cli_logger.doassert(
            "KeyName" in config["worker_nodes"],
            "`KeyName` missing for worker nodes.")  # todo: err msg

        assert "KeyName" in config["head_node"]
        assert "KeyName" in config["worker_nodes"]
        return config
    _set_config_info(keypair_src="default")

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
            cli_logger.verbose(
                "Creating new key pair {} for use as the default.",
                cf.bold(key_name))
            cli_logger.old_info(
                logger, "_configure_key_pair: "
                "Creating new key pair {}", key_name)
            key = ec2.create_key_pair(KeyName=key_name)

            # We need to make sure to _create_ the file with the right
            # permissions. In order to do that we need to change the default
            # os.open behavior to include the mode we want.
            with open(key_path, "w", opener=partial(os.open, mode=0o600)) as f:
                f.write(key.key_material)
            break

    if not key:
        cli_logger.abort(
            "No matching local key file for any of the key pairs in this "
            "account with ids from 0..{}. "
            "Consider deleting some unused keys pairs from your account.",
            key_name)  # todo: err msg
        raise ValueError(
            "No matching local key file for any of the key pairs in this "
            "account with ids from 0..{}. ".format(key_name) +
            "Consider deleting some unused keys pairs from your account.")

    cli_logger.doassert(
        os.path.exists(key_path), "Private key file " + cf.bold("{}") +
        " not found for " + cf.bold("{}"), key_path, key_name)  # todo: err msg
    assert os.path.exists(key_path), \
        "Private key file {} not found for {}".format(key_path, key_name)

    cli_logger.old_info(
        logger, "_configure_key_pair: "
        "KeyName not specified for nodes, using {}", key_name)

    config["auth"]["ssh_private_key"] = key_path
    config["head_node"]["KeyName"] = key_name
    config["worker_nodes"]["KeyName"] = key_name

    return config


def _configure_subnet(config):
    ec2 = _resource("ec2", config)
    use_internal_ips = config["provider"].get("use_internal_ips", False)

    try:
        subnets = sorted(
            (s for s in ec2.subnets.all() if s.state == "available" and (
                use_internal_ips or s.map_public_ip_on_launch)),
            reverse=True,  # sort from Z-A
            key=lambda subnet: subnet.availability_zone)
    except botocore.exceptions.ClientError as exc:
        handle_boto_error(exc, "Failed to fetch available subnets from AWS.")
        raise exc

    if not subnets:
        cli_logger.abort(
            "No usable subnets found, try manually creating an instance in "
            "your specified region to populate the list of subnets "
            "and trying this again.\n"
            "Note that the subnet must map public IPs "
            "on instance launch unless you set `use_internal_ips: true` in "
            "the `provider` config.")  # todo: err msg
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
            cli_logger.abort(
                "No usable subnets matching availability zone {} found.\n"
                "Choose a different availability zone or try "
                "manually creating an instance in your specified region "
                "to populate the list of subnets and trying this again.",
                config["provider"]["availability_zone"])  # todo: err msg
            raise Exception(
                "No usable subnets matching availability zone {} "
                "found. Choose a different availability zone or try "
                "manually creating an instance in your specified region "
                "to populate the list of subnets and trying this again.".
                format(config["provider"]["availability_zone"]))

    subnet_ids = [s.subnet_id for s in subnets]
    subnet_descr = [(s.subnet_id, s.availability_zone) for s in subnets]
    if "SubnetIds" not in config["head_node"]:
        _set_config_info(head_subnet_src="default")
        config["head_node"]["SubnetIds"] = subnet_ids
        cli_logger.old_info(
            logger, "_configure_subnet: "
            "SubnetIds not specified for head node, using {}", subnet_descr)
    else:
        _set_config_info(head_subnet_src="config")

    if "SubnetIds" not in config["worker_nodes"]:
        _set_config_info(workers_subnet_src="default")
        config["worker_nodes"]["SubnetIds"] = subnet_ids
        cli_logger.old_info(
            logger, "_configure_subnet: "
            "SubnetId not specified for workers,"
            " using {}", subnet_descr)
    else:
        _set_config_info(workers_subnet_src="config")

    return config


def _configure_security_group(config):
    _set_config_info(
        head_security_group_src="config", workers_security_group_src="config")

    node_types_to_configure = [
        node_type for node_type, config_key in NODE_TYPE_CONFIG_KEYS.items()
        if "SecurityGroupIds" not in config[NODE_TYPE_CONFIG_KEYS[node_type]]
    ]
    if not node_types_to_configure:
        return config  # have user-defined groups

    security_groups = _upsert_security_groups(config, node_types_to_configure)

    if NODE_TYPE_HEAD in node_types_to_configure:
        head_sg = security_groups[NODE_TYPE_HEAD]

        _set_config_info(head_security_group_src="default")
        cli_logger.old_info(
            logger, "_configure_security_group: "
            "SecurityGroupIds not specified for head node, using {} ({})",
            head_sg.group_name, head_sg.id)
        config["head_node"]["SecurityGroupIds"] = [head_sg.id]

    if NODE_TYPE_WORKER in node_types_to_configure:
        workers_sg = security_groups[NODE_TYPE_WORKER]

        _set_config_info(workers_security_group_src="default")
        cli_logger.old_info(
            logger, "_configure_security_group: "
            "SecurityGroupIds not specified for workers, using {} ({})",
            workers_sg.group_name, workers_sg.id)
        config["worker_nodes"]["SecurityGroupIds"] = [workers_sg.id]

    return config


def _check_ami(config):
    """Provide helpful message for missing ImageId for node configuration."""

    _set_config_info(head_ami_src="config", workers_ami_src="config")

    region = config["provider"]["region"]
    default_ami = DEFAULT_AMI.get(region)
    if not default_ami:
        # If we do not provide a default AMI for the given region, noop.
        return

    if config["head_node"].get("ImageId", "").lower() == "latest_dlami":
        config["head_node"]["ImageId"] = default_ami
        _set_config_info(head_ami_src="dlami")
        cli_logger.old_info(
            logger,
            "_check_ami: head node ImageId is 'latest_dlami'. "
            "Using '{ami_id}', which is the default {ami_name} "
            "for your region ({region}).",
            ami_id=default_ami,
            ami_name=DEFAULT_AMI_NAME,
            region=region)

    if config["worker_nodes"].get("ImageId", "").lower() == "latest_dlami":
        config["worker_nodes"]["ImageId"] = default_ami
        _set_config_info(workers_ami_src="dlami")
        cli_logger.old_info(
            logger,
            "_check_ami: worker nodes ImageId is 'latest_dlami'. "
            "Using '{ami_id}', which is the default {ami_name} "
            "for your region ({region}).",
            ami_id=default_ami,
            ami_name=DEFAULT_AMI_NAME,
            region=region)


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

    # TODO: better error message
    cli_logger.doassert(len(subnet) == 1, "Subnet ID not found: {}", subnet_id)
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

    cli_logger.verbose(
        "Created new security group {}",
        cf.bold(security_group.group_name),
        _tags=dict(id=security_group.id))
    cli_logger.old_info(
        logger, "_create_security_group: Created new security group {} ({})",
        security_group.group_name, security_group.id)

    cli_logger.doassert(security_group,
                        "Failed to create security group")  # err msg
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
            handle_boto_error(
                exc, "Failed to fetch IAM role data for {} from AWS.",
                cf.bold(role_name))
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
            handle_boto_error(
                exc,
                "Failed to fetch IAM instance profile data for {} from AWS.",
                cf.bold(profile_name))
            raise exc


def _get_key(key_name, config):
    ec2 = _resource("ec2", config)
    try:
        for key in ec2.key_pairs.filter(Filters=[{
                "Name": "key-name",
                "Values": [key_name]
        }]):
            if key.name == key_name:
                return key
    except botocore.exceptions.ClientError as exc:
        handle_boto_error(exc, "Failed to fetch EC2 key pair {} from AWS.",
                          cf.bold(key_name))
        raise exc


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
