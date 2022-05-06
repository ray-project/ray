from typing import Dict, List
import ray
import copy

from ray.tests.aws.utils import helpers
from ray.tests.aws.utils.constants import (
    DEFAULT_INSTANCE_PROFILE,
    DEFAULT_KEY_PAIR,
    DEFAULT_SUBNET,
    A_THOUSAND_SUBNETS_IN_DIFFERENT_VPCS,
    DEFAULT_LT,
    TWENTY_SUBNETS_IN_DIFFERENT_AZS,
)
from ray.autoscaler._private.aws.config import key_pair

from unittest import mock

from botocore.stub import ANY


def configure_iam_role_default(iam_client_stub):
    iam_client_stub.add_response(
        "get_instance_profile",
        expected_params={
            "InstanceProfileName": ray.autoscaler._private.aws.config.DEFAULT_RAY_INSTANCE_PROFILE  # noqa: E501
        },
        service_response={"InstanceProfile": DEFAULT_INSTANCE_PROFILE},
    )


def configure_key_pair_default(
    ec2_client_stub, region="us-west-2", expected_key_pair=DEFAULT_KEY_PAIR
):
    patcher = mock.patch("os.path.exists")

    def mock_path_exists_key_pair(path):
        _, key_path = key_pair(0, region, expected_key_pair["KeyName"])
        return path == key_path

    os_path_exists_mock = patcher.start()
    os_path_exists_mock.side_effect = mock_path_exists_key_pair

    ec2_client_stub.add_response(
        "describe_key_pairs",
        expected_params={
            "Filters": [{"Name": "key-name", "Values": [expected_key_pair["KeyName"]]}]
        },
        service_response={"KeyPairs": [expected_key_pair]},
    )


def configure_subnet_default(ec2_client_stub):
    ec2_client_stub.add_response(
        "describe_subnets",
        expected_params={},
        service_response={"Subnets": [DEFAULT_SUBNET]},
    )


def describe_a_thousand_subnets_in_different_vpcs(ec2_client_stub):
    ec2_client_stub.add_response(
        "describe_subnets",
        expected_params={},
        service_response={"Subnets": A_THOUSAND_SUBNETS_IN_DIFFERENT_VPCS},
    )


def describe_twenty_subnets_in_different_azs(ec2_client_stub):
    ec2_client_stub.add_response(
        "describe_subnets",
        expected_params={},
        service_response={"Subnets": TWENTY_SUBNETS_IN_DIFFERENT_AZS},
    )


def skip_to_configure_sg(ec2_client_stub, iam_client_stub):
    configure_iam_role_default(iam_client_stub)
    configure_key_pair_default(ec2_client_stub)
    configure_subnet_default(ec2_client_stub)


def describe_subnets_echo(ec2_client_stub, subnets: List[Dict[str, str]]):
    ec2_client_stub.add_response(
        "describe_subnets",
        expected_params={
            "Filters": [
                {"Name": "subnet-id", "Values": [s["SubnetId"] for s in subnets]}
            ]
        },
        service_response={"Subnets": subnets},
    )


def describe_no_security_groups(ec2_client_stub):
    ec2_client_stub.add_response(
        "describe_security_groups",
        expected_params={"Filters": ANY},
        service_response={},
    )


def describe_a_security_group(ec2_client_stub, security_group):
    ec2_client_stub.add_response(
        "describe_security_groups",
        expected_params={
            "Filters": [{"Name": "group-id", "Values": [security_group["GroupId"]]}]
        },
        service_response={"SecurityGroups": [security_group]},
    )


def describe_an_sg_2(ec2_client_stub, security_group):
    """Same as last function, different input param format.

    A call with this input parameter format is made when sg.ip_permissions is
    accessed in aws/config.py.
    """
    ec2_client_stub.add_response(
        "describe_security_groups",
        expected_params={"GroupIds": [security_group["GroupId"]]},
        service_response={"SecurityGroups": [security_group]},
    )


def create_sg_echo(ec2_client_stub, security_group):
    ec2_client_stub.add_response(
        "create_security_group",
        expected_params={
            "Description": security_group["Description"],
            "GroupName": security_group["GroupName"],
            "VpcId": security_group["VpcId"],
        },
        service_response={"GroupId": security_group["GroupId"]},
    )


def describe_sgs_by_id(ec2_client_stub, security_group_ids, security_groups):
    ec2_client_stub.add_response(
        "describe_security_groups",
        expected_params={
            "Filters": [{"Name": "group-id", "Values": security_group_ids}]
        },
        service_response={"SecurityGroups": security_groups},
    )


def describe_sgs_on_vpc(ec2_client_stub, vpc_ids, security_groups):
    ec2_client_stub.add_response(
        "describe_security_groups",
        expected_params={"Filters": [{"Name": "vpc-id", "Values": vpc_ids}]},
        service_response={"SecurityGroups": security_groups},
    )


def authorize_sg_ingress(ec2_client_stub, security_group):
    ec2_client_stub.add_response(
        "authorize_security_group_ingress",
        expected_params={
            "GroupId": security_group["GroupId"],
            "IpPermissions": security_group["IpPermissions"],
        },
        service_response={},
    )


def describe_sg_echo(ec2_client_stub, security_group):
    ec2_client_stub.add_response(
        "describe_security_groups",
        expected_params={"GroupIds": [security_group["GroupId"]]},
        service_response={"SecurityGroups": [security_group]},
    )


def run_instances_with_network_interfaces_consumer(ec2_client_stub, network_interfaces):
    ec2_client_stub.add_response(
        "run_instances",
        expected_params={
            "NetworkInterfaces": network_interfaces,
            "ImageId": ANY,
            "InstanceType": ANY,
            "KeyName": ANY,
            "MinCount": ANY,
            "MaxCount": ANY,
            "TagSpecifications": ANY,
        },
        service_response={},
    )


def run_instances_with_launch_template_consumer(
    ec2_client_stub, config, node_cfg, node_type_name, lt_data, max_count
):
    # create a copy of both node config and launch template data to modify
    lt_data_cp = copy.deepcopy(lt_data)
    node_cfg_cp = copy.deepcopy(node_cfg)
    # override launch template parameters with explicit node config parameters
    lt_data_cp.update(node_cfg_cp)
    # copy all launch template parameters back to node config
    node_cfg_cp.update(lt_data_cp)
    # copy all default node provider config updates to node config
    helpers.apply_node_provider_config_updates(
        config, node_cfg_cp, node_type_name, max_count
    )
    # remove any security group and subnet IDs copied from network interfaces
    node_cfg_cp.pop("SecurityGroupIds", [])
    node_cfg_cp.pop("SubnetIds", [])
    ec2_client_stub.add_response(
        "run_instances", expected_params=node_cfg_cp, service_response={}
    )


def describe_instances_with_any_filter_consumer(ec2_client_stub):
    ec2_client_stub.add_response(
        "describe_instances", expected_params={"Filters": ANY}, service_response={}
    )


def describe_launch_template_versions_by_id_default(ec2_client_stub, versions):
    ec2_client_stub.add_response(
        "describe_launch_template_versions",
        expected_params={
            "LaunchTemplateId": DEFAULT_LT["LaunchTemplateId"],
            "Versions": versions,
        },
        service_response={"LaunchTemplateVersions": [DEFAULT_LT]},
    )


def describe_launch_template_versions_by_name_default(ec2_client_stub, versions):
    ec2_client_stub.add_response(
        "describe_launch_template_versions",
        expected_params={
            "LaunchTemplateName": DEFAULT_LT["LaunchTemplateName"],
            "Versions": versions,
        },
        service_response={"LaunchTemplateVersions": [DEFAULT_LT]},
    )
