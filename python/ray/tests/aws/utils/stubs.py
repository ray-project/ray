import ray
from ray.tests.aws.utils.mocks import mock_path_exists_key_pair
from ray.tests.aws.utils.constants import DEFAULT_INSTANCE_PROFILE, \
    DEFAULT_KEY_PAIR, DEFAULT_SUBNET

from unittest import mock

from botocore.stub import ANY


def configure_iam_role_default(iam_client_stub):
    iam_client_stub.add_response(
        "get_instance_profile",
        expected_params={
            "InstanceProfileName": ray.autoscaler.aws.config.
            DEFAULT_RAY_INSTANCE_PROFILE
        },
        service_response={"InstanceProfile": DEFAULT_INSTANCE_PROFILE})


def configure_key_pair_default(ec2_client_stub):
    patcher = mock.patch("os.path.exists")
    os_path_exists_mock = patcher.start()
    os_path_exists_mock.side_effect = mock_path_exists_key_pair

    ec2_client_stub.add_response(
        "describe_key_pairs",
        expected_params={
            "Filters": [{
                "Name": "key-name",
                "Values": [DEFAULT_KEY_PAIR["KeyName"]]
            }]
        },
        service_response={"KeyPairs": [DEFAULT_KEY_PAIR]})


def configure_subnet_default(ec2_client_stub):
    ec2_client_stub.add_response(
        "describe_subnets",
        expected_params={},
        service_response={"Subnets": [DEFAULT_SUBNET]})


def skip_to_configure_sg(ec2_client_stub, iam_client_stub):
    configure_iam_role_default(iam_client_stub)
    configure_key_pair_default(ec2_client_stub)
    configure_subnet_default(ec2_client_stub)


def describe_subnets_echo(ec2_client_stub, subnet):
    ec2_client_stub.add_response(
        "describe_subnets",
        expected_params={
            "Filters": [{
                "Name": "subnet-id",
                "Values": [subnet["SubnetId"]]
            }]
        },
        service_response={"Subnets": [subnet]})


def describe_no_security_groups(ec2_client_stub):
    ec2_client_stub.add_response(
        "describe_security_groups",
        expected_params={"Filters": ANY},
        service_response={})


def create_sg_echo(ec2_client_stub, security_group):
    ec2_client_stub.add_response(
        "create_security_group",
        expected_params={
            "Description": security_group["Description"],
            "GroupName": security_group["GroupName"],
            "VpcId": security_group["VpcId"]
        },
        service_response={"GroupId": security_group["GroupId"]})


def describe_sgs_on_vpc(ec2_client_stub, vpc_ids, security_groups):
    ec2_client_stub.add_response(
        "describe_security_groups",
        expected_params={"Filters": [{
            "Name": "vpc-id",
            "Values": vpc_ids
        }]},
        service_response={"SecurityGroups": security_groups})


def authorize_sg_ingress(ec2_client_stub, security_group):
    ec2_client_stub.add_response(
        "authorize_security_group_ingress",
        expected_params={
            "GroupId": security_group["GroupId"],
            "IpPermissions": security_group["IpPermissions"]
        },
        service_response={})


def describe_sg_echo(ec2_client_stub, security_group):
    ec2_client_stub.add_response(
        "describe_security_groups",
        expected_params={"GroupIds": [security_group["GroupId"]]},
        service_response={"SecurityGroups": [security_group]})


def create_tag_specifications(resource_types, keys, values):
    tag_specs = []
    for r in resource_types:
        tag_specs.append({ 
            "ResourceType": r,
            "Tags": [{"Key": keys[i], "Value": values[i]}
                     for i in range(len(keys))]
        })
    return tag_specs


def describe_launch_template_versions_echo(ec2_client_stub, has_iam=True, 
    has_key_name=True, has_tags=True, has_image=True, has_network=True,
    has_subnet=True, has_security_group=True):
    
    data = {}

    if has_iam:
        data["IamInstanceProfile"] = {"Name": "foo"}
    
    if has_key_name:
        data["KeyName"] = "foo"

    if has_tags:
        data["TagSpecifications"] = [
            {
                "ResourceType": "instance",
                "Tags": [
                    {
                        "Key": "Name",
                        "Value": "foo"
                    }
                ]
            },
            {
                "ResourceType": "volume",
                "Tags": [
                    {
                        "Key": "Name",
                        "Value": "foo"
                    }
                ]
            }
        ]

    if has_image:
        data["ImageId"] = "foo"

    if has_network:
        data["NetworkInterfaces"] = [{}]
        if has_subnet:
            data["NetworkInterfaces"][0]["SubnetId"] = "foo"
        if has_security_group:
            data["NetworkInterfaces"][0]["Groups"] = ["bar"]

    response = {
        "LaunchTemplateVersions": [
            {
                "LaunchTemplateData": data
            }
        ]
    }

    ec2_client_stub.add_response(
        "describe_launch_template_versions",
        service_response=response)
