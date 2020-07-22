import ray
import json
from ray.tests.aws.utils.mocks import mock_path_exists_key_pair
from ray.tests.aws.utils.constants import DEFAULT_INSTANCE_PROFILE, \
    DEFAULT_KEY_PAIR, DEFAULT_SUBNET
from ray.tests.aws.utils.helpers import \
    get_cloudwatch_dashboard_config_file_path,\
    get_cloudwatch_alarm_config_file_path
from ray.autoscaler.aws.cloudwatch.cloudwatch_helper import \
    CWA_CONFIG_SSM_PARAM_NAME

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


def describe_instance_status_ok(ec2_client_stub, instance_ids):
    ec2_client_stub.add_response(
        "describe_instance_status",
        expected_params={"InstanceIds": instance_ids},
        service_response={
            "InstanceStatuses": [{
                "InstanceId": instance_id,
                "InstanceState": {
                    "Code": 16,
                    "Name": "running"
                },
                "AvailabilityZone": "us-west-2",
                "SystemStatus": {
                    "Status": "ok",
                    "Details": [{
                        "Status": "passed",
                        "Name": "reachability"
                    }]
                },
                "InstanceStatus": {
                    "Status": "ok",
                    "Details": [{
                        "Status": "passed",
                        "Name": "reachability"
                    }]
                }
            }]
            for instance_id in instance_ids
        })


def send_command_cwa_install(ssm_client_stub, node_ids):
    ssm_client_stub.add_response(
        "send_command",
        expected_params={
            "DocumentName": "AWS-ConfigureAWSPackage",
            "InstanceIds": node_ids,
            "MaxConcurrency": str(min(len(node_ids), 100)),
            "MaxErrors": "0",
            "Parameters": {
                "action": ["Install"],
                "name": ["AmazonCloudWatchAgent"],
                "version": ["latest"]
            }
        },
        service_response={
            "Command": {
                "CommandId": "92853adf-ba41-4cd6-9a88-142d1EXAMPLE",
                "DocumentName": "AWS-ConfigureAWSPackage"
            }
        })


def get_command_invocation_success(ssm_client_stub, node_ids):
    for node_id in node_ids:
        ssm_client_stub.add_response(
            "get_command_invocation",
            expected_params={
                "CommandId": "92853adf-ba41-4cd6-9a88-142d1EXAMPLE",
                "InstanceId": node_id
            },
            service_response={"Status": "Success"})


def put_parameter_cloudwatch_agent_config(ssm_client_stub):
    ssm_client_stub.add_response(
        "put_parameter",
        expected_params={
            "Name": CWA_CONFIG_SSM_PARAM_NAME,
            "Type": "String",
            "Value": ANY,
            "Overwrite": True
        },
        service_response={})


def send_command_cwa_collectd_setup_script(ssm_client_stub, node_ids):
    ssm_client_stub.add_response(
        "send_command",
        expected_params={
            "DocumentName": "AWS-RunShellScript",
            "InstanceIds": node_ids,
            "MaxConcurrency": str(min(len(node_ids), 100)),
            "MaxErrors": "0",
            "Parameters": {
                "commands": [
                    "mkdir -p /usr/share/collectd/",
                    "touch /usr/share/collectd/types.db"
                ],
            }
        },
        service_response={})


def send_command_start_cwa(ssm_client_stub, node_ids):
    ssm_client_stub.add_response(
        "send_command",
        expected_params={
            "DocumentName": "AmazonCloudWatch-ManageAgent",
            "InstanceIds": node_ids,
            "MaxConcurrency": str(min(len(node_ids), 100)),
            "MaxErrors": "0",
            "Parameters": {
                "action": ["configure"],
                "mode": ["ec2"],
                "optionalConfigurationSource": ["ssm"],
                "optionalConfigurationLocation": [CWA_CONFIG_SSM_PARAM_NAME],
                "optionalRestart": ["yes"]
            }
        },
        service_response={})


def put_cluster_dashboard_success(cloudwatch_client_stub, cloudwatch_helper):
    widgets = []
    json_config_path = get_cloudwatch_dashboard_config_file_path()
    with open(json_config_path) as f:
        dashboard_config = json.load(f)
    for node_id in cloudwatch_helper.node_ids:
        for item in dashboard_config:
            cloudwatch_helper._replace_all_config_variables(
                item,
                node_id,
                cloudwatch_helper.cluster_name,
            )
            widgets.append(item)
        cloudwatch_client_stub.add_response(
            "put_dashboard",
            expected_params={
                "DashboardName": "example-dashboard-name",
                "DashboardBody": json.dumps({
                    "widgets": widgets
                })
            },
            service_response={"ResponseMetadata": {
                "HTTPStatusCode": 200
            }})


def put_cluster_alarms_success(cloudwatch_client_stub, cloudwatch_helper):
    json_config_path = get_cloudwatch_alarm_config_file_path()
    with open(json_config_path) as f:
        data = json.load(f)
    for node_id in cloudwatch_helper.node_ids:
        for item in data:
            cloudwatch_helper._replace_all_config_variables(
                item,
                node_id,
                cloudwatch_helper.cluster_name,
            )
            cloudwatch_client_stub.add_response(
                "put_metric_alarm",
                expected_params=item,
                service_response={"ResponseMetadata": {
                    "HTTPStatusCode": 200
                }})
