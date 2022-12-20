from typing import Dict, List
import ray
import copy
import json

from uuid import uuid4
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
from ray.tests.aws.utils.helpers import (
    get_cloudwatch_dashboard_config_file_path,
    get_cloudwatch_alarm_config_file_path,
)
from ray.autoscaler._private.aws.cloudwatch.cloudwatch_helper import (
    CLOUDWATCH_AGENT_INSTALLED_TAG,
    CLOUDWATCH_CONFIG_HASH_TAG_BASE,
)
from ray.autoscaler.tags import NODE_KIND_HEAD, TAG_RAY_NODE_KIND

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


def describe_instance_status_ok(ec2_client_stub, instance_ids):
    ec2_client_stub.add_response(
        "describe_instance_status",
        expected_params={"InstanceIds": instance_ids},
        service_response={
            "InstanceStatuses": [
                {
                    "InstanceId": instance_id,
                    "InstanceState": {"Code": 16, "Name": "running"},
                    "AvailabilityZone": "us-west-2",
                    "SystemStatus": {
                        "Status": "ok",
                        "Details": [{"Status": "passed", "Name": "reachability"}],
                    },
                    "InstanceStatus": {
                        "Status": "ok",
                        "Details": [{"Status": "passed", "Name": "reachability"}],
                    },
                }
            ]
            for instance_id in instance_ids
        },
    )


def get_ec2_cwa_installed_tag_true(ec2_client_stub, node_id):
    ec2_client_stub.add_response(
        "describe_instances",
        expected_params={"InstanceIds": [node_id]},
        service_response={
            "Reservations": [
                {
                    "Instances": [
                        {
                            "InstanceId": node_id,
                            "Tags": [
                                {
                                    "Key": CLOUDWATCH_AGENT_INSTALLED_TAG,
                                    "Value": "True",
                                },
                            ],
                        }
                    ]
                }
            ]
        },
    )


def update_hash_tag_success(ec2_client_stub, node_id, config_type, cloudwatch_helper):
    hash_key_value = "-".join([CLOUDWATCH_CONFIG_HASH_TAG_BASE, config_type])
    cur_hash_value = get_sha1_hash_of_cloudwatch_config_file(
        config_type, cloudwatch_helper
    )
    ec2_client_stub.add_response(
        "create_tags",
        expected_params={
            "Resources": [node_id],
            "Tags": [{"Key": hash_key_value, "Value": cur_hash_value}],
        },
        service_response={"ResponseMetadata": {"HTTPStatusCode": 200}},
    )


def add_cwa_installed_tag_response(ec2_client_stub, node_id):
    ec2_client_stub.add_response(
        "create_tags",
        expected_params={
            "Resources": node_id,
            "Tags": [{"Key": CLOUDWATCH_AGENT_INSTALLED_TAG, "Value": "True"}],
        },
        service_response={"ResponseMetadata": {"HTTPStatusCode": 200}},
    )


def get_head_node_config_hash_different(ec2_client_stub, config_type, cwh, node_id):
    hash_key_value = "-".join([CLOUDWATCH_CONFIG_HASH_TAG_BASE, config_type])
    cur_hash_value = get_sha1_hash_of_cloudwatch_config_file(config_type, cwh)
    filters = cwh._get_current_cluster_session_nodes(cwh.cluster_name)
    filters.append(
        {
            "Name": "tag:{}".format(TAG_RAY_NODE_KIND),
            "Values": [NODE_KIND_HEAD],
        }
    )
    ec2_client_stub.add_response(
        "describe_instances",
        expected_params={"Filters": filters},
        service_response={
            "Reservations": [
                {
                    "Instances": [
                        {
                            "InstanceId": node_id,
                            "Tags": [
                                {"Key": hash_key_value, "Value": cur_hash_value},
                            ],
                        }
                    ]
                }
            ]
        },
    )


def get_cur_node_config_hash_different(ec2_client_stub, config_type, node_id):
    hash_key_value = "-".join([CLOUDWATCH_CONFIG_HASH_TAG_BASE, config_type])
    ec2_client_stub.add_response(
        "describe_instances",
        expected_params={"InstanceIds": [node_id]},
        service_response={
            "Reservations": [
                {
                    "Instances": [
                        {
                            "InstanceId": node_id,
                            "Tags": [
                                {"Key": hash_key_value, "Value": str(uuid4())},
                            ],
                        }
                    ]
                }
            ]
        },
    )


def send_command_cwa_install(ssm_client_stub, node_id):
    command_id = str(uuid4())
    ssm_client_stub.add_response(
        "send_command",
        expected_params={
            "DocumentName": "AWS-ConfigureAWSPackage",
            "InstanceIds": node_id,
            "MaxConcurrency": "1",
            "MaxErrors": "0",
            "Parameters": {
                "action": ["Install"],
                "name": ["AmazonCloudWatchAgent"],
                "version": ["latest"],
            },
        },
        service_response={
            "Command": {
                "CommandId": command_id,
                "DocumentName": "AWS-ConfigureAWSPackage",
            }
        },
    )
    return command_id


def list_command_invocations_status(ssm_client_stub, node_id, cmd_id, status):
    ssm_client_stub.add_response(
        "list_command_invocations",
        expected_params={"CommandId": cmd_id, "InstanceId": node_id},
        service_response={"CommandInvocations": [{"Status": status}]},
    )


def list_command_invocations_failed(ssm_client_stub, node_id, cmd_id):
    status = "Failed"
    list_command_invocations_status(ssm_client_stub, node_id, cmd_id, status)


def list_command_invocations_success(ssm_client_stub, node_id, cmd_id):
    status = "Success"
    list_command_invocations_status(ssm_client_stub, node_id, cmd_id, status)


def put_parameter_cloudwatch_config(ssm_client_stub, cluster_name, section_name):
    ssm_config_param_name = helpers.get_ssm_param_name(cluster_name, section_name)
    ssm_client_stub.add_response(
        "put_parameter",
        expected_params={
            "Name": ssm_config_param_name,
            "Type": "String",
            "Value": ANY,
            "Overwrite": True,
            "Tier": ANY,
        },
        service_response={},
    )


def send_command_cwa_collectd_init(ssm_client_stub, node_id):
    command_id = str(uuid4())
    ssm_client_stub.add_response(
        "send_command",
        expected_params={
            "DocumentName": "AWS-RunShellScript",
            "InstanceIds": [node_id],
            "MaxConcurrency": "1",
            "MaxErrors": "0",
            "Parameters": {
                "commands": [
                    "mkdir -p /usr/share/collectd/",
                    "touch /usr/share/collectd/types.db",
                ],
            },
        },
        service_response={"Command": {"CommandId": command_id}},
    )
    return command_id


def send_command_start_cwa(ssm_client_stub, node_id, parameter_name):
    command_id = str(uuid4())
    ssm_client_stub.add_response(
        "send_command",
        expected_params={
            "DocumentName": "AmazonCloudWatch-ManageAgent",
            "InstanceIds": [node_id],
            "MaxConcurrency": "1",
            "MaxErrors": "0",
            "Parameters": {
                "action": ["configure"],
                "mode": ["ec2"],
                "optionalConfigurationSource": ["ssm"],
                "optionalConfigurationLocation": [parameter_name],
                "optionalRestart": ["yes"],
            },
        },
        service_response={"Command": {"CommandId": command_id}},
    )
    return command_id


def send_command_stop_cwa(ssm_client_stub, node_id):
    command_id = str(uuid4())
    ssm_client_stub.add_response(
        "send_command",
        expected_params={
            "DocumentName": "AmazonCloudWatch-ManageAgent",
            "InstanceIds": [node_id],
            "MaxConcurrency": "1",
            "MaxErrors": "0",
            "Parameters": {
                "action": ["stop"],
                "mode": ["ec2"],
            },
        },
        service_response={"Command": {"CommandId": command_id}},
    )
    return command_id


def get_param_ssm_same(ssm_client_stub, ssm_param_name, cloudwatch_helper, config_type):
    command_id = str(uuid4())
    cw_value_json = (
        cloudwatch_helper.CLOUDWATCH_CONFIG_TYPE_TO_CONFIG_VARIABLE_REPLACE_FUNC.get(
            config_type
        )(config_type)
    )
    ssm_client_stub.add_response(
        "get_parameter",
        expected_params={"Name": ssm_param_name},
        service_response={"Parameter": {"Value": json.dumps(cw_value_json)}},
    )
    return command_id


def get_sha1_hash_of_cloudwatch_config_file(config_type, cloudwatch_helper):
    cw_value_file = cloudwatch_helper._sha1_hash_file(config_type)
    return cw_value_file


def get_param_ssm_different(ssm_client_stub, ssm_param_name):
    command_id = str(uuid4())
    ssm_client_stub.add_response(
        "get_parameter",
        expected_params={"Name": ssm_param_name},
        service_response={"Parameter": {"Value": "value"}},
    )
    return command_id


def get_param_ssm_exception(ssm_client_stub, ssm_param_name):
    command_id = str(uuid4())
    ssm_client_stub.add_client_error(
        "get_parameter",
        "ParameterNotFound",
        expected_params={"Name": ssm_param_name},
        response_meta={"Error": {"Code": "ParameterNotFound"}},
    )
    return command_id


def put_cluster_dashboard_success(cloudwatch_client_stub, cloudwatch_helper):
    widgets = []
    json_config_path = get_cloudwatch_dashboard_config_file_path()
    with open(json_config_path) as f:
        dashboard_config = json.load(f)

    for item in dashboard_config:
        item_out = cloudwatch_helper._replace_all_config_variables(
            item,
            cloudwatch_helper.node_id,
            cloudwatch_helper.cluster_name,
            cloudwatch_helper.provider_config["region"],
        )
        widgets.append(item_out)

    dashboard_name = cloudwatch_helper.cluster_name + "-" + "example-dashboard-name"
    cloudwatch_client_stub.add_response(
        "put_dashboard",
        expected_params={
            "DashboardName": dashboard_name,
            "DashboardBody": json.dumps({"widgets": widgets}),
        },
        service_response={"ResponseMetadata": {"HTTPStatusCode": 200}},
    )


def put_cluster_alarms_success(cloudwatch_client_stub, cloudwatch_helper):
    json_config_path = get_cloudwatch_alarm_config_file_path()
    with open(json_config_path) as f:
        data = json.load(f)
    for item in data:
        item_out = copy.deepcopy(item)
        cloudwatch_helper._replace_all_config_variables(
            item_out,
            cloudwatch_helper.node_id,
            cloudwatch_helper.cluster_name,
            cloudwatch_helper.provider_config["region"],
        )
        cloudwatch_client_stub.add_response(
            "put_metric_alarm",
            expected_params=item_out,
            service_response={"ResponseMetadata": {"HTTPStatusCode": 200}},
        )


def get_metric_alarm(cloudwatch_client_stub):
    cloudwatch_client_stub.add_response(
        "describe_alarms",
        expected_params={},
        service_response={"MetricAlarms": [{"AlarmName": "myalarm"}]},
    )


def delete_metric_alarms(cloudwatch_client_stub):
    cloudwatch_client_stub.add_response(
        "delete_alarms",
        expected_params={"AlarmNames": ["myalarm"]},
        service_response={"ResponseMetadata": {"HTTPStatusCode": 200}},
    )
