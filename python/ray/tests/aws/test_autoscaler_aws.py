import pytest

import ray.tests.aws.utils.stubs as stubs
import ray.tests.aws.utils.helpers as helpers
from ray.tests.aws.utils.constants import AUX_SUBNET, DEFAULT_SUBNET, \
    DEFAULT_SG_AUX_SUBNET, DEFAULT_SG, DEFAULT_SG_DUAL_GROUP_RULES, \
    DEFAULT_SG_WITH_RULES_AUX_SUBNET, DEFAULT_SG_WITH_RULES, AUX_SG


def test_create_sg_different_vpc_same_rules(iam_client_stub, ec2_client_stub):
    # use default stubs to skip ahead to security group configuration
    stubs.skip_to_configure_sg(ec2_client_stub, iam_client_stub)

    # given head and worker nodes with custom subnets defined...
    # expect to first describe the worker subnet ID
    stubs.describe_subnets_echo(ec2_client_stub, AUX_SUBNET)
    # expect to second describe the head subnet ID
    stubs.describe_subnets_echo(ec2_client_stub, DEFAULT_SUBNET)
    # given no existing security groups within the VPC...
    stubs.describe_no_security_groups(ec2_client_stub)
    # expect to first create a security group on the worker node VPC
    stubs.create_sg_echo(ec2_client_stub, DEFAULT_SG_AUX_SUBNET)
    # expect new worker security group details to be retrieved after creation
    stubs.describe_sgs_on_vpc(
        ec2_client_stub,
        [AUX_SUBNET["VpcId"]],
        [DEFAULT_SG_AUX_SUBNET],
    )
    # expect to second create a security group on the head node VPC
    stubs.create_sg_echo(ec2_client_stub, DEFAULT_SG)
    # expect new head security group details to be retrieved after creation
    stubs.describe_sgs_on_vpc(
        ec2_client_stub,
        [DEFAULT_SUBNET["VpcId"]],
        [DEFAULT_SG],
    )

    # given no existing default head security group inbound rules...
    # expect to authorize all default head inbound rules
    stubs.authorize_sg_ingress(
        ec2_client_stub,
        DEFAULT_SG_DUAL_GROUP_RULES,
    )
    # given no existing default worker security group inbound rules...
    # expect to authorize all default worker inbound rules
    stubs.authorize_sg_ingress(
        ec2_client_stub,
        DEFAULT_SG_WITH_RULES_AUX_SUBNET,
    )

    # given the prior modification to the head security group...
    # expect the next read of a head security group property to reload it
    stubs.describe_sg_echo(ec2_client_stub, DEFAULT_SG_WITH_RULES)
    # given the prior modification to the worker security group...
    # expect the next read of a worker security group property to reload it
    stubs.describe_sg_echo(ec2_client_stub, DEFAULT_SG_WITH_RULES_AUX_SUBNET)
    # given our mocks and an example config file as input...
    # expect the config to be loaded, validated, and bootstrapped successfully
    config = helpers.bootstrap_aws_example_config_file("example-subnets.yaml")

    # expect the bootstrapped config to show different head and worker security
    # groups residing on different subnets
    assert config["head_node"]["SecurityGroupIds"] == [DEFAULT_SG["GroupId"]]
    assert config["head_node"]["SubnetIds"] == [DEFAULT_SUBNET["SubnetId"]]
    assert config["worker_nodes"]["SecurityGroupIds"] == [AUX_SG["GroupId"]]
    assert config["worker_nodes"]["SubnetIds"] == [AUX_SUBNET["SubnetId"]]

    # expect no pending responses left in IAM or EC2 client stub queues
    iam_client_stub.assert_no_pending_responses()
    ec2_client_stub.assert_no_pending_responses()


def test_cloudwatch_agent_setup(ec2_client_stub, ssm_client_stub):
    # create test cluster node IDs and an associated cloudwatch helper
    node_ids = ["i-abc", "i-def"]
    cloudwatch_helper = helpers.get_cloudwatch_helper(node_ids)

    # given a directive to install CloudWatch Agent on all nodes...
    # expect to wait for each EC2 instance status to report on OK state
    stubs.describe_instance_status_ok(ec2_client_stub, node_ids)
    # given all cluster EC2 instance status checks passed...
    # expect to send a CloudWatch Agent install command to all nodes via SSM
    stubs.send_command_cwa_install(ssm_client_stub, node_ids)
    # given a CloudWatch Agent install command sent to all nodes...
    # expect to wait for the command to complete successfully on every node
    stubs.get_command_invocation_success(ssm_client_stub, node_ids)
    # given a successful CloudWatch Agent install on all nodes...
    # expect to store the CloudWatch Agent config as an SSM parameter
    stubs.put_parameter_cloudwatch_agent_config(ssm_client_stub)
    # given a successful CloudWatch Agent install on all nodes...
    # expect to send a command to satisfy CWA collectd preconditions via SSM
    stubs.send_command_cwa_collectd_setup_script(ssm_client_stub, node_ids)
    # given that all CloudWatch Agent start preconditions are satisfied...
    # expect to send an SSM command to start CloudWatch Agent on all nodes
    stubs.send_command_start_cwa(ssm_client_stub, node_ids)

    # given our mocks and the example CloudWatch Agent config as input...
    # expect CloudWatch Agent to be installed on each cluster node successfully
    cloudwatch_helper.ssm_install_cloudwatch_agent()

    # expect no pending responses left in client stub queues
    ec2_client_stub.assert_no_pending_responses()
    ssm_client_stub.assert_no_pending_responses()


def test_cloudwatch_dashboard_creation(cloudwatch_client_stub):
    # create test cluster node IDs and an associated cloudwatch helper
    node_ids = ["i-abc", "i-def"]
    cloudwatch_helper = helpers.get_cloudwatch_helper(node_ids)

    # given a directive to create a cluster CloudWatch dashboard...
    # expect to make a call to create a dashboard for each node in the cluster
    stubs.put_cluster_dashboard_success(
        cloudwatch_client_stub,
        cloudwatch_helper,
    )

    # given our mocks and the example cloudwatch dashboard config as input...
    # expect a cluster CloudWatch dashboard to be created successfully
    cloudwatch_helper.put_cloudwatch_dashboard()

    # expect no pending responses left in the CloudWatch client stub queue
    cloudwatch_client_stub.assert_no_pending_responses()


def test_cloudwatch_alarm_creation(cloudwatch_client_stub):
    # create test cluster node IDs and an associated cloudwatch helper
    node_ids = ["i-abc", "i-def"]
    cloudwatch_helper = helpers.get_cloudwatch_helper(node_ids)

    # given a directive to create cluster CloudWatch alarms...
    # expect to make a call to create alarms for each node in the cluster
    stubs.put_cluster_alarms_success(cloudwatch_client_stub, cloudwatch_helper)

    # given our mocks and the example cloudwatch alarm config as input...
    # expect cluster alarms to be created successfully
    cloudwatch_helper.put_cloudwatch_alarm()

    # expect no pending responses left in the CloudWatch client stub queue
    cloudwatch_client_stub.assert_no_pending_responses()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-s", __file__]))
