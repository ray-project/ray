import pytest

from ray.autoscaler._private.aws.config import _get_vpc_id_or_die
import ray.tests.aws.utils.stubs as stubs
import ray.tests.aws.utils.helpers as helpers
from ray.tests.aws.utils.constants import AUX_SUBNET, DEFAULT_SUBNET, \
    DEFAULT_SG_AUX_SUBNET, DEFAULT_SG, DEFAULT_SG_DUAL_GROUP_RULES, \
    DEFAULT_SG_WITH_RULES_AUX_SUBNET, AUX_SG, \
    DEFAULT_SG_WITH_NAME, DEFAULT_SG_WITH_NAME_AND_RULES, CUSTOM_IN_BOUND_RULES


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


def test_create_sg_with_custom_inbound_rules_and_name(iam_client_stub,
                                                      ec2_client_stub):
    # use default stubs to skip ahead to security group configuration
    stubs.skip_to_configure_sg(ec2_client_stub, iam_client_stub)

    # expect to describe the head subnet ID
    stubs.describe_subnets_echo(ec2_client_stub, DEFAULT_SUBNET)
    # given no existing security groups within the VPC...
    stubs.describe_no_security_groups(ec2_client_stub)
    # expect to create a security group on the head node VPC
    stubs.create_sg_echo(ec2_client_stub, DEFAULT_SG_WITH_NAME)
    # expect new head security group details to be retrieved after creation
    stubs.describe_sgs_on_vpc(
        ec2_client_stub,
        [DEFAULT_SUBNET["VpcId"]],
        [DEFAULT_SG_WITH_NAME],
    )

    # given custom existing default head security group inbound rules...
    # expect to authorize both default and custom inbound rules
    stubs.authorize_sg_ingress(
        ec2_client_stub,
        DEFAULT_SG_WITH_NAME_AND_RULES,
    )

    # given the prior modification to the head security group...
    # expect the next read of a head security group property to reload it
    stubs.describe_sg_echo(ec2_client_stub, DEFAULT_SG_WITH_NAME_AND_RULES)

    _get_vpc_id_or_die.cache_clear()
    # given our mocks and an example config file as input...
    # expect the config to be loaded, validated, and bootstrapped successfully
    config = helpers.bootstrap_aws_example_config_file(
        "example-security-group.yaml")

    # expect the bootstrapped config to have the custom security group...
    # name and in bound rules
    assert config["provider"]["security_group"][
        "GroupName"] == DEFAULT_SG_WITH_NAME_AND_RULES["GroupName"]
    assert config["provider"]["security_group"][
        "IpPermissions"] == CUSTOM_IN_BOUND_RULES

    # expect no pending responses left in IAM or EC2 client stub queues
    iam_client_stub.assert_no_pending_responses()
    ec2_client_stub.assert_no_pending_responses()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
