import copy
import pytest

from ray.autoscaler.aws.config import _merge_tag_specs, \
    _subnets_in_network_config, _security_groups_in_network_config, \
    _configure_node_from_launch_template, _configure_security_group, \
    _configure_subnet

import ray.tests.aws.utils.stubs as stubs
from ray.tests.aws.utils.constants import DEFAULT_SUBNET, DEFAULT_SG, \
    DEFAULT_SG_WITH_RULES, DEFAULT_CLUSTER_NAME


def test_merge_tag_specs_both_empty():
    tag_specs = _merge_tag_specs([], [])
    expected = stubs.create_tag_specifications(["instance", "volume"], [])
    assert tag_specs == expected


def test_merge_tag_specs_config_overrides_template():
    config_tag_specs = stubs.create_tag_specifications(["instance", "volume"],
                                                       [{
                                                           "foo": "bar"
                                                       }])

    template_tag_specs = stubs.create_tag_specifications(
        ["instance", "volume"], [{
            "foo": "foo"
        }, {
            "bar": "bar"
        }])

    expected = stubs.create_tag_specifications(["instance", "volume"], [{
        "foo": "bar"
    }, {
        "bar": "bar"
    }])

    tag_specs = _merge_tag_specs(config_tag_specs, template_tag_specs)
    assert tag_specs == expected


def test_subnets_in_network_config_no_network_interfaces():
    config = {}
    subnets = _subnets_in_network_config(config)
    assert subnets == []


def test_subnets_in_network_config_partial_subnets():
    config = {
        "NetworkInterfaces": [{}, {
            "SubnetId": "foo"
        }, {
            "SubnetId": "bar"
        }]
    }
    subnets = _subnets_in_network_config(config)
    assert subnets.sort() == ["foo", "", "bar"].sort()


def test_security_groups_in_network_config_no_network_interfaces():
    config = {}
    security_groups = _security_groups_in_network_config(config)
    assert security_groups == []


def test_security_groups_in_network_config_partial_groups():
    config = {
        "NetworkInterfaces": [{}, {
            "Groups": ["foo"]
        }, {
            "Groups": ["bar"]
        }]
    }
    security_groups = _security_groups_in_network_config(config)
    assert security_groups.sort() == [["foo"], [], ["bar"]].sort()


def test_configure_from_launch_template_no_template():
    config = {"head_node": {}}
    expected = {"head_node": {}}
    config = _configure_node_from_launch_template(config, "head_node")
    assert config == expected


@pytest.mark.parametrize("key", [("LaunchTemplateName"), ("LaunchTemplateId")])
def test_configure_from_launch_template(ec2_client_stub, key):
    config = {
        "provider": {
            "type": "aws",
            "region": "us-west-2"
        },
        "head_node": {
            "LaunchTemplate": {
                key: "foo",
                "Version": "1"
            }
        }
    }

    stubs.describe_launch_template_versions_echo(ec2_client_stub)
    new_config = _configure_node_from_launch_template(config, "head_node")
    assert new_config["head_node"]["IamInstanceProfile"]["Name"] == "foo"
    assert new_config["head_node"]["KeyName"] == "foo"
    assert new_config["head_node"]["ImageId"] == "foo"
    assert new_config["head_node"]["NetworkInterfaces"][0]["SubnetId"] == "foo"
    assert new_config["head_node"]["TagSpecifications"][0]["Tags"][0][
        "Value"] == "foo"


def test_configure_from_launch_template_config_overrides(ec2_client_stub):
    config = {
        "provider": {
            "type": "aws",
            "region": "us-west-2"
        },
        "head_node": {
            "LaunchTemplate": {
                "LaunchTemplateId": "foo",
                "Version": "1"
            },
            "IamInstanceProfile": {
                "Name": "bar"
            },
            "KeyName": "bar",
            "ImageId": "bar",
            "NetworkInterfaces": [{
                "SubnetId": "bar",
                "Groups": ["bar"]
            }],
            "TagSpecifications": [{
                "ResourceType": "instance",
                "Tags": [{
                    "Key": "Name",
                    "Value": "bar"
                }]
            }, {
                "ResourceType": "volume",
                "Tags": [{
                    "Key": "Name",
                    "Value": "bar"
                }]
            }]
        }
    }

    stubs.describe_launch_template_versions_echo(ec2_client_stub)
    new_config = _configure_node_from_launch_template(config, "head_node")
    assert new_config["head_node"]["IamInstanceProfile"]["Name"] == "bar"
    assert new_config["head_node"]["KeyName"] == "bar"
    assert new_config["head_node"]["ImageId"] == "bar"
    assert new_config["head_node"]["NetworkInterfaces"][0]["SubnetId"] == "bar"
    assert new_config["head_node"]["TagSpecifications"][0]["Tags"][0][
        "Value"] == "bar"


@pytest.mark.parametrize("key,value", [("SubnetIds", ["us-west-2a"]),
                                       ("SubnetId", "us-west-2a"),
                                       ("SecurityGroupIds", ["foo"])])
def test_configure_from_launch_template_network_interface_exceptions(
        ec2_client_stub, key, value):
    config = {
        "provider": {
            "type": "aws",
            "region": "us-west-2"
        },
        "head_node": {
            "LaunchTemplate": {
                "LaunchTemplateId": "foo",
                "Version": "1"
            },
            key: value
        }
    }

    stubs.describe_launch_template_versions_echo(ec2_client_stub)
    with pytest.raises(Exception) as x:
        _ = _configure_node_from_launch_template(config, "head_node")
        assert "part of the NetworkInterface" in str(x.value)


def test_configure_from_launch_template_missing_subnet_id_exception(
        ec2_client_stub):
    config = {
        "provider": {
            "type": "aws",
            "region": "us-west-2"
        },
        "head_node": {
            "LaunchTemplate": {
                "LaunchTemplateId": "foo",
                "Version": "1"
            }
        }
    }

    stubs.describe_launch_template_versions_echo(
        ec2_client_stub, has_subnet=False)
    with pytest.raises(Exception) as x:
        _ = _configure_node_from_launch_template(config, "head_node")
        assert "missing a subnet" in str(x.value)


def test_configure_security_group_with_network_interfaces_with_sg():
    config = {
        "head_node": {
            "NetworkInterfaces": [{
                "SubnetId": "bar",
                "Groups": ["bar"]
            }]
        },
        "worker_nodes": {
            "NetworkInterfaces": [{
                "SubnetId": "bar",
                "Groups": ["bar"]
            }]
        }
    }

    expected = copy.deepcopy(config)
    config = _configure_security_group(config)
    assert config == expected


def test_configure_security_group_with_network_interfaces_without_sg(
        ec2_client_stub):
    config = {
        "cluster_name": DEFAULT_CLUSTER_NAME,
        "provider": {
            "type": "aws",
            "region": "us-west-2"
        },
        "head_node": {
            "NetworkInterfaces": [{
                "SubnetId": DEFAULT_SUBNET["SubnetId"]
            }]
        },
        "worker_nodes": {
            "NetworkInterfaces": [{
                "SubnetId": DEFAULT_SUBNET["SubnetId"],
                "Groups": []
            }]
        }
    }

    stubs.describe_subnets_echo(ec2_client_stub, DEFAULT_SUBNET)
    stubs.describe_sgs_on_vpc(ec2_client_stub, [DEFAULT_SG["VpcId"]],
                              [DEFAULT_SG])
    stubs.authorize_sg_ingress(ec2_client_stub, DEFAULT_SG_WITH_RULES)
    stubs.describe_sg_echo(ec2_client_stub, DEFAULT_SG_WITH_RULES)

    new_config = _configure_security_group(config)
    assert new_config["head_node"]["NetworkInterfaces"][0]["Groups"] == [
        DEFAULT_SG["GroupId"]
    ]
    assert new_config["worker_nodes"]["NetworkInterfaces"][0]["Groups"] == [
        DEFAULT_SG["GroupId"]
    ]


def test_configure_subnet_with_network_interfaces_with_subnet(ec2_client_stub):
    config = {
        "provider": {
            "type": "aws",
            "region": "us-west-2"
        },
        "head_node": {
            "NetworkInterfaces": [{
                "SubnetId": DEFAULT_SUBNET["SubnetId"]
            }]
        },
        "worker_nodes": {
            "NetworkInterfaces": [{
                "SubnetId": DEFAULT_SUBNET["SubnetId"]
            }]
        }
    }

    stubs.configure_subnet_default(ec2_client_stub)
    expected = copy.deepcopy(config)
    config = _configure_subnet(config)
    assert expected == config


@pytest.mark.parametrize("node_type", [("head_node"), ("worker_nodes")])
def test_configure_subnet_with_network_interfaces_missing_subnet_errors(
        ec2_client_stub, node_type):
    empty = {}
    default = {"SubnetId": DEFAULT_SUBNET["SubnetId"]}
    config = {
        "provider": {
            "type": "aws",
            "region": "us-west-2"
        },
        "head_node": {
            "NetworkInterfaces": [
                empty if node_type == "head_node" else default
            ]
        },
        "worker_nodes": {
            "NetworkInterfaces": [
                empty if node_type == "worker_nodes" else default
            ]
        }
    }

    stubs.configure_subnet_default(ec2_client_stub)

    with pytest.raises(Exception) as x:
        _ = _configure_subnet(config)
        assert "missing a subnet" in str(x.value)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
