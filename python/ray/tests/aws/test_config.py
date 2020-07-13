import pytest
import ray.tests.aws.utils.stubs as stubs
from ray.autoscaler.aws.config import _merge_tag_specs, \
    _subnets_in_network_config, _security_groups_in_network_config


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


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
