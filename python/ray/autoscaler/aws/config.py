from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


def bootstrap_aws(config):
    config = _get_or_create_iam_role(config)
    config = _get_or_create_key_pair(config)
    config = _get_or_create_security_group(config)
    return config


def _get_or_create_iam_role(config):
    assert "IamInstanceProfile" in config["head_node"]  # TODO auto-create
    return config


def _get_or_create_security_group(config):
    assert "SecurityGroupIds" in config["head_node"]  # TODO auto-create
    assert "SecurityGroupIds" in config["node"]  # TODO auto-create
    return config


def _get_or_create_key_pair(config):
    assert "KeyName" in config["head_node"]  # TODO auto-create
    assert "KeyName" in config["node"]  # TODO auto-create
    return config
