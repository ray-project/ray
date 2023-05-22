# coding: utf-8
import copy
import os
import sys
import yaml

import pytest  # noqa

from ray.autoscaler.v2.instance_manager.config import NodeProviderConfig


def test_node_provider_pass_through():
    config_yaml = yaml.safe_load("")
    config = NodeProviderConfig(config_yaml)


    