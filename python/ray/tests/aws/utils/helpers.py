import os
import yaml
import ray
import copy
from typing import Dict, Any

from ray.autoscaler._private.aws.node_provider import AWSNodeProvider
from ray.autoscaler.tags import (
    TAG_RAY_NODE_KIND,
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
    TAG_RAY_USER_NODE_TYPE,
    TAG_RAY_CLUSTER_NAME,
)
from ray.autoscaler._private.commands import prepare_config, validate_config
from ray.tests.aws.utils.constants import (
    DEFAULT_CLUSTER_NAME,
    DEFAULT_NODE_PROVIDER_INSTANCE_TAGS,
)
from ray.autoscaler._private.aws.cloudwatch.cloudwatch_helper import CloudwatchHelper


def get_aws_example_config_file_path(file_name):
    import ray.autoscaler.aws

    return os.path.join(os.path.dirname(ray.autoscaler.aws.__file__), file_name)


def load_aws_example_config_file(file_name):
    config_file_path = get_aws_example_config_file_path(file_name)
    return yaml.safe_load(open(config_file_path).read())


def fake_fillout_available_node_types_resources(config: Dict[str, Any]) -> None:
    """A cheap way to fill out the resources field (the same way a node
    provider would autodetect them) as far as schema validation is concerned."""
    available_node_types = config.get("available_node_types", {})
    for label, value in available_node_types.items():
        value["resources"] = value.get("resources", {"filler": 1})


def bootstrap_aws_config(config):
    config = prepare_config(config)
    fake_fillout_available_node_types_resources(config)
    validate_config(config)
    config["cluster_name"] = DEFAULT_CLUSTER_NAME
    return ray.autoscaler._private.aws.config.bootstrap_aws(config)


def bootstrap_aws_example_config_file(file_name):
    config = load_aws_example_config_file(file_name)
    return bootstrap_aws_config(config)


def node_provider_tags(config, type_name):
    """
    Returns a copy of DEFAULT_NODE_PROVIDER_INSTANCE_TAGS with the Ray node
    kind and Ray user node type filled in from the input config and node type
    name.

    Args:
        config: autoscaler config
        type_name: node type name
    Returns:
        tags: node provider tags
    """
    tags = copy.copy(DEFAULT_NODE_PROVIDER_INSTANCE_TAGS)
    head_name = config["head_node_type"]
    node_kind = NODE_KIND_HEAD if type_name is head_name else NODE_KIND_WORKER
    tags[TAG_RAY_NODE_KIND] = node_kind
    tags[TAG_RAY_USER_NODE_TYPE] = type_name
    return tags


def apply_node_provider_config_updates(config, node_cfg, node_type_name, max_count):
    """
    Applies default updates made by AWSNodeProvider to node_cfg during node
    creation. This should only be used for testing purposes.

    Args:
        config: autoscaler config
        node_cfg: node config
        node_type_name: node type name
        max_count: max nodes of the given type to launch
    """
    tags = node_provider_tags(config, node_type_name)
    tags[TAG_RAY_CLUSTER_NAME] = DEFAULT_CLUSTER_NAME
    user_tag_specs = node_cfg.get("TagSpecifications", [])
    tag_specs = [
        {
            "ResourceType": "instance",
            "Tags": [{"Key": k, "Value": v} for k, v in sorted(tags.items())],
        }
    ]
    node_provider_cfg_updates = {
        "MinCount": 1,
        "MaxCount": max_count,
        "TagSpecifications": tag_specs,
    }
    tags.pop(TAG_RAY_CLUSTER_NAME)
    node_cfg.update(node_provider_cfg_updates)
    # merge node provider tag specs with user overrides
    AWSNodeProvider._merge_tag_specs(tag_specs, user_tag_specs)


def get_cloudwatch_agent_config_file_path():
    return get_aws_example_config_file_path(
        "cloudwatch/example-cloudwatch-agent-config.json"
    )


def get_cloudwatch_dashboard_config_file_path():
    return get_aws_example_config_file_path(
        "cloudwatch/example-cloudwatch-dashboard-config.json"
    )


def get_cloudwatch_alarm_config_file_path():
    return get_aws_example_config_file_path(
        "cloudwatch/example-cloudwatch-alarm-config.json"
    )


def load_cloudwatch_example_config_file():
    config = load_aws_example_config_file("example-cloudwatch.yaml")
    cw_cfg = config["provider"]["cloudwatch"]
    cw_cfg["agent"]["config"] = get_cloudwatch_agent_config_file_path()
    cw_cfg["dashboard"]["config"] = get_cloudwatch_dashboard_config_file_path()
    cw_cfg["alarm"]["config"] = get_cloudwatch_alarm_config_file_path()
    return config


def get_cloudwatch_helper(node_ids):
    config = load_cloudwatch_example_config_file()
    config["cluster_name"] = DEFAULT_CLUSTER_NAME
    return CloudwatchHelper(
        config["provider"],
        node_ids,
        config["cluster_name"],
    )


def get_ssm_param_name(cluster_name, config_type):
    ssm_config_param_name = "AmazonCloudWatch-" + "ray_{}_config_{}".format(
        config_type, cluster_name
    )
    return ssm_config_param_name
