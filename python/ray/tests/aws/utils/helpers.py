import os
import yaml
import ray

from ray.autoscaler._private.commands import prepare_config, validate_config
from ray.tests.aws.utils.constants import DEFAULT_CLUSTER_NAME


def get_aws_example_config_file_path(file_name):
    import ray.autoscaler.aws
    return os.path.join(
        os.path.dirname(ray.autoscaler.aws.__file__), file_name)


def load_aws_example_config_file(file_name):
    config_file_path = get_aws_example_config_file_path(file_name)
    return yaml.safe_load(open(config_file_path).read())


def bootstrap_aws_config(config):
    config = prepare_config(config)
    validate_config(config)
    config["cluster_name"] = DEFAULT_CLUSTER_NAME
    return ray.autoscaler._private.aws.config.bootstrap_aws(config)


def bootstrap_aws_example_config_file(file_name):
    config = load_aws_example_config_file(file_name)
    return bootstrap_aws_config(config)
