import jsonschema
import os
import sys
import tempfile
import unittest
import urllib
import yaml
import copy
from unittest.mock import MagicMock, Mock, patch

from ray.autoscaler._private.util import prepare_config, validate_config
from ray.test_utils import recursive_fnmatch

RAY_PATH = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
CONFIG_PATHS = recursive_fnmatch(
    os.path.join(RAY_PATH, "autoscaler"), "*.yaml")

CONFIG_PATHS += recursive_fnmatch(
    os.path.join(RAY_PATH, "tune", "examples"), "*.yaml")


class AutoscalingConfigTest(unittest.TestCase):
    def testValidateDefaultConfig(self):
        for config_path in CONFIG_PATHS:
            with open(config_path) as f:
                config = yaml.safe_load(f)
            config = prepare_config(config)
            try:
                validate_config(config)
            except Exception:
                self.fail("Config did not pass validation test!")

    def testValidateDefaultConfigAWSMultiNodeTypes(self):
        aws_config_path = os.path.join(
            RAY_PATH, "autoscaler/aws/example-multi-node-type.yaml")
        with open(aws_config_path) as f:
            config = yaml.safe_load(f)
            orig_config = copy.deepcopy(config)
        config = prepare_config(orig_config)
        try:
            validate_config(config)
        except Exception:
            self.fail("Config did not pass validation test!")

        new_config = copy.deepcopy(orig_config)
        # modify it here
        new_config["available_node_types"] = {
            "cpu_4_ondemand": new_config["available_node_types"][
                "cpu_4_ondemand"],
            "gpu_8_ondemand": new_config["available_node_types"][
                "gpu_8_ondemand"]
        }
        new_config["worker_default_node_type"] = "cpu_4_ondemand"
        orig_new_config = copy.deepcopy(new_config)
        available_node_types = new_config["available_node_types"]
        for node_type in available_node_types:
            del available_node_types[node_type]["resources"]
        boto3_dict = {
            "InstanceTypes": [{
                "InstanceType": "m4.xlarge",
                "VCpuInfo": {
                    "DefaultVCpus": 4
                }
            }, {
                "InstanceType": "p3.8xlarge",
                "VCpuInfo": {
                    "DefaultVCpus": 32
                },
                "GpuInfo": {
                    "Gpus": [{
                        "Name": "V100",
                        "Count": 4
                    }]
                }
            }]
        }
        boto3_mock = Mock()
        describe_instance_types_mock = Mock()
        describe_instance_types_mock.describe_instance_types = MagicMock(
            return_value=boto3_dict)
        boto3_mock.client = MagicMock(
            return_value=describe_instance_types_mock)
        with patch.multiple(
                "ray.autoscaler._private.aws.node_provider",
                boto3=boto3_mock,
        ):
            new_config = prepare_config(new_config)

        try:
            validate_config(new_config)
            orig_new_config["available_node_types"] == new_config[
                "available_node_types"]
        except Exception:
            self.fail("Config did not multi node types auto fill test!")

    def testValidateNetworkConfig(self):
        web_yaml = "https://raw.githubusercontent.com/ray-project/ray/" \
            "master/python/ray/autoscaler/aws/example-full.yaml"
        response = urllib.request.urlopen(web_yaml, timeout=5)
        content = response.read()
        with tempfile.TemporaryFile() as f:
            f.write(content)
            f.seek(0)
            config = yaml.safe_load(f)
        config = prepare_config(config)
        try:
            validate_config(config)
        except Exception:
            self.fail("Config did not pass validation test!")

    def _test_invalid_config(self, config_path):
        with open(os.path.join(RAY_PATH, config_path)) as f:
            config = yaml.safe_load(f)
        try:
            validate_config(config)
            self.fail("Expected validation to fail for {}".format(config_path))
        except jsonschema.ValidationError:
            pass

    @unittest.skipIf(sys.platform == "win32", "Failing on Windows.")
    def testInvalidConfig(self):
        self._test_invalid_config(
            os.path.join("tests", "additional_property.yaml"))


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
