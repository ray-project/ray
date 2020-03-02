import os
from parameterized import parameterized
import unittest
import yaml

import ray.autoscaler
from ray.autoscaler.autoscaler import fillout_defaults, validate_config
from ray.test_utils import recursive_fnmatch

RAY_PATH = os.path.abspath(os.path.join(__file__, "../../"))
CONFIG_PATHS = recursive_fnmatch(
    os.path.join(RAY_PATH, "autoscaler"), "*.yaml")

CONFIG_PATHS += recursive_fnmatch(
    os.path.join(RAY_PATH, "tune/examples/"), "*.yaml")


class AutoscalingConfigTest(unittest.TestCase):
    def testValidateDefaultConfig(self):

        for config_path in CONFIG_PATHS:
            with open(config_path) as f:
                config = yaml.safe_load(f)
            config = fillout_defaults(config)
            try:
                validate_config(config)
            except Exception:
                self.fail("Config did not pass validation test!")

    @parameterized.expand([
        ["tests/project_files/yaml_validation/additional_property.yaml"],
        ["autoscaler/aws/development-example.yaml"],
        ["autoscaler/aws/example-full.yaml"],
        ["autoscaler/aws/example-gpu-docker.yaml"],
        ["autoscaler/aws/example-minimal.yaml"],
        ["autoscaler/gcp/example-full.yaml"],
        ["autoscaler/gcp/example-gpu-docker.yaml"],
        ["autoscaler/gcp/example-minimal.yaml"],
        ["autoscaler/kubernetes/example-full.yaml"],
        ["autoscaler/kubernetes/example-minimal.yaml"],
        ["autoscaler/local/development-example.yaml"],
        ["autoscaler/local/example-full.yaml"],
    ])
    def testInvalidConfig(self, config_path):
        config = yaml.safe_load(os.path.join(RAY_PATH, config_path))

        with open(os.path.join(os.path.dirname(ray.autoscaler.__file__), "ray-schema.json")) as f:
            schema = f.read() 

        try:
            jsonschema.validate(config, json.loads(schema))
            self.fail("Expected validation to fail")
        except:
            pass

if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
