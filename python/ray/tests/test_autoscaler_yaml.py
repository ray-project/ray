from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import unittest
import yaml

from ray.autoscaler.autoscaler import fillout_defaults, validate_config
from ray.tests.utils import recursive_fnmatch

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
