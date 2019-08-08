from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from flaky import flaky
import os
import shutil
import tempfile
import threading
import time
import unittest
import yaml

CONFIG_PATHS = recursive_fnmatch(
    os.path.join(__file__, "../autoscaler/"), "*.yaml")


class AutoscalingConfigTest(unittest.TestCase):
    def testValidateDefaultConfig(self):

        from ray.tests.util import recursive_fnmatch
        from ray.autoscaler.autoscaler import StandardAutoscaler, LoadMetrics, \
            fillout_defaults, validate_config

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
