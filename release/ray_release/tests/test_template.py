import sys
import unittest

from ray_release.config import Test
from ray_release.exception import ReleaseTestConfigError
from ray_release.template import populate_cluster_env_variables, render_yaml_template

TEST_APP_CONFIG_CPU = """
base_image: {{ env["RAY_IMAGE_NIGHTLY_CPU"] | default("anyscale/ray:nightly-py37") }}
env_vars: {}
debian_packages:
  - curl
"""

TEST_APP_CONFIG_GPU = """
base_image: {{ env["RAY_IMAGE_ML_NIGHTLY_GPU"] | default("anyscale/ray-ml:nightly-py37-gpu") }}
env_vars: {}
debian_packages:
  - curl
"""  # noqa: E501


class TemplateTest(unittest.TestCase):
    def testPythonVersionDefaultCPU(self):
        test = Test()

        env = populate_cluster_env_variables(test, ray_wheels_url="")
        result = render_yaml_template(TEST_APP_CONFIG_CPU, env=env)

        assert result["base_image"] == "anyscale/ray:nightly-py37"

    def testPythonVersion39CPU(self):
        test = Test(python="3.9")

        env = populate_cluster_env_variables(test, ray_wheels_url="")
        result = render_yaml_template(TEST_APP_CONFIG_CPU, env=env)

        assert result["base_image"] == "anyscale/ray:nightly-py39"

    def testPythonVersionDefaultGPU(self):
        test = Test()

        env = populate_cluster_env_variables(test, ray_wheels_url="")
        result = render_yaml_template(TEST_APP_CONFIG_GPU, env=env)

        assert result["base_image"] == "anyscale/ray-ml:nightly-py37-gpu"

    def testPythonVersion39GPU(self):
        test = Test(python="3.9")

        env = populate_cluster_env_variables(test, ray_wheels_url="")
        result = render_yaml_template(TEST_APP_CONFIG_GPU, env=env)

        assert result["base_image"] == "anyscale/ray-ml:nightly-py39-gpu"

    def testPythonVersionInvalid(self):
        test = Test(python="3.x")

        with self.assertRaises(ReleaseTestConfigError):
            populate_cluster_env_variables(test, ray_wheels_url="")


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
