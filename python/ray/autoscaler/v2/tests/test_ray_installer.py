# coding: utf-8
import os
import sys
import unittest

import pytest  # noqa

from ray._private.test_utils import load_test_config
from ray.autoscaler.tags import TAG_RAY_NODE_KIND
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
from ray.autoscaler.v2.instance_manager.ray_installer import RayInstaller
from ray.core.generated.instance_manager_pb2 import Instance
from ray.tests.autoscaler_test_utils import MockProcessRunner, MockProvider


class RayInstallerTest(unittest.TestCase):
    def setUp(self):
        self.base_provider = MockProvider()
        self.config = AutoscalingConfig(load_test_config("test_ray_complex.yaml"))
        self.runner = MockProcessRunner()
        self.ray_installer = RayInstaller(self.base_provider, self.config, self.runner)

    def test_install_succeeded(self):
        self.base_provider.create_node({}, {TAG_RAY_NODE_KIND: "worker_nodes1"}, 1)
        self.runner.respond_to_call("json .Config.Env", ["[]" for i in range(1)])

        self.ray_installer.install_ray(
            Instance(
                instance_id="0", instance_type="worker_nodes1", cloud_instance_id="0"
            ),
            head_node_ip="1.2.3.4",
        )

    def test_install_failed(self):
        # creation failed because no such node.
        with self.assertRaisesRegex(KeyError, "0"):
            assert not self.ray_installer.install_ray(
                Instance(
                    instance_id="0",
                    instance_type="worker_nodes1",
                    cloud_instance_id="0",
                ),
                head_node_ip="1.2.3.4",
            )

        self.base_provider.create_node({}, {TAG_RAY_NODE_KIND: "worker_nodes1"}, 1)
        self.runner.fail_cmds = [
            "echo"  # this is the command used in the test_ray_complex.yaml
        ]
        self.runner.respond_to_call("json .Config.Env", ["[]" for i in range(1)])

        # creation failed because setup command failed.
        with self.assertRaisesRegex(Exception, "unexpected status"):
            self.ray_installer.install_ray(
                Instance(
                    instance_id="0",
                    instance_type="worker_nodes1",
                    cloud_instance_id="0",
                ),
                head_node_ip="1.2.3.4",
            )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
