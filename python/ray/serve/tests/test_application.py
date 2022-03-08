import pytest
import sys

from ray import serve
from ray.serve.application import Application


class TestConfigureRuntimeEnv:
    @serve.deployment
    def f():
        pass

    @pytest.mark.parametrize("ray_actor_options", [None, {}])
    def test_empty_ray_actor_options(self, ray_actor_options):
        runtime_env = {
            "working_dir": "http://test.com",
            "pip": ["requests", "pendulum==2.1.2"],
        }
        deployment = TestConfigureRuntimeEnv.f.options(
            ray_actor_options=ray_actor_options
        )
        Application()._configure_runtime_env(deployment, runtime_env)
        assert deployment.ray_actor_options["runtime_env"] == runtime_env

    def test_overwrite_all_options(self):
        old_runtime_env = {
            "working_dir": "http://test.com",
            "pip": ["requests", "pendulum==2.1.2"],
        }
        new_runtime_env = {
            "working_dir": "http://new.com",
            "pip": [],
            "env_vars": {"test_var": "test"},
        }
        deployment = TestConfigureRuntimeEnv.f.options(
            ray_actor_options={"runtime_env": old_runtime_env}
        )
        Application()._configure_runtime_env(deployment, new_runtime_env)
        assert deployment.ray_actor_options["runtime_env"] == new_runtime_env

    def test_overwrite_some_options(self):
        old_runtime_env = {
            "working_dir": "http://new.com",
            "pip": [],
            "env_vars": {"test_var": "test"},
        }
        new_runtime_env = {
            "working_dir": "http://test.com",
            "pip": ["requests", "pendulum==2.1.2"],
        }
        merged_env = {
            "working_dir": "http://test.com",
            "pip": ["requests", "pendulum==2.1.2"],
            "env_vars": {"test_var": "test"},
        }
        deployment = TestConfigureRuntimeEnv.f.options(
            ray_actor_options={"runtime_env": old_runtime_env}
        )
        Application()._configure_runtime_env(deployment, new_runtime_env)
        assert deployment.ray_actor_options["runtime_env"] == merged_env

    def test_overwrite_no_options(self):
        runtime_env = {
            "working_dir": "http://test.com",
            "pip": ["requests", "pendulum==2.1.2"],
        }
        deployment = TestConfigureRuntimeEnv.f.options(
            ray_actor_options={"runtime_env": runtime_env}
        )
        Application()._configure_runtime_env(deployment, {})
        assert deployment.ray_actor_options["runtime_env"] == runtime_env


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))