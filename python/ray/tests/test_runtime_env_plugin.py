import os
import tempfile

import pytest
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import (RAY_PLUGIN_ENV_KEY_PREFIX,
                                             RuntimeEnvPlugin)

import ray


class MyPlugin(RuntimeEnvPlugin):
    entry_key = "my_test_plugin_key"
    env_key = "MY_PLUGIN_ENV_KEY"

    @staticmethod
    def validate(runtime_env_dict: dict) -> str:
        value = runtime_env_dict[MyPlugin.entry_key]
        if value == "fail":
            raise ValueError("not allowed")
        return value

    @staticmethod
    def modify_context(uri: str, runtime_env_dict: dict,
                       ctx: RuntimeEnvContext) -> None:
        plugin_config_dict = runtime_env_dict[MyPlugin.entry_key]
        ctx.env_vars[MyPlugin.env_key] = str(plugin_config_dict["env_value"])
        ctx.command_prefix.append(
            f"echo {plugin_config_dict['tmp_content']} > "
            f"{plugin_config_dict['tmp_file']}")
        ctx.py_executable = (
            plugin_config_dict['prefix_command'] + " " + ctx.py_executable)


@pytest.fixture
def _inject_plugin_env():
    key = RAY_PLUGIN_ENV_KEY_PREFIX + MyPlugin.entry_key
    os.environ[key] = "ray.tests.test_runtime_env_plugin.MyPlugin"
    yield
    del os.environ[key]


def test_simple_env_modification_plugin(_inject_plugin_env, ray_start_regular):
    _, tmp_file_path = tempfile.mkstemp()

    @ray.remote
    def f():
        import psutil
        with open(tmp_file_path, "r") as f:
            content = f.read().strip()
        return {
            "env_value": os.environ[MyPlugin.env_key],
            "tmp_content": content,
            "nice": psutil.Process().nice(),
        }

    with pytest.raises(ValueError, match="not allowed"):
        f.options(runtime_env={MyPlugin.entry_key: "fail"}).remote()

    output = ray.get(
        f.options(
            runtime_env={
                MyPlugin.entry_key: {
                    "env_value": 42,
                    "tmp_file": tmp_file_path,
                    "tmp_content": "hello",
                    "prefix_command": "nice -n 19",
                }
            }).remote())

    assert output == {"env_value": "42", "tmp_content": "hello", "nice": 19}


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-sv", __file__]))
