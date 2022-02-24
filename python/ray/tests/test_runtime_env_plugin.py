import os
import tempfile

import pytest
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin

import ray

MY_PLUGIN_CLASS_PATH = "ray.tests.test_runtime_env_plugin.MyPlugin"


class MyPlugin(RuntimeEnvPlugin):
    env_key = "MY_PLUGIN_TEST_ENVIRONMENT_KEY"

    @staticmethod
    def validate(runtime_env_dict: dict) -> str:
        value = runtime_env_dict["plugins"][MY_PLUGIN_CLASS_PATH]
        if value == "fail":
            raise ValueError("not allowed")
        return value

    @staticmethod
    def modify_context(
        uri: str, plugin_config_dict: dict, ctx: RuntimeEnvContext
    ) -> None:
        ctx.env_vars[MyPlugin.env_key] = str(plugin_config_dict["env_value"])
        ctx.command_prefix.append(
            f"echo {plugin_config_dict['tmp_content']} > "
            f"{plugin_config_dict['tmp_file']}"
        )
        ctx.py_executable = (
            plugin_config_dict["prefix_command"] + " " + ctx.py_executable
        )


def test_simple_env_modification_plugin(ray_start_regular):
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
        f.options(runtime_env={"plugins": {MY_PLUGIN_CLASS_PATH: "fail"}}).remote()

    if os.name != "nt":
        output = ray.get(
            f.options(
                runtime_env={
                    "plugins": {
                        MY_PLUGIN_CLASS_PATH: {
                            "env_value": 42,
                            "tmp_file": tmp_file_path,
                            "tmp_content": "hello",
                            # See https://en.wikipedia.org/wiki/Nice_(Unix)
                            "prefix_command": "nice -n 19",
                        }
                    }
                }
            ).remote()
        )

        assert output == {"env_value": "42", "tmp_content": "hello", "nice": 19}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
