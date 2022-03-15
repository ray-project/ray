import os
import tempfile
from time import sleep

import pytest
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.test_utils import wait_for_condition
from ray.exceptions import RuntimeEnvSetupError

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


MY_PLUGIN_FOR_HANG_CLASS_PATH = "ray.tests.test_runtime_env_plugin.MyPluginForHang"
my_plugin_setup_times = 0


# This plugin will hang when first setup, second setup will ok
class MyPluginForHang(RuntimeEnvPlugin):
    env_key = "MY_PLUGIN_FOR_HANG_TEST_ENVIRONMENT_KEY"

    @staticmethod
    def validate(runtime_env_dict: dict) -> str:
        return "True"

    @staticmethod
    def create(uri: str, runtime_env: dict, ctx: RuntimeEnvContext) -> float:
        global my_plugin_setup_times
        my_plugin_setup_times += 1

        # first setup
        if my_plugin_setup_times == 1:
            # sleep forever
            sleep(3600)

    @staticmethod
    def modify_context(
        uri: str, plugin_config_dict: dict, ctx: RuntimeEnvContext
    ) -> None:
        global my_plugin_setup_times
        ctx.env_vars[MyPluginForHang.env_key] = str(my_plugin_setup_times)


def test_plugin_hang(ray_start_regular):
    env_key = MyPluginForHang.env_key

    @ray.remote(num_cpus=0.1)
    def f():
        return os.environ[env_key]

    refs = [
        f.options(
            # Avoid hitting the cache of runtime_env
            runtime_env={"plugins": {MY_PLUGIN_FOR_HANG_CLASS_PATH: {"name": "f1"}}}
        ).remote(),
        f.options(
            runtime_env={"plugins": {MY_PLUGIN_FOR_HANG_CLASS_PATH: {"name": "f2"}}}
        ).remote(),
    ]

    def condition():
        for ref in refs:
            try:
                res = ray.get(ref, timeout=1)
                print("result:", res)
                assert int(res) == 2
                return True
            except Exception as error:
                print(f"Got error: {error}")
                pass
        return False

    wait_for_condition(condition, timeout=60)


DUMMY_PLUGIN_CLASS_PATH = "ray.tests.test_runtime_env_plugin.DummyPlugin"
HANG_PLUGIN_CLASS_PATH = "ray.tests.test_runtime_env_plugin.HangPlugin"
DISABLE_TIMEOUT_PLUGIN_CLASS_PATH = (
    "ray.tests.test_runtime_env_plugin.DiasbleTimeoutPlugin"
)


class DummyPlugin(RuntimeEnvPlugin):
    @staticmethod
    def validate(runtime_env_dict: dict) -> str:
        return 1


class HangPlugin(DummyPlugin):
    def create(
        uri: str, runtime_env: "RuntimeEnv", ctx: RuntimeEnvContext  # noqa: F821
    ) -> float:
        sleep(3600)


class DiasbleTimeoutPlugin(DummyPlugin):
    def create(
        uri: str, runtime_env: "RuntimeEnv", ctx: RuntimeEnvContext  # noqa: F821
    ) -> float:
        sleep(10)


def test_plugin_timeout(start_cluster):
    @ray.remote(num_cpus=0.1)
    def f():
        return True

    refs = [
        f.options(
            runtime_env={
                "plugins": {
                    HANG_PLUGIN_CLASS_PATH: {"name": "f1"},
                },
                "config": {"setup_timeout_seconds": 10},
            }
        ).remote(),
        f.options(
            runtime_env={"plugins": {DUMMY_PLUGIN_CLASS_PATH: {"name": "f2"}}}
        ).remote(),
        f.options(
            runtime_env={
                "plugins": {
                    HANG_PLUGIN_CLASS_PATH: {"name": "f3"},
                },
                "config": {"setup_timeout_seconds": -1},
            }
        ).remote(),
    ]

    def condition():
        good_fun_num = 0
        bad_fun_num = 0
        for ref in refs:
            try:
                res = ray.get(ref, timeout=1)
                print("result:", res)
                if res:
                    good_fun_num += 1
                return True
            except RuntimeEnvSetupError:
                bad_fun_num += 1
        return bad_fun_num == 1 and good_fun_num == 2

    wait_for_condition(condition, timeout=60)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
