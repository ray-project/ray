import logging
import os
import tempfile
from time import sleep
from typing import List

import pytest

import ray
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.test_utils import test_external_redis, wait_for_condition
from ray.exceptions import RuntimeEnvSetupError

MY_PLUGIN_CLASS_PATH = "ray.tests.test_runtime_env_plugin.MyPlugin"
MY_PLUGIN_NAME = "MyPlugin"


class MyPlugin(RuntimeEnvPlugin):
    name = MY_PLUGIN_NAME
    env_key = "MY_PLUGIN_TEST_ENVIRONMENT_KEY"

    @staticmethod
    def validate(runtime_env_dict: dict) -> str:
        value = runtime_env_dict[MY_PLUGIN_NAME]
        if value == "fail":
            raise ValueError("not allowed")
        return value

    def modify_context(
        self,
        uris: List[str],
        plugin_config_dict: dict,
        ctx: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> None:
        ctx.env_vars[MyPlugin.env_key] = str(plugin_config_dict["env_value"])
        ctx.command_prefix.append(
            f"echo {plugin_config_dict['tmp_content']} > "
            f"{plugin_config_dict['tmp_file']}"
        )
        ctx.py_executable = (
            plugin_config_dict["prefix_command"] + " " + ctx.py_executable
        )


@pytest.mark.parametrize(
    "set_runtime_env_plugins",
    [
        MY_PLUGIN_CLASS_PATH,
    ],
    indirect=True,
)
def test_simple_env_modification_plugin(set_runtime_env_plugins, ray_start_regular):
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

    with pytest.raises(RuntimeEnvSetupError, match="not allowed"):
        ray.get(f.options(runtime_env={MY_PLUGIN_NAME: "fail"}).remote())

    if os.name != "nt":
        output = ray.get(
            f.options(
                runtime_env={
                    MY_PLUGIN_NAME: {
                        "env_value": 42,
                        "tmp_file": tmp_file_path,
                        "tmp_content": "hello",
                        # See https://en.wikipedia.org/wiki/Nice_(Unix)
                        "prefix_command": "nice -n 19",
                    }
                }
            ).remote()
        )

        assert output == {"env_value": "42", "tmp_content": "hello", "nice": 19}


MY_PLUGIN_FOR_HANG_CLASS_PATH = "ray.tests.test_runtime_env_plugin.MyPluginForHang"
MY_PLUGIN_FOR_HANG_NAME = "MyPluginForHang"
my_plugin_setup_times = 0


# This plugin will hang when first setup, second setup will ok
class MyPluginForHang(RuntimeEnvPlugin):
    name = MY_PLUGIN_FOR_HANG_NAME
    env_key = "MY_PLUGIN_FOR_HANG_TEST_ENVIRONMENT_KEY"

    @staticmethod
    def validate(runtime_env_dict: dict) -> str:
        return "True"

    def create(self, uri: str, runtime_env: dict, ctx: RuntimeEnvContext) -> float:
        global my_plugin_setup_times
        my_plugin_setup_times += 1

        # first setup
        if my_plugin_setup_times == 1:
            # sleep forever
            sleep(3600)

    def modify_context(
        self,
        uris: List[str],
        plugin_config_dict: dict,
        ctx: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> None:
        global my_plugin_setup_times
        ctx.env_vars[MyPluginForHang.env_key] = str(my_plugin_setup_times)


@pytest.mark.parametrize(
    "set_runtime_env_plugins",
    [
        MY_PLUGIN_FOR_HANG_CLASS_PATH,
    ],
    indirect=True,
)
def test_plugin_hang(set_runtime_env_plugins, ray_start_regular):
    env_key = MyPluginForHang.env_key

    @ray.remote(num_cpus=0.1)
    def f():
        return os.environ[env_key]

    refs = [
        f.options(
            # Avoid hitting the cache of runtime_env
            runtime_env={MY_PLUGIN_FOR_HANG_NAME: {"name": "f1"}}
        ).remote(),
        f.options(runtime_env={MY_PLUGIN_FOR_HANG_NAME: {"name": "f2"}}).remote(),
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
DUMMY_PLUGIN_NAME = "DummyPlugin"
HANG_PLUGIN_CLASS_PATH = "ray.tests.test_runtime_env_plugin.HangPlugin"
HANG_PLUGIN_NAME = "HangPlugin"
DISABLE_TIMEOUT_PLUGIN_CLASS_PATH = (
    "ray.tests.test_runtime_env_plugin.DiasbleTimeoutPlugin"
)
DISABLE_TIMEOUT_PLUGIN_NAME = "test_plugin_timeout"


class DummyPlugin(RuntimeEnvPlugin):
    name = DUMMY_PLUGIN_NAME

    @staticmethod
    def validate(runtime_env_dict: dict) -> str:
        return 1


class HangPlugin(DummyPlugin):
    name = HANG_PLUGIN_NAME

    def create(
        self, uri: str, runtime_env: "RuntimeEnv", ctx: RuntimeEnvContext  # noqa: F821
    ) -> float:
        sleep(3600)


class DiasbleTimeoutPlugin(DummyPlugin):
    name = DISABLE_TIMEOUT_PLUGIN_NAME

    def create(
        self, uri: str, runtime_env: "RuntimeEnv", ctx: RuntimeEnvContext  # noqa: F821
    ) -> float:
        sleep(10)


@pytest.mark.parametrize(
    "set_runtime_env_plugins",
    [
        f"{DUMMY_PLUGIN_CLASS_PATH},"
        f"{HANG_PLUGIN_CLASS_PATH},"
        f"{DISABLE_TIMEOUT_PLUGIN_CLASS_PATH}",
    ],
    indirect=True,
)
@pytest.mark.skipif(test_external_redis(), reason="Failing in redis mode.")
def test_plugin_timeout(set_runtime_env_plugins, start_cluster):
    @ray.remote(num_cpus=0.1)
    def f():
        return True

    refs = [
        f.options(
            runtime_env={
                HANG_PLUGIN_NAME: {"name": "f1"},
                "config": {"setup_timeout_seconds": 10},
            }
        ).remote(),
        f.options(runtime_env={DUMMY_PLUGIN_NAME: {"name": "f2"}}).remote(),
        f.options(
            runtime_env={
                HANG_PLUGIN_NAME: {"name": "f3"},
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

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
