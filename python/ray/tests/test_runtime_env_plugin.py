import asyncio
import logging
import os
from pathlib import Path
import time
from unittest import mock

import tempfile
import json
from typing import List

import pytest

import ray
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.test_utils import enable_external_redis, wait_for_condition
from ray.exceptions import RuntimeEnvSetupError
from ray.runtime_env.runtime_env import RuntimeEnv

MY_PLUGIN_CLASS_PATH = "ray.tests.test_runtime_env_plugin.MyPlugin"
MY_PLUGIN_NAME = "MyPlugin"


class MyPlugin(RuntimeEnvPlugin):
    name = MY_PLUGIN_NAME
    env_key = "MY_PLUGIN_TEST_ENVIRONMENT_KEY"

    @staticmethod
    def validate(runtime_env: RuntimeEnv) -> str:
        value = runtime_env[MY_PLUGIN_NAME]
        if value == "fail":
            raise ValueError("not allowed")
        return value

    def modify_context(
        self,
        uris: List[str],
        runtime_env: RuntimeEnv,
        ctx: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> None:
        plugin_config_dict = runtime_env[MY_PLUGIN_NAME]
        ctx.env_vars[MyPlugin.env_key] = str(plugin_config_dict["env_value"])
        ctx.command_prefix += [
            "echo",
            plugin_config_dict["tmp_content"],
            ">",
            plugin_config_dict["tmp_file"],
            "&&",
        ]
        ctx.py_executable = (
            plugin_config_dict["prefix_command"] + " " + ctx.py_executable
        )


@pytest.mark.parametrize(
    "set_runtime_env_plugins",
    [
        '[{"class":"' + MY_PLUGIN_CLASS_PATH + '"}]',
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

    async def create(
        self,
        uri: str,
        runtime_env: dict,
        ctx: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> float:
        global my_plugin_setup_times
        my_plugin_setup_times += 1

        # first setup
        if my_plugin_setup_times == 1:
            # sleep forever
            await asyncio.sleep(3600)

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
        '[{"class":"' + MY_PLUGIN_FOR_HANG_CLASS_PATH + '"}]',
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


class DummyPlugin(RuntimeEnvPlugin):
    name = DUMMY_PLUGIN_NAME

    @staticmethod
    def validate(runtime_env_dict: dict) -> str:
        return 1


class HangPlugin(DummyPlugin):
    name = HANG_PLUGIN_NAME

    async def create(
        self,
        uri: str,
        runtime_env: "RuntimeEnv",
        ctx: RuntimeEnvContext,
        logger: logging.Logger,  # noqa: F821
    ) -> float:
        await asyncio.sleep(3600)


@pytest.mark.parametrize(
    "set_runtime_env_plugins",
    [
        '[{"class":"' + DUMMY_PLUGIN_CLASS_PATH + '"},'
        '{"class":"' + HANG_PLUGIN_CLASS_PATH + '"}]',
    ],
    indirect=True,
)
@pytest.mark.skipif(enable_external_redis(), reason="Failing in redis mode.")
def test_plugin_timeout(set_runtime_env_plugins, start_cluster):
    @ray.remote(num_cpus=0.1)
    def f():
        return True

    refs = [
        f.options(
            runtime_env={
                HANG_PLUGIN_NAME: {"name": "f1"},
                "config": {"setup_timeout_seconds": 1},
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


PRIORITY_TEST_PLUGIN1_CLASS_PATH = (
    "ray.tests.test_runtime_env_plugin.PriorityTestPlugin1"
)
PRIORITY_TEST_PLUGIN1_NAME = "PriorityTestPlugin1"
PRIORITY_TEST_PLUGIN2_CLASS_PATH = (
    "ray.tests.test_runtime_env_plugin.PriorityTestPlugin2"
)
PRIORITY_TEST_PLUGIN2_NAME = "PriorityTestPlugin2"
PRIORITY_TEST_ENV_VAR_NAME = "PriorityTestEnv"


class PriorityTestPlugin1(RuntimeEnvPlugin):
    name = PRIORITY_TEST_PLUGIN1_NAME
    priority = 11
    env_value = " world"

    @staticmethod
    def validate(runtime_env_dict: dict) -> str:
        return None

    def modify_context(
        self,
        uris: List[str],
        plugin_config_dict: dict,
        ctx: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> None:
        if PRIORITY_TEST_ENV_VAR_NAME in ctx.env_vars:
            ctx.env_vars[PRIORITY_TEST_ENV_VAR_NAME] += PriorityTestPlugin1.env_value
        else:
            ctx.env_vars[PRIORITY_TEST_ENV_VAR_NAME] = PriorityTestPlugin1.env_value


class PriorityTestPlugin2(RuntimeEnvPlugin):
    name = PRIORITY_TEST_PLUGIN2_NAME
    priority = 10
    env_value = "hello"

    @staticmethod
    def validate(runtime_env_dict: dict) -> str:
        return None

    def modify_context(
        self,
        uris: List[str],
        plugin_config_dict: dict,
        ctx: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> None:
        if PRIORITY_TEST_ENV_VAR_NAME in ctx.env_vars:
            raise RuntimeError(
                f"Env var {PRIORITY_TEST_ENV_VAR_NAME} has been set to "
                f"{ctx.env_vars[PRIORITY_TEST_ENV_VAR_NAME]}."
            )
        ctx.env_vars[PRIORITY_TEST_ENV_VAR_NAME] = PriorityTestPlugin2.env_value


priority_test_plugin_config_without_priority = [
    {
        "class": PRIORITY_TEST_PLUGIN1_CLASS_PATH,
    },
    {
        "class": PRIORITY_TEST_PLUGIN2_CLASS_PATH,
    },
]


priority_test_plugin_config = [
    {
        "class": PRIORITY_TEST_PLUGIN1_CLASS_PATH,
        "priority": 1,
    },
    {
        "class": PRIORITY_TEST_PLUGIN2_CLASS_PATH,
        "priority": 0,
    },
]

priority_test_plugin_bad_config = [
    {
        "class": PRIORITY_TEST_PLUGIN1_CLASS_PATH,
        "priority": 0,
        # Only used to distinguish the bad config in test body.
        "tag": "bad",
    },
    {
        "class": PRIORITY_TEST_PLUGIN2_CLASS_PATH,
        "priority": 1,
    },
]


@pytest.mark.parametrize(
    "set_runtime_env_plugins",
    [
        json.dumps(priority_test_plugin_config_without_priority),
        json.dumps(priority_test_plugin_config),
        json.dumps(priority_test_plugin_bad_config),
    ],
    indirect=True,
)
def test_plugin_priority(set_runtime_env_plugins, ray_start_regular):
    config = set_runtime_env_plugins
    _, tmp_file_path = tempfile.mkstemp()

    @ray.remote
    def f():
        import os

        return os.environ.get(PRIORITY_TEST_ENV_VAR_NAME)

    if "bad" in config:
        with pytest.raises(RuntimeEnvSetupError, match="has been set"):
            value = ray.get(
                f.options(
                    runtime_env={
                        PRIORITY_TEST_PLUGIN1_NAME: {},
                        PRIORITY_TEST_PLUGIN2_NAME: {},
                    }
                ).remote()
            )
    else:
        value = ray.get(
            f.options(
                runtime_env={
                    PRIORITY_TEST_PLUGIN1_NAME: {},
                    PRIORITY_TEST_PLUGIN2_NAME: {},
                }
            ).remote()
        )
        assert value is not None
        assert value == "hello world"


def test_unexpected_field_warning(shutdown_only):
    """Test that an unexpected runtime_env field doesn't error."""
    ray.init(runtime_env={"unexpected_field": "value"})

    @ray.remote
    def f():
        return True

    # Run a task to trigger runtime_env creation.
    assert ray.get(f.remote())

    # Check that the warning is logged.
    session_dir = ray._private.worker.global_worker.node.address_info["session_dir"]
    dashboard_agent_log_path = Path(session_dir) / "logs" / "dashboard_agent.log"
    wait_for_condition(lambda: dashboard_agent_log_path.exists())
    with open(dashboard_agent_log_path, "r") as f:
        wait_for_condition(lambda: "unexpected_field is not recognized" in f.read())


URI_CACHING_TEST_PLUGIN_CLASS_PATH = (
    "ray.tests.test_runtime_env_plugin.UriCachingTestPlugin"
)
URI_CACHING_TEST_PLUGIN_NAME = "UriCachingTestPlugin"
URI_CACHING_TEST_DIR = Path(tempfile.gettempdir()) / "runtime_env_uri_caching_test"
uri_caching_test_file_path = URI_CACHING_TEST_DIR / "uri_caching_test_file.json"
URI_CACHING_TEST_DIR.mkdir(parents=True, exist_ok=True)
uri_caching_test_file_path.write_text("{}")


def get_plugin_usage_data():
    with open(uri_caching_test_file_path, "r") as f:
        data = json.loads(f.read())
        return data


class UriCachingTestPlugin(RuntimeEnvPlugin):
    """A plugin that fakes taking up local disk space when creating its environment.

    This plugin is used to test that the URI caching is working correctly.
    Example:
        runtime_env = {"UriCachingTestPlugin": {"uri": "file:///a", "size_bytes": 10}}
    """

    name = URI_CACHING_TEST_PLUGIN_NAME

    def __init__(self):
        # Keeps track of the "disk space" each URI takes up for the
        # UriCachingTestPlugin.
        self.uris_to_sizes = {}
        self.modify_context_call_count = 0
        self.create_call_count = 0

    def write_plugin_usage_data(self) -> None:
        with open(uri_caching_test_file_path, "w") as f:
            data = {
                "uris_to_sizes": self.uris_to_sizes,
                "modify_context_call_count": self.modify_context_call_count,
                "create_call_count": self.create_call_count,
            }
            f.write(json.dumps(data))

    def get_uris(self, runtime_env: "RuntimeEnv") -> List[str]:  # noqa: F811
        return [runtime_env[self.name]["uri"]]

    async def create(
        self,
        uri,
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> float:
        self.create_call_count += 1
        created_size_bytes = runtime_env[self.name]["size_bytes"]
        self.uris_to_sizes[uri] = created_size_bytes
        self.write_plugin_usage_data()
        return created_size_bytes

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> None:
        self.modify_context_call_count += 1
        self.write_plugin_usage_data()

    def delete_uri(self, uri: str, logger: logging.Logger) -> int:
        size = self.uris_to_sizes.pop(uri)
        self.write_plugin_usage_data()
        return size


# Set scope to "class" to force this to run before start_cluster, whose scope
# is "function".  We need these env vars to be set before Ray is started.
@pytest.fixture(scope="class")
def uri_cache_size_100_gb():
    var = f"RAY_RUNTIME_ENV_{URI_CACHING_TEST_PLUGIN_NAME}_CACHE_SIZE_GB".upper()
    with mock.patch.dict(
        os.environ,
        {
            var: "100",
        },
    ):
        print("Set URI cache size for UriCachingTestPlugin to 100 GB")
        yield


def gb_to_bytes(size_gb: int) -> int:
    return size_gb * 1024 * 1024 * 1024


class TestGC:
    @pytest.mark.parametrize(
        "set_runtime_env_plugins",
        [
            json.dumps([{"class": URI_CACHING_TEST_PLUGIN_CLASS_PATH}]),
        ],
        indirect=True,
    )
    def test_uri_caching(
        self, set_runtime_env_plugins, start_cluster, uri_cache_size_100_gb
    ):
        cluster, address = start_cluster

        ray.init(address=address)

        def reinit():
            ray.shutdown()
            # TODO(architkulkarni): Currently, reinit the driver will generate the same
            # job id. And if we reinit immediately after shutdown, raylet may
            # process new job started before old job finished in some cases. This
            # inconsistency could disorder the URI reference and delete a valid
            # runtime env. We sleep here to walk around this issue.
            time.sleep(5)
            ray.init(address=address)

        @ray.remote
        def f():
            return True

        # Run a task to trigger runtime_env creation.
        ref1 = f.options(
            runtime_env={
                URI_CACHING_TEST_PLUGIN_NAME: {
                    "uri": "file:///tmp/test_uri_1",
                    "size_bytes": gb_to_bytes(50),
                }
            }
        ).remote()
        ray.get(ref1)
        # Check that the URI was "created on disk".
        print(get_plugin_usage_data())
        wait_for_condition(
            lambda: get_plugin_usage_data()
            == {
                "uris_to_sizes": {"file:///tmp/test_uri_1": gb_to_bytes(50)},
                "modify_context_call_count": 1,
                "create_call_count": 1,
            }
        )

        # Shutdown ray to stop the worker and remove the runtime_env reference.
        reinit()

        # Run a task with a different runtime env.
        ref2 = f.options(
            runtime_env={
                URI_CACHING_TEST_PLUGIN_NAME: {
                    "uri": "file:///tmp/test_uri_2",
                    "size_bytes": gb_to_bytes(51),
                }
            }
        ).remote()
        ray.get(ref2)
        # This should delete the old URI and create a new one, because 50 + 51 > 100
        # and the cache size limit is 100.
        wait_for_condition(
            lambda: get_plugin_usage_data()
            == {
                "uris_to_sizes": {"file:///tmp/test_uri_2": gb_to_bytes(51)},
                "modify_context_call_count": 2,
                "create_call_count": 2,
            }
        )

        reinit()

        # Run a task with the cached runtime env, to check that the runtime env is not
        # created anew.
        ref3 = f.options(
            runtime_env={
                URI_CACHING_TEST_PLUGIN_NAME: {
                    "uri": "file:///tmp/test_uri_2",
                    "size_bytes": gb_to_bytes(51),
                }
            }
        ).remote()
        ray.get(ref3)
        # modify_context should still be called even if create() is not called.
        # Example: for a "conda" plugin, even if the conda env is already created
        # and cached, we still need to call modify_context to add "conda activate" to
        # the RuntimeEnvContext.command_prefix for the worker.
        wait_for_condition(
            lambda: get_plugin_usage_data()
            == {
                "uris_to_sizes": {"file:///tmp/test_uri_2": gb_to_bytes(51)},
                "modify_context_call_count": 3,
                "create_call_count": 2,
            }
        )

        reinit()

        # Run a task with a new runtime env
        ref4 = f.options(
            runtime_env={
                URI_CACHING_TEST_PLUGIN_NAME: {
                    "uri": "file:///tmp/test_uri_3",
                    "size_bytes": gb_to_bytes(10),
                }
            }
        ).remote()
        ray.get(ref4)
        # The last two URIs should still be present in the cache, because 51 + 10 < 100.
        wait_for_condition(
            lambda: get_plugin_usage_data()
            == {
                "uris_to_sizes": {
                    "file:///tmp/test_uri_2": gb_to_bytes(51),
                    "file:///tmp/test_uri_3": gb_to_bytes(10),
                },
                "modify_context_call_count": 4,
                "create_call_count": 3,
            }
        )


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
