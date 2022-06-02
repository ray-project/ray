import pytest
from ray._private.runtime_env_prototype.sdk.runtime_env import RuntimeEnv
from ray._private.runtime_env_prototype.pip.sdk.pip import Pip
from ray._private.runtime_env_prototype.pluggability.plugin_manager import (
    RuntimeEnvPluginManager,
)
from ray.core.generated.common_pb2 import Language
import json


def test_runtime_env():
    # [runtime env agent] Load plugins when the Ray node is started.
    plugins = [
        "ray._private.runtime_env_prototype.pip.pip_plugin.PipPlugin",
        "ray._private.runtime_env_prototype.working_dir.working_dir_plugin."
        "WorkingDirPlugin",
    ]
    RuntimeEnvPluginManager.load_plugins(plugins)

    # [user code] Construct runtime env in user code.
    # Strong-typed API
    runtime_env = RuntimeEnv()
    pip_runtime_env = Pip(packages=["requests"], pip_check=True)
    runtime_env.set("pip", pip_runtime_env)
    # Simple Dict API
    runtime_env["working_dir"] = "https://path/to/working_dir.zip"

    # The instance is both RuntimeEnv and dict.
    assert isinstance(runtime_env, RuntimeEnv)
    assert isinstance(runtime_env, dict)

    # [Worker internal] Serialize it in caller worker side.
    serialized_runtime_env = runtime_env.serialize()

    # [Worker internal] Get current runtime env for users
    current_runtime_env = RuntimeEnv.deserialize(serialized_runtime_env)
    # Make sure we can recover the instance.
    current_pip_runtime_env = current_runtime_env.get("pip", Pip)
    current_pip_runtime_env == pip_runtime_env
    current_runtime_env["working_dir"] == "https://path/to/working_dir.zip"
    assert current_runtime_env == runtime_env
    # The instance is both RuntimeEnv and dict.
    assert isinstance(current_runtime_env, RuntimeEnv)
    assert isinstance(current_runtime_env, dict)

    # [runtime env agent] parse json string
    runtime_env_in_agent = json.loads(serialized_runtime_env)
    all_uris = []
    for name, plugin_config in runtime_env_in_agent.items():
        # Setup runtime env in runtime env agent by the plugins.
        uris = RuntimeEnvPluginManager.plugins[name].validate(plugin_config)
        all_uris.append(uris)
        size, workerly, jobly = RuntimeEnvPluginManager.plugins[name].create(
            uris, plugin_config, None, None, None, Language.PYTHON
        )
    assert len(all_uris) == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
