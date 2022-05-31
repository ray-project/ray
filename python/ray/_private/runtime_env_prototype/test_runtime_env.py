import pytest
from ray._private.runtime_env_prototype.sdk.runtime_env import RuntimeEnv
from ray._private.runtime_env_prototype.pip.sdk.pip import Pip
from ray._private.runtime_env_prototype.pluggability.plugin_manager import (
    RuntimeEnvPluginManager,
)
from ray.core.generated.common_pb2 import Language


def test_runtime_env():
    # Load plugins when the Ray node is started.
    plugins = [
        "ray._private.runtime_env_prototype.pip.pip_plugin.PipPlugin",
        "ray._private.runtime_env_prototype.working_dir.working_dir_plugin."
        "WorkingDirPlugin",
    ]
    RuntimeEnvPluginManager.load_plugins(plugins)

    # Construct runtime env in user code.
    # Strong-typed API
    runtime_env = RuntimeEnv()
    pip_runtime_env = Pip(packages=["requests"], pip_check=True)
    runtime_env.set("pip", pip_runtime_env)
    # Simple Dict API
    runtime_env["working_dir"] = "https://path/to/working_dir.zip"

    # The instance is both RuntimeEnv and dict.
    assert isinstance(runtime_env, RuntimeEnv)
    assert isinstance(runtime_env, dict)

    # Serialize it in caller worker side.
    serialized_runtime_env = runtime_env.serialize()

    # Deserialize it in runtime env agent.
    runtime_env_2 = RuntimeEnv.deserialize(serialized_runtime_env)

    # Make sure we can recover the instance.
    pip_runtime_env_2 = runtime_env_2.get("pip", Pip)
    pip_runtime_env_2 == pip_runtime_env
    runtime_env_2["working_dir"] == "https://path/to/working_dir.zip"
    assert runtime_env_2 == runtime_env

    # The instance is both RuntimeEnv and dict.
    assert isinstance(runtime_env_2, RuntimeEnv)
    assert isinstance(runtime_env_2, dict)

    # Setup runtime env in runtime env agent by the plugins.
    all_uris = []
    for name, plugin_config in runtime_env_2.items():
        uris, size, workerly, jobly = RuntimeEnvPluginManager.plugins[name].create(
            plugin_config, None, None, None, Language.PYTHON
        )
        all_uris.append(uris)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
