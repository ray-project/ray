import copy
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.nsight import NSIGHT_DEFAULT_CONFIG, NsightPlugin

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Nsight requires Linux")


def _capture_nsight_args(context):
    # Exercise shell parsing at worker launch without requiring Nsight or a GPU.
    command = 'nsys() { printf "%s\\0" "$@"; }\n' + context.py_executable
    return subprocess.check_output(["bash", "-c", command], text=True).split("\0")[:-1]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "directory_name",
    ["logs with spaces", "logs'quotes", "logs$HOME", r"logs\backslash"],
)
async def test_nsight_preserves_logs_dir(tmp_path, monkeypatch, directory_name):
    logs_dir = tmp_path / directory_name
    plugin = NsightPlugin(str(tmp_path / "runtime_resources"), str(logs_dir))
    monkeypatch.setattr(
        plugin, "_check_nsight_script", AsyncMock(return_value=(True, None))
    )
    config = {"t": "cuda", "o": "trace report_%p"}
    original_config = copy.deepcopy(config)
    runtime_env = SimpleNamespace(nsight=lambda: config)
    context = RuntimeEnvContext()

    await plugin.create(None, runtime_env, context)
    plugin.modify_context([], runtime_env, context)

    assert _capture_nsight_args(context) == [
        "profile",
        "-t",
        "cuda",
        "-o",
        str(logs_dir / "nsight" / "trace report_%p"),
        "python",
    ]
    assert config == original_config


@pytest.mark.asyncio
async def test_nsight_default_report_stays_in_each_nodes_logs(tmp_path, monkeypatch):
    runtime_env = SimpleNamespace(nsight=lambda: "default")
    original_default = copy.deepcopy(NSIGHT_DEFAULT_CONFIG)
    # Isolate a mutation by the old implementation from other tests.
    monkeypatch.setattr(
        "ray._private.runtime_env.nsight.NSIGHT_DEFAULT_CONFIG",
        copy.deepcopy(original_default),
    )
    for name in ["first logs", "second logs"]:
        logs_dir = tmp_path / name
        plugin = NsightPlugin(str(tmp_path / "runtime_resources"), str(logs_dir))
        monkeypatch.setattr(
            plugin, "_check_nsight_script", AsyncMock(return_value=(True, None))
        )
        context = RuntimeEnvContext()
        await plugin.create(None, runtime_env, context)
        plugin.modify_context([], runtime_env, context)

        args = _capture_nsight_args(context)
        assert args[args.index("-o") + 1] == str(
            logs_dir / "nsight" / "worker_process_%p"
        )
        assert args[-1] == "python"


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
