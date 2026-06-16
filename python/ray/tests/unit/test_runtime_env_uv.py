import sys
from unittest.mock import patch

import pytest

from ray._private.runtime_env import uv


class TestRuntimeEnv:
    def __init__(self, uv_config=None, env_vars=None):
        self._uv_config = uv_config or {"packages": ["requests"]}
        self._env_vars = env_vars or {}

    def uv_config(self):
        return self._uv_config

    def env_vars(self):
        return self._env_vars


@pytest.fixture
def mock_install_uv():
    with patch(
        "ray._private.runtime_env.uv.UvProcessor._install_uv"
    ) as mock_install_uv:
        mock_install_uv.return_value = None
        yield mock_install_uv


@pytest.fixture
def mock_install_uv_packages():
    with patch(
        "ray._private.runtime_env.uv.UvProcessor._install_uv_packages"
    ) as mock_install_uv_packages:
        mock_install_uv_packages.return_value = None
        yield mock_install_uv_packages


@pytest.mark.asyncio
async def test_run(mock_install_uv, mock_install_uv_packages):
    target_dir = "/tmp"
    runtime_env = TestRuntimeEnv()

    uv_processor = uv.UvProcessor(target_dir=target_dir, runtime_env=runtime_env)
    await uv_processor._run()


def test_expand_runtime_env_vars():
    env = {"WORKING_DIR": "/tmp/ray/session/working_dir", "PACKAGE_INDEX": "test"}

    assert (
        uv._expand_runtime_env_vars(
            "--requirements=${WORKING_DIR}/requirements.txt", env
        )
        == "--requirements=/tmp/ray/session/working_dir/requirements.txt"
    )
    assert uv._expand_runtime_env_vars("--index=$PACKAGE_INDEX", env) == "--index=test"
    assert (
        uv._expand_runtime_env_vars("--constraint=${UNKNOWN}/constraints.txt", env)
        == "--constraint=${UNKNOWN}/constraints.txt"
    )


@pytest.mark.asyncio
async def test_install_uv_packages_expands_install_options(tmp_path):
    working_dir = tmp_path / "working_dir"
    target_dir = tmp_path / "uv_env"
    exec_cwd = tmp_path / "exec_cwd"
    working_dir.mkdir()
    target_dir.mkdir()
    exec_cwd.mkdir()

    runtime_env = TestRuntimeEnv(
        uv_config={
            "packages": [],
            "uv_pip_install_options": [
                "--requirement=${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}/requirements.txt",
                "--constraint=$RAY_RUNTIME_ENV_CREATE_WORKING_DIR/constraints.txt",
            ],
        },
        env_vars={"RAY_RUNTIME_ENV_CREATE_WORKING_DIR": str(working_dir)},
    )
    uv_processor = uv.UvProcessor(target_dir=str(target_dir), runtime_env=runtime_env)
    captured_cmds = []

    async def fake_check_output_cmd(cmd, **kwargs):
        captured_cmds.append(cmd)
        return ""

    with patch.object(uv_processor, "_check_uv_existence", return_value=True), patch(
        "ray._private.runtime_env.uv.check_output_cmd", fake_check_output_cmd
    ):
        await uv_processor._install_uv_packages(
            str(target_dir),
            [],
            str(exec_cwd),
            uv_processor._uv_env,
            uv.default_logger,
        )

    assert len(captured_cmds) == 1
    assert f"--requirement={working_dir}/requirements.txt" in captured_cmds[0]
    assert f"--constraint={working_dir}/constraints.txt" in captured_cmds[0]
    assert not any(
        "RAY_RUNTIME_ENV_CREATE_WORKING_DIR" in arg for arg in captured_cmds[0]
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
