import sys
from unittest.mock import AsyncMock, patch

import pytest

from ray._private.runtime_env import uv


class FakeRuntimeEnv:
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
async def test_run(mock_install_uv, mock_install_uv_packages, tmp_path):
    target_dir = str(tmp_path)
    runtime_env = FakeRuntimeEnv()

    uv_processor = uv.UvProcessor(target_dir=target_dir, runtime_env=runtime_env)
    await uv_processor._run()


@pytest.mark.asyncio
async def test_install_uv_packages_expands_env_vars_from_install_env(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("UV_TEST_ROOT", "/wrong/process/path")
    expected_root = "/expected/runtime-env/path"
    runtime_env = FakeRuntimeEnv(
        uv_config={
            "packages": [
                "-r ${UV_TEST_ROOT}/requirements.txt",
                "demo-package==1.0",
            ],
            "uv_pip_install_options": ["--find-links=${UV_TEST_ROOT}/wheels"],
        }
    )
    uv_processor = uv.UvProcessor(target_dir=str(tmp_path), runtime_env=runtime_env)
    install_env = {"UV_TEST_ROOT": expected_root}

    with patch.object(
        uv_processor,
        "_check_uv_existence",
        new=AsyncMock(return_value=True),
    ):
        with patch(
            "ray._private.runtime_env.uv.check_output_cmd",
            new=AsyncMock(),
        ) as check_output_cmd:
            await uv_processor._install_uv_packages(
                str(tmp_path),
                runtime_env.uv_config()["packages"],
                str(tmp_path),
                install_env,
                uv.default_logger,
            )

    requirements_file = tmp_path / "ray_runtime_env_internal_pip_requirements.txt"
    assert requirements_file.read_text().splitlines() == [
        f"-r {expected_root}/requirements.txt",
        "demo-package==1.0",
    ]
    uv_install_cmd = check_output_cmd.await_args.args[0]
    assert f"--find-links={expected_root}/wheels" in uv_install_cmd


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
