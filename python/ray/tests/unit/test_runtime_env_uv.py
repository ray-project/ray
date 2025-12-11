import sys
from unittest.mock import patch

import pytest

from ray._private.runtime_env import uv


class TestRuntimeEnv:
    def uv_config(self):
        return {"packages": ["requests"]}

    def env_vars(self):
        return {}


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
