import sys
from unittest.mock import MagicMock, patch

import pytest

from ray.dashboard.modules.reporter.jax_profile_manager import JaxProfilingManager


@pytest.fixture
def mock_profiler_client():
    mock_client = MagicMock()
    mock_profiler_module = MagicMock()
    mock_profiler_module.profiler_client = mock_client

    modules_to_patch = {
        "tensorflow": MagicMock(),
        "tensorflow.python": MagicMock(),
        "tensorflow.python.profiler": mock_profiler_module,
    }

    with patch.dict("sys.modules", modules_to_patch):
        yield mock_client


@pytest.mark.asyncio
async def test_jax_profile_success(tmp_path, mock_profiler_client):
    manager = JaxProfilingManager(tmp_path)

    # Mock success
    mock_profiler_client.trace.return_value = None

    success, output = await manager.jax_profile(pid=123, port=6000, duration_s=2)

    assert success
    assert "profiles" in output
    mock_profiler_client.trace.assert_called_once_with(
        "grpc://localhost:6000", logdir=str(tmp_path / "profiles"), duration_ms=2000
    )


@pytest.mark.asyncio
async def test_jax_profile_failure(tmp_path, mock_profiler_client):
    manager = JaxProfilingManager(tmp_path)

    # Mock failure
    mock_profiler_client.trace.side_effect = Exception("Connection failed")

    success, output = await manager.jax_profile(pid=123, port=6000, duration_s=2)

    assert not success
    assert "Failed to capture trace: Connection failed" in output


@pytest.mark.asyncio
async def test_jax_profile_no_tensorflow(tmp_path):
    manager = JaxProfilingManager(tmp_path)

    # Force ImportError on tensorflow
    with patch.dict("sys.modules", {"tensorflow": None}):
        success, output = await manager.jax_profile(pid=123, port=6000, duration_s=2)

        assert not success
        assert "TensorFlow is required" in output


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
