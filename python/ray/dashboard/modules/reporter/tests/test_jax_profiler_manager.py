import sys
from unittest.mock import MagicMock, patch

import pytest

from ray.dashboard.modules.reporter.jax_profile_manager import JaxProfilingManager
from ray.util.tpu import init_jax_profiler


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
    assert output.startswith("profiles")
    assert "123_" in output

    mock_profiler_client.trace.assert_called_once()
    call = mock_profiler_client.trace.call_args
    assert call.args[0] == "grpc://localhost:6000"
    assert call.kwargs["logdir"].startswith(str(tmp_path / "profiles"))
    assert call.kwargs["duration_ms"] == 2000


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


@patch("ray.util.tpu.os.getpid")
@patch("ray.util.tpu.os.getenv")
def test_setup_jax_profiler_success(mock_getenv, mock_getpid):
    mock_getenv.return_value = "9999"
    mock_getpid.return_value = 12345

    mock_jax = MagicMock()
    mock_worker = MagicMock()
    mock_worker.node.node_id = "mock_node_id_hex"

    with (
        patch.dict("sys.modules", {"jax": mock_jax}),
        patch("ray._private.worker.global_worker", mock_worker),
        patch("ray.experimental.internal_kv._internal_kv_put") as mock_kv_put,
    ):

        init_jax_profiler()

        mock_jax.profiler.start_server.assert_called_once_with(9999)
        import ray

        mock_kv_put.assert_called_once_with(
            "jax_profiler_port:mock_node_id_hex:12345",
            b"9999",
            namespace=ray._private.ray_constants.KV_NAMESPACE_DASHBOARD,
        )


def test_setup_jax_profiler_no_jax():
    with patch.dict("sys.modules", {"jax": None}):
        # Should skip starting profiler and not raise error
        init_jax_profiler()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
