import os
import sys
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray._private.utils import set_omp_num_threads_if_unset


def test_set_omp_num_threads_if_unset_updates_thread_pool_modules(monkeypatch):
    """Test that set_omp_num_threads_if_unset calls .set_num_threads() on loaded
    THREAD_POOL_MODULES.
    """
    monkeypatch.delenv("OMP_NUM_THREADS", raising=False)
    mock_ctx = MagicMock()
    mock_ctx.worker.mode = ray._private.worker.WORKER_MODE
    mock_ctx.get_assigned_resources.return_value = {"CPU": 4}
    mock_torch = MagicMock()
    with patch("ray.get_runtime_context", return_value=mock_ctx), patch.dict(
        sys.modules, {"torch": mock_torch}
    ):
        assert set_omp_num_threads_if_unset() is True
        assert os.environ["OMP_NUM_THREADS"] == "4"
        mock_torch.set_num_threads.assert_called_once_with(4)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
