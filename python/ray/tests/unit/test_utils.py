import os
import sys
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray._private.utils import set_num_threads_env_vars_if_unset


@pytest.mark.parametrize(
    "num_threads_env_vars,preset_omp,expected_ret,expected_torch_threads",
    [
        (None, None, ["OMP_NUM_THREADS"], 4),
        ("OMP_NUM_THREADS,RAYON_NUM_THREADS", "2", ["RAYON_NUM_THREADS"], None),
    ],
)
def test_set_num_threads_env_vars_if_unset_updates_thread_pool_modules(
    monkeypatch,
    num_threads_env_vars,
    preset_omp,
    expected_ret,
    expected_torch_threads,
):
    """Test that set_num_threads_env_vars_if_unset calls .set_num_threads() on loaded
    THREAD_POOL_MODULES only when OMP_NUM_THREADS is set by Ray.
    """
    if num_threads_env_vars is None:
        monkeypatch.delenv("RAY_NUM_THREADS_ENV_VARS", raising=False)
    else:
        monkeypatch.setenv("RAY_NUM_THREADS_ENV_VARS", num_threads_env_vars)
    if preset_omp is None:
        monkeypatch.delenv("OMP_NUM_THREADS", raising=False)
    else:
        monkeypatch.setenv("OMP_NUM_THREADS", preset_omp)
    monkeypatch.delenv("RAYON_NUM_THREADS", raising=False)

    mock_ctx = MagicMock()
    mock_ctx.worker.mode = ray._private.worker.WORKER_MODE
    mock_ctx.get_assigned_resources.return_value = {"CPU": 4}
    mock_torch = MagicMock()
    with patch("ray.get_runtime_context", return_value=mock_ctx), patch.dict(
        sys.modules, {"torch": mock_torch}
    ):
        assert set_num_threads_env_vars_if_unset() == expected_ret
        if expected_torch_threads is not None:
            assert os.environ["OMP_NUM_THREADS"] == str(expected_torch_threads)
            mock_torch.set_num_threads.assert_called_once_with(expected_torch_threads)
        else:
            assert os.environ["OMP_NUM_THREADS"] == preset_omp
            mock_torch.set_num_threads.assert_not_called()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
