import os
import sys
from unittest.mock import MagicMock, patch

import pytest

import ray
import ray._private.utils as ray_utils
from ray._private.utils import (
    set_num_threads_env_vars_if_unset,
    set_thread_pool_module_threads,
)


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


def test_set_thread_pool_module_threads_resizes_cached_native_pools(monkeypatch):
    """Verify cached native setters resize while honoring explicit env vars."""
    omp_fn = MagicMock()
    openblas_fn = MagicMock()
    mkl_fn = MagicMock()
    monkeypatch.setattr(
        ray_utils,
        "_cached_native_thread_pool_setters",
        [
            ("omp", object(), omp_fn),
            ("openblas", object(), openblas_fn),
            ("mkl", object(), mkl_fn),
        ],
    )
    monkeypatch.delenv("OPENBLAS_NUM_THREADS", raising=False)
    monkeypatch.setenv("MKL_NUM_THREADS", "2")

    set_thread_pool_module_threads(4)
    omp_fn.assert_called_once_with(4)
    openblas_fn.assert_called_once_with(4)
    mkl_fn.assert_not_called()

    # Post-task reset back to 1 still leaves explicit MKL_NUM_THREADS untouched.
    omp_fn.reset_mock()
    openblas_fn.reset_mock()
    set_thread_pool_module_threads(1)
    omp_fn.assert_called_once_with(1)
    openblas_fn.assert_called_once_with(1)
    mkl_fn.assert_not_called()

    # When set_num_threads_env_vars_if_unset defaults both OMP and OPENBLAS,
    # OpenBLAS is still resized even though it is written to os.environ first.
    omp_fn.reset_mock()
    openblas_fn.reset_mock()
    mkl_fn.reset_mock()
    monkeypatch.setenv(
        "RAY_NUM_THREADS_ENV_VARS", "OMP_NUM_THREADS,OPENBLAS_NUM_THREADS"
    )
    monkeypatch.delenv("OMP_NUM_THREADS", raising=False)
    monkeypatch.delenv("OPENBLAS_NUM_THREADS", raising=False)
    mock_ctx = MagicMock()
    mock_ctx.worker.mode = ray._private.worker.WORKER_MODE
    mock_ctx.get_assigned_resources.return_value = {"CPU": 6}
    with patch("ray.get_runtime_context", return_value=mock_ctx):
        assert set_num_threads_env_vars_if_unset() == [
            "OMP_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
        ]
    omp_fn.assert_called_once_with(6)
    openblas_fn.assert_called_once_with(6)
    mkl_fn.assert_not_called()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
