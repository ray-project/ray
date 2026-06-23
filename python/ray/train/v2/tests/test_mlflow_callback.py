"""Tests for MLflowLoggerCallback and _MLflowTrackerUtil.

Unit tests use mocks (no real MLflow server). Integration tests
use a local SQLite-backed MLflow tracking URI.
"""

import importlib.util
import os
import sys
from typing import Dict, Optional
from unittest.mock import MagicMock, patch

import pytest

from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.utils.mlflow_util import _MLflowTrackerUtil
from ray.train.v2.api.result import Result

try:
    from ray.train.v2.api.mlflow import MLflowLoggerCallback
except ImportError:
    MLflowLoggerCallback = None  # type: ignore[assignment,misc]

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_run_context(config: Optional[Dict] = None) -> TrainRunContext:
    ctx = MagicMock(spec=TrainRunContext)
    ctx.run_id = "test-run-id"
    ctx.run_config = MagicMock()
    ctx.run_config.name = "test-run"
    ctx.scaling_config = MagicMock()
    ctx.train_loop_config = config or {"lr": 0.01, "epochs": 10}
    return ctx


def _make_result(error: bool = False) -> Result:
    result = MagicMock(spec=Result)
    result.error = RuntimeError("boom") if error else None
    result.metrics = {"loss": 0.1}
    result.checkpoint = MagicMock()
    return result


def _make_checkpoint() -> MagicMock:
    cp = MagicMock()
    ctx_mgr = MagicMock()
    ctx_mgr.__enter__ = MagicMock(return_value="/tmp/ckpt")
    ctx_mgr.__exit__ = MagicMock(return_value=False)
    cp.as_directory = MagicMock(return_value=ctx_mgr)
    return cp


# ===========================================================================
# _MLflowTrackerUtil tests
# ===========================================================================


class TestMLflowTrackerUtilFlatten:
    """Test _flatten_dict."""

    def test_flat_dict(self):
        assert _MLflowTrackerUtil._flatten_dict({"a": 1, "b": 2}) == {"a": 1, "b": 2}

    def test_nested_dict(self):
        result = _MLflowTrackerUtil._flatten_dict(
            {"optimizer": {"lr": 0.01, "momentum": 0.9}}
        )
        assert result == {"optimizer.lr": 0.01, "optimizer.momentum": 0.9}

    def test_deeply_nested(self):
        result = _MLflowTrackerUtil._flatten_dict({"a": {"b": {"c": 1}}})
        assert result == {"a.b.c": 1}

    def test_empty_dict(self):
        assert _MLflowTrackerUtil._flatten_dict({}) == {}


class TestMLflowTrackerUtilSafeValue:
    """Test _to_safe_param_value."""

    def test_string(self):
        assert _MLflowTrackerUtil._to_safe_param_value("hello") == "hello"

    def test_int(self):
        assert _MLflowTrackerUtil._to_safe_param_value(42) == "42"

    def test_float(self):
        assert _MLflowTrackerUtil._to_safe_param_value(3.14) == "3.14"

    def test_bool(self):
        assert _MLflowTrackerUtil._to_safe_param_value(True) == "True"

    def test_dict_to_json(self):
        result = _MLflowTrackerUtil._to_safe_param_value({"a": 1})
        assert '"a": 1' in result or '"a":1' in result


_mlflow_installed = importlib.util.find_spec("mlflow") is not None
_skip_no_mlflow = pytest.mark.skipif(
    not _mlflow_installed, reason="mlflow not installed"
)


@_skip_no_mlflow
class TestMLflowTrackerUtilInit:
    """Test initialization."""

    @patch("mlflow.tracking.MlflowClient")
    def test_init_default(self, MockClient):
        _MLflowTrackerUtil()
        MockClient.assert_called_once_with(tracking_uri=None)

    @patch("mlflow.tracking.MlflowClient")
    def test_init_with_tracking_uri(self, MockClient):
        _MLflowTrackerUtil(tracking_uri="http://mlflow:5000")
        MockClient.assert_called_once_with(tracking_uri="http://mlflow:5000")


@_skip_no_mlflow
class TestMLflowTrackerUtilSafeCall:
    """Test _safe_call graceful degradation."""

    def test_success(self):
        util = _MLflowTrackerUtil()
        util._client.some_method = MagicMock(return_value="ok")
        assert util._safe_call(util._client.some_method) == "ok"

    def test_failure_silent(self):
        util = _MLflowTrackerUtil(raise_on_error=False)
        util._client.some_method = MagicMock(side_effect=ConnectionError("refused"))
        assert util._safe_call(util._client.some_method) is None

    def test_failure_raises(self):
        util = _MLflowTrackerUtil(raise_on_error=True)
        util._client.some_method = MagicMock(side_effect=ConnectionError("refused"))
        with pytest.raises(ConnectionError):
            util._safe_call(util._client.some_method)


@_skip_no_mlflow
class TestMLflowTrackerUtilLogMetricsBatch:
    """Test batch metric logging with filtering."""

    @patch("mlflow.tracking.MlflowClient")
    def test_filters_nan(self, MockClient):
        mock_client = MagicMock()
        MockClient.return_value = mock_client
        util = _MLflowTrackerUtil()
        util.log_metrics_batch("run-1", {"loss": float("nan"), "acc": 0.9})
        metrics = mock_client.log_batch.call_args.kwargs["metrics"]
        assert len(metrics) == 1
        assert metrics[0].key == "acc"

    @patch("mlflow.tracking.MlflowClient")
    def test_filters_inf(self, MockClient):
        mock_client = MagicMock()
        MockClient.return_value = mock_client
        util = _MLflowTrackerUtil()
        util.log_metrics_batch("run-1", {"a": float("inf"), "b": 1.0})
        metrics = mock_client.log_batch.call_args.kwargs["metrics"]
        assert len(metrics) == 1

    @patch("mlflow.tracking.MlflowClient")
    def test_filters_non_numeric(self, MockClient):
        mock_client = MagicMock()
        MockClient.return_value = mock_client
        util = _MLflowTrackerUtil()
        util.log_metrics_batch("run-1", {"a": "string", "b": 1.0})
        metrics = mock_client.log_batch.call_args.kwargs["metrics"]
        assert len(metrics) == 1


# ===========================================================================
# MLflowLoggerCallback tests
# ===========================================================================


@_skip_no_mlflow
class TestMLflowLoggerCallbackInit:
    """Test callback initialization."""

    def test_default_params(self):
        cb = MLflowLoggerCallback(experiment_name="exp1")
        assert cb._experiment_name == "exp1"
        assert cb._tracking_uri is None
        assert cb._save_checkpoints == "none"
        assert cb._run_id is None

    def test_best_strategy_requires_metric(self):
        with pytest.raises(ValueError, match="checkpoint_metric"):
            MLflowLoggerCallback(
                experiment_name="exp1",
                save_checkpoints="best",
                checkpoint_metric=None,
            )

    def test_all_strategy_no_metric_required(self):
        cb = MLflowLoggerCallback(experiment_name="exp1", save_checkpoints="all")
        assert cb._save_checkpoints == "all"


@_skip_no_mlflow
class TestMLflowLoggerCallbackLifecycle:
    """Test full lifecycle: before_run -> after_report -> after_run."""

    @patch("ray.train.v2.api.mlflow._MLflowTrackerUtil")
    def test_before_run_creates_experiment_and_run(self, MockUtil):
        mock_util = MagicMock()
        mock_util.setup_experiment.return_value = "exp-123"
        mock_util.start_run.return_value = "run-456"
        MockUtil.return_value = mock_util

        cb = MLflowLoggerCallback(experiment_name="exp1")
        ctx = _make_run_context()
        cb.before_run(ctx)

        mock_util.setup_experiment.assert_called_once_with("exp1")
        mock_util.start_run.assert_called_once()
        mock_util.log_params.assert_called_once_with("run-456", ctx.train_loop_config)
        assert cb._run_id == "run-456"

    @patch("ray.train.v2.api.mlflow._MLflowTrackerUtil")
    def test_after_report_logs_metrics(self, MockUtil):
        mock_util = MagicMock()
        MockUtil.return_value = mock_util

        cb = MLflowLoggerCallback(experiment_name="exp1", save_checkpoints="none")
        cb._util = mock_util
        cb._run_id = "run-456"

        cb.after_report(
            _make_run_context(), [{"loss": 0.5, "training_iteration": 1}], None
        )
        mock_util.log_metrics_batch.assert_called_once()

    @patch("ray.train.v2.api.mlflow._MLflowTrackerUtil")
    def test_after_run_ends_run_finished(self, MockUtil):
        mock_util = MagicMock()
        MockUtil.return_value = mock_util

        cb = MLflowLoggerCallback(experiment_name="exp1", save_checkpoints="none")
        cb._util = mock_util
        cb._run_id = "run-456"

        cb.after_run(_make_run_context(), _make_result(error=False))
        mock_util.end_run.assert_called_once_with("run-456", status="FINISHED")

    @patch("ray.train.v2.api.mlflow._MLflowTrackerUtil")
    def test_after_run_ends_run_failed_on_error(self, MockUtil):
        mock_util = MagicMock()
        MockUtil.return_value = mock_util

        cb = MLflowLoggerCallback(experiment_name="exp1", save_checkpoints="none")
        cb._util = mock_util
        cb._run_id = "run-456"

        cb.after_run(_make_run_context(), _make_result(error=True))
        mock_util.end_run.assert_called_once_with("run-456", status="FAILED")

    @patch("ray.train.v2.api.mlflow._MLflowTrackerUtil")
    def test_after_exception_sets_failed_flag(self, MockUtil):
        mock_util = MagicMock()
        MockUtil.return_value = mock_util

        cb = MLflowLoggerCallback(experiment_name="exp1", save_checkpoints="none")
        cb._util = mock_util
        cb._run_id = "run-456"

        cb.after_exception(_make_run_context(), {0: RuntimeError("boom")})
        # after_exception only sets the flag; closing is deferred to after_run
        mock_util.end_run.assert_not_called()
        assert cb._failed is True

    @patch("ray.train.v2.api.mlflow._MLflowTrackerUtil")
    def test_after_run_closes_failed_run(self, MockUtil):
        mock_util = MagicMock()
        MockUtil.return_value = mock_util

        cb = MLflowLoggerCallback(experiment_name="exp1", save_checkpoints="none")
        cb._util = mock_util
        cb._run_id = "run-456"
        cb._failed = True  # already marked by after_exception

        cb.after_run(_make_run_context(), _make_result(error=True))
        mock_util.end_run.assert_called_once_with("run-456", status="FAILED")


@_skip_no_mlflow
class TestMLflowLoggerCallbackCheckpointStrategy:
    """Test checkpoint upload strategies."""

    @patch("ray.train.v2.api.mlflow._MLflowTrackerUtil")
    def test_strategy_none(self, MockUtil):
        mock_util = MagicMock()
        MockUtil.return_value = mock_util

        cb = MLflowLoggerCallback(experiment_name="exp1", save_checkpoints="none")
        cb._util = mock_util
        cb._run_id = "run-1"
        cp = _make_checkpoint()

        cb.after_report(_make_run_context(), [{"loss": 0.5}], cp)
        mock_util.log_artifacts.assert_not_called()

    @patch("ray.train.v2.api.mlflow._MLflowTrackerUtil")
    def test_strategy_all(self, MockUtil):
        mock_util = MagicMock()
        MockUtil.return_value = mock_util

        cb = MLflowLoggerCallback(experiment_name="exp1", save_checkpoints="all")
        cb._util = mock_util
        cb._run_id = "run-1"
        cp = _make_checkpoint()

        cb.after_report(
            _make_run_context(), [{"loss": 0.5, "training_iteration": 3}], cp
        )
        mock_util.log_artifacts.assert_called_once()

    @patch("ray.train.v2.api.mlflow._MLflowTrackerUtil")
    def test_strategy_best_improving(self, MockUtil):
        mock_util = MagicMock()
        MockUtil.return_value = mock_util

        cb = MLflowLoggerCallback(
            experiment_name="exp1",
            save_checkpoints="best",
            checkpoint_metric="loss",
            checkpoint_metric_mode="min",
        )
        cb._util = mock_util
        cb._run_id = "run-1"
        cp = _make_checkpoint()

        # First report — always uploads
        cb.after_report(_make_run_context(), [{"loss": 0.5}], cp)
        assert mock_util.log_artifacts.call_count == 1

        # Second report — worse metric, no upload
        cb.after_report(_make_run_context(), [{"loss": 0.6}], cp)
        assert mock_util.log_artifacts.call_count == 1

        # Third report — better metric, uploads
        cb.after_report(_make_run_context(), [{"loss": 0.3}], cp)
        assert mock_util.log_artifacts.call_count == 2

    @patch("ray.train.v2.api.mlflow._MLflowTrackerUtil")
    def test_strategy_last_defers_upload(self, MockUtil):
        mock_util = MagicMock()
        MockUtil.return_value = mock_util

        cb = MLflowLoggerCallback(experiment_name="exp1", save_checkpoints="last")
        cb._util = mock_util
        cb._run_id = "run-1"
        cp = _make_checkpoint()

        cb.after_report(_make_run_context(), [{"loss": 0.5}], cp)
        mock_util.log_artifacts.assert_not_called()
        assert cb._last_checkpoint is cp

        cb.after_run(_make_run_context(), _make_result())
        mock_util.log_artifacts.assert_called_once()


@_skip_no_mlflow
class TestMLflowLoggerCallbackGracefulDegradation:
    """Test that MLflow failures don't block training."""

    @patch("ray.train.v2.api.mlflow._MLflowTrackerUtil")
    def test_before_run_failure_silent(self, MockUtil):
        MockUtil.side_effect = ConnectionError("refused")
        cb = MLflowLoggerCallback(experiment_name="exp1", raise_on_error=False)
        cb.before_run(_make_run_context())
        assert cb._util is None

    @patch("ray.train.v2.api.mlflow._MLflowTrackerUtil")
    def test_before_run_failure_raises(self, MockUtil):
        MockUtil.side_effect = ConnectionError("refused")
        cb = MLflowLoggerCallback(experiment_name="exp1", raise_on_error=True)
        with pytest.raises(ConnectionError):
            cb.before_run(_make_run_context())

    @patch("ray.train.v2.api.mlflow._MLflowTrackerUtil")
    def test_after_report_noop_when_not_initialized(self, MockUtil):
        cb = MLflowLoggerCallback(experiment_name="exp1")
        # Should not raise
        cb.after_report(_make_run_context(), [{"loss": 0.5}], None)


# ===========================================================================
# mlflow not installed test
# ===========================================================================


class TestMLflowNotInstalled:
    """Test behavior when mlflow is not installed."""

    def test_before_run_raises_import_error(self):
        """before_run should raise ImportError when mlflow is not installed."""
        import sys

        # Save and remove all mlflow-related modules to simulate mlflow
        # not being installed. Setting only sys.modules["mlflow"] = None
        # is insufficient because submodules (e.g. mlflow.tracking) may
        # already be cached.
        originals = {
            k: v
            for k, v in sys.modules.items()
            if k == "mlflow" or k.startswith("mlflow.")
        }
        try:
            for k in originals:
                sys.modules[k] = None  # type: ignore[assignment]
            cb = MLflowLoggerCallback(experiment_name="exp1", raise_on_error=True)
            with pytest.raises(ImportError, match="mlflow is required"):
                cb.before_run(_make_run_context())
        finally:
            sys.modules.update(originals)


# ===========================================================================
# Integration tests (require running MLflow server at localhost:8050)
# ===========================================================================

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:8050")


def _mlflow_available() -> bool:
    """Check if MLflow server is reachable."""
    import urllib.request

    try:
        urllib.request.urlopen(f"{MLFLOW_TRACKING_URI}/health", timeout=3)
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _mlflow_available(), reason="MLflow server not available")
class TestMLflowIntegration:
    """Integration tests using a real MLflow server."""

    def _cleanup_experiment(self, experiment_name: str):
        """Delete experiment if it exists."""
        from mlflow.tracking import MlflowClient

        client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
        exp = client.get_experiment_by_name(experiment_name)
        if exp is not None:
            client.delete_experiment(exp.experiment_id)

    def test_full_lifecycle_success(self):
        """Full lifecycle: before_run -> after_report -> after_run (success)."""
        import uuid

        from mlflow.tracking import MlflowClient

        exp_name = f"integration-test-{uuid.uuid4().hex[:8]}"
        try:
            cb = MLflowLoggerCallback(
                experiment_name=exp_name,
                tracking_uri=MLFLOW_TRACKING_URI,
                save_checkpoints="none",
            )
            ctx = _make_run_context({"lr": 0.01, "batch_size": 32})

            # before_run: create experiment + run
            cb.before_run(ctx)
            assert cb._run_id is not None

            # after_report: log metrics
            cb.after_report(ctx, [{"loss": 0.5, "training_iteration": 1}], None)
            cb.after_report(ctx, [{"loss": 0.3, "training_iteration": 2}], None)

            # after_run: finalize
            cb.after_run(ctx, _make_result(error=False))

            # Verify in MLflow
            client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
            exp = client.get_experiment_by_name(exp_name)
            assert exp is not None

            runs = client.search_runs([exp.experiment_id])
            assert len(runs) == 1
            assert runs[0].info.status == "FINISHED"

            # Verify metrics
            metrics = runs[0].data.metrics
            assert "loss" in metrics
            assert metrics["loss"] == 0.3  # last reported value

            # Verify params
            params = runs[0].data.params
            assert "lr" in params
            assert params["lr"] == "0.01"
        finally:
            self._cleanup_experiment(exp_name)

    def test_full_lifecycle_failure(self):
        """Full lifecycle: before_run -> after_exception -> after_run (failure)."""
        import uuid

        from mlflow.tracking import MlflowClient

        exp_name = f"integration-test-fail-{uuid.uuid4().hex[:8]}"
        try:
            cb = MLflowLoggerCallback(
                experiment_name=exp_name,
                tracking_uri=MLFLOW_TRACKING_URI,
                save_checkpoints="none",
            )
            ctx = _make_run_context()

            cb.before_run(ctx)
            assert cb._run_id is not None

            # Worker exception
            cb.after_exception(ctx, {0: RuntimeError("boom")})

            # after_run should not override FAILED
            cb.after_run(ctx, _make_result(error=True))

            client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
            exp = client.get_experiment_by_name(exp_name)
            runs = client.search_runs([exp.experiment_id])
            assert runs[0].info.status == "FAILED"
        finally:
            self._cleanup_experiment(exp_name)

    def test_nested_params_flattened(self):
        """Nested dict params are flattened correctly."""
        import uuid

        from mlflow.tracking import MlflowClient

        exp_name = f"integration-test-params-{uuid.uuid4().hex[:8]}"
        try:
            cb = MLflowLoggerCallback(
                experiment_name=exp_name,
                tracking_uri=MLFLOW_TRACKING_URI,
                save_checkpoints="none",
            )
            ctx = _make_run_context(
                {"optimizer": {"lr": 0.01, "momentum": 0.9}, "epochs": 10}
            )

            cb.before_run(ctx)
            cb.after_run(ctx, _make_result())

            client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
            exp = client.get_experiment_by_name(exp_name)
            runs = client.search_runs([exp.experiment_id])

            params = runs[0].data.params
            assert "optimizer.lr" in params
            assert params["optimizer.lr"] == "0.01"
            assert "optimizer.momentum" in params
            assert "epochs" in params
        finally:
            self._cleanup_experiment(exp_name)

    def test_batch_metrics_logged(self):
        """Multiple metrics are logged in a single batch."""
        import uuid

        from mlflow.tracking import MlflowClient

        exp_name = f"integration-test-batch-{uuid.uuid4().hex[:8]}"
        try:
            cb = MLflowLoggerCallback(
                experiment_name=exp_name,
                tracking_uri=MLFLOW_TRACKING_URI,
                save_checkpoints="none",
            )
            ctx = _make_run_context()

            cb.before_run(ctx)
            cb.after_report(
                ctx,
                [{"loss": 0.5, "acc": 0.8, "f1": 0.75, "training_iteration": 1}],
                None,
            )
            cb.after_run(ctx, _make_result())

            client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
            exp = client.get_experiment_by_name(exp_name)
            runs = client.search_runs([exp.experiment_id])

            metrics = runs[0].data.metrics
            assert metrics["loss"] == 0.5
            assert metrics["acc"] == 0.8
            assert metrics["f1"] == 0.75
        finally:
            self._cleanup_experiment(exp_name)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
