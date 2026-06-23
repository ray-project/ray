"""Native MLflow integration for Ray Train v2.

Provides ``MLflowLoggerCallback``, a ``UserCallback`` implementation that
automatically manages MLflow experiment tracking for training runs.

Key design choices:

- **Controller-only execution**: all MLflow operations run on the controller
  process, avoiding N workers creating N runs.
- **Pure ``MlflowClient``**: no global ``mlflow.start_run()`` calls that
  mutate process state.
- **Configurable checkpoint strategy**: controls artifact upload frequency
  to avoid network bottlenecks in large-scale training.
- **Graceful degradation**: MLflow failures do not block training unless
  ``raise_on_error=True``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional

from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.utils.mlflow_util import _MLflowTrackerUtil
from ray.train.v2.api.callback import UserCallback
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.train import Checkpoint
    from ray.train.v2.api.result import Result

logger = logging.getLogger(__name__)

CheckpointStrategy = Literal["all", "best", "last", "none"]


@PublicAPI(stability="alpha")
class MLflowLoggerCallback(UserCallback):
    """Native MLflow logging callback for Ray Train v2.

    Automatically manages MLflow runs, logs training parameters and metrics,
    and optionally uploads checkpoint artifacts.

    Usage::

        from ray.train import RunConfig
        from ray.train.v2.api.mlflow import MLflowLoggerCallback

        callback = MLflowLoggerCallback(
            experiment_name="my_experiment",
            tracking_uri="http://mlflow:5000",
        )
        trainer.fit(run_config=RunConfig(callbacks=[callback]))

    Args:
        experiment_name: MLflow experiment name.
        tracking_uri: MLflow tracking server URI. ``None`` uses default.
        run_name: Run name. Defaults to ``run_context.run_id``.
        tags: Run tags.
        save_checkpoints: Checkpoint upload strategy:

            - ``"all"``: upload every checkpoint.
            - ``"best"``: upload only when metric improves (requires
              ``checkpoint_metric``).
            - ``"last"``: upload final checkpoint in ``after_run``.
            - ``"none"``: skip artifact upload.

        checkpoint_metric: Metric name for ``"best"`` strategy.
        checkpoint_metric_mode: ``"min"`` or ``"max"``.
        log_params: Whether to log ``train_loop_config`` as parameters.
        raise_on_error: If ``True``, MLflow failures abort training.
    """

    def __init__(
        self,
        experiment_name: str,
        tracking_uri: Optional[str] = None,
        run_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        save_checkpoints: CheckpointStrategy = "none",
        checkpoint_metric: Optional[str] = None,
        checkpoint_metric_mode: Literal["min", "max"] = "min",
        log_params: bool = True,
        raise_on_error: bool = False,
    ):
        try:
            import mlflow  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "mlflow is required for MLflowLoggerCallback. "
                "Install with: pip install mlflow"
            ) from e

        self._experiment_name = experiment_name
        self._tracking_uri = tracking_uri
        self._run_name = run_name
        self._tags = tags
        self._save_checkpoints = save_checkpoints
        self._checkpoint_metric = checkpoint_metric
        self._checkpoint_metric_mode = checkpoint_metric_mode
        self._log_params = log_params
        self._raise_on_error = raise_on_error

        # State
        self._util: Optional[_MLflowTrackerUtil] = None
        self._experiment_id: Optional[str] = None
        self._run_id: Optional[str] = None
        self._best_metric_value: Optional[float] = None
        self._last_checkpoint: Optional["Checkpoint"] = None
        self._failed: bool = False

        # Validate config
        if save_checkpoints == "best" and checkpoint_metric is None:
            raise ValueError(
                "checkpoint_metric is required when save_checkpoints='best'"
            )

    # ------------------------------------------------------------------
    # UserCallback hooks
    # ------------------------------------------------------------------

    def before_run(self, run_context: TrainRunContext) -> None:
        """Create MLflow experiment and run, log parameters."""
        try:
            self._util = _MLflowTrackerUtil(
                tracking_uri=self._tracking_uri,
                raise_on_error=self._raise_on_error,
            )
            self._experiment_id = self._util.setup_experiment(self._experiment_name)
            if self._experiment_id is None:
                return

            self._run_id = self._util.start_run(
                experiment_id=self._experiment_id,
                run_name=self._run_name or run_context.run_id,
                tags=self._tags,
            )

            if self._log_params and self._run_id:
                train_loop_config = getattr(run_context, "train_loop_config", None)
                if train_loop_config:
                    self._util.log_params(self._run_id, train_loop_config)

            logger.info(
                "MLflow run started: experiment=%s, run=%s",
                self._experiment_name,
                self._run_id,
            )
        except Exception as e:
            logger.warning("MLflow before_run failed: %s", e)
            if self._raise_on_error:
                raise

    def after_report(
        self,
        run_context: TrainRunContext,
        metrics: List[Dict[str, Any]],
        checkpoint: Optional["Checkpoint"],
    ) -> None:
        """Log metrics and optionally upload checkpoint."""
        if self._util is None or self._run_id is None:
            return

        try:
            # Log metrics from rank 0
            if metrics:
                rank_0_metrics = metrics[0]
                step = rank_0_metrics.get("training_iteration")
                self._util.log_metrics_batch(self._run_id, rank_0_metrics, step=step)

            # Handle checkpoint upload
            if checkpoint and self._save_checkpoints != "none":
                self._handle_checkpoint(checkpoint, metrics)
        except Exception as e:
            logger.warning("MLflow after_report failed: %s", e)
            if self._raise_on_error:
                raise

    def after_exception(
        self,
        run_context: TrainRunContext,
        worker_exceptions: Dict[int, Exception],
    ) -> None:
        """Mark run as FAILED."""
        if self._util is None or self._run_id is None:
            return

        try:
            self._util.end_run(self._run_id, status="FAILED")
            self._failed = True
            logger.error("MLflow run %s marked FAILED", self._run_id)
        except Exception as e:
            logger.warning("MLflow after_exception failed: %s", e)
            if self._raise_on_error:
                raise

    def after_run(self, run_context: TrainRunContext, result: "Result") -> None:
        """Finalize MLflow run: upload last checkpoint if needed, end run."""
        if self._util is None or self._run_id is None:
            return

        try:
            # Upload last checkpoint if strategy is "last"
            if self._save_checkpoints == "last" and self._last_checkpoint is not None:
                self._upload_checkpoint(self._last_checkpoint, "last")

            # End run (don't override FAILED status from after_exception)
            if not self._failed:
                status = "FINISHED" if result.error is None else "FAILED"
                self._util.end_run(self._run_id, status=status)

            logger.info("MLflow run %s completed", self._run_id)
        except Exception as e:
            logger.warning("MLflow after_run failed: %s", e)
            if self._raise_on_error:
                raise

    # ------------------------------------------------------------------
    # Checkpoint handling
    # ------------------------------------------------------------------

    def _handle_checkpoint(
        self, checkpoint: "Checkpoint", metrics: List[Dict[str, Any]]
    ) -> None:
        """Handle checkpoint based on strategy."""
        if self._save_checkpoints == "all":
            step = metrics[0].get("training_iteration", 0) if metrics else 0
            self._upload_checkpoint(checkpoint, f"step_{step}")
        elif self._save_checkpoints == "best":
            if metrics and self._checkpoint_metric:
                metric_value = metrics[0].get(self._checkpoint_metric)
                if metric_value is not None and self._is_better(metric_value):
                    self._best_metric_value = metric_value
                    self._upload_checkpoint(checkpoint, "best")
        elif self._save_checkpoints == "last":
            self._last_checkpoint = checkpoint

    def _is_better(self, new_value: float) -> bool:
        """Check if new metric value is better than current best."""
        if self._best_metric_value is None:
            return True
        if self._checkpoint_metric_mode == "min":
            return new_value < self._best_metric_value
        return new_value > self._best_metric_value

    def _upload_checkpoint(self, checkpoint: "Checkpoint", label: str) -> None:
        """Upload checkpoint as MLflow artifact."""
        if self._util is None or self._run_id is None:
            return
        try:
            with checkpoint.as_directory() as checkpoint_dir:
                self._util.log_artifacts(
                    self._run_id,
                    checkpoint_dir,
                    artifact_path=f"checkpoints/{label}",
                )
        except Exception as e:
            logger.warning("Failed to upload checkpoint '%s': %s", label, e)
            if self._raise_on_error:
                raise
