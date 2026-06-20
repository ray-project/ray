import logging
from typing import Any, Dict, List, Optional

from mlflow.tracking import MlflowClient
from ray.train import Checkpoint
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2.api.callback import UserCallback
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@DeveloperAPI
class MLflowLoggerCallback(UserCallback):
    """Native MLflow logging callback for Ray Train v2 using explicit tracking clients."""

    def __init__(
        self,
        experiment_name: Optional[str] = None,
        run_name: Optional[str] = None,
        tracking_uri: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None,
        save_checkpoints_as_artifacts: bool = True,
    ):
        self.experiment_name = experiment_name or "Default"
        self.run_name = run_name
        self.tracking_uri = tracking_uri
        self.tags = tags or {}
        self.save_checkpoints_as_artifacts = save_checkpoints_as_artifacts

        self.client = MlflowClient(tracking_uri=self.tracking_uri)
        self._experiment_id = None
        self._run_id = None

    def _setup_mlflow(self, run_context: TrainRunContext):
        if self._run_id:
            return

        exp = self.client.get_experiment_by_name(self.experiment_name)
        if exp is None:
            self._experiment_id = self.client.create_experiment(self.experiment_name)
        else:
            self._experiment_id = exp.experiment_id

        run = self.client.create_run(
            experiment_id=self._experiment_id,
            run_name=self.run_name or run_context.run_id,
            tags=self.tags,
        )
        self._run_id = run.info.run_id
        logger.info(f"Started native MLflow run: {self._run_id}")

    def after_report(
        self,
        run_context: TrainRunContext,
        metrics: List[Dict[str, Any]],
        checkpoint: Optional[Checkpoint],
    ):
        self._setup_mlflow(run_context)

        if not metrics:
            return

        rank_0_metrics = metrics[0]
        step = rank_0_metrics.get("training_iteration", 0)

        for k, v in rank_0_metrics.items():
            if isinstance(v, (int, float)):
                self.client.log_metric(self._run_id, k, v, step=step)

        if self.save_checkpoints_as_artifacts and checkpoint:
            try:
                with checkpoint.as_directory() as checkpoint_dir:
                    self.client.log_artifact(
                        self._run_id,
                        checkpoint_dir,
                        artifact_path=f"checkpoints/step_{step}",
                    )
            except Exception as e:
                logger.warning(f"Failed to log checkpoint directory to MLflow: {e}")

    def after_exception(
        self, run_context: TrainRunContext, worker_exceptions: Dict[int, Exception]
    ):
        if self._run_id:
            self.client.set_terminated(self._run_id, status="FAILED")
            self._run_id = None

    def __del__(self):
        """Ensure the run status is closed out as FINISHED when the training workflow concludes."""
        if hasattr(self, "_run_id") and self._run_id:
            try:
                self.client.set_terminated(self._run_id, status="FINISHED")
            except Exception:
                pass
