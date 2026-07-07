import logging
import os
import time
from typing import Any, Dict, List, Optional
from mlflow.entities import Metric
from mlflow.tracking import MlflowClient
from ray.train import Checkpoint
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2.api.callback import UserCallback
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

@DeveloperAPI
class MLflowLoggerCallback(UserCallback):
    def __init__(self, experiment_name: str = None, run_id: str = None, 
                 tracking_uri: str = None, save_checkpoints_as_artifacts: bool = True):
        self.client = MlflowClient(tracking_uri=tracking_uri)
        self._experiment_name = experiment_name
        self._run_id = run_id
        self._save_checkpoints = save_checkpoints_as_artifacts
        self._step_counter = 0

    def after_report(self, run_context: TrainRunContext, metrics: List[Dict[str, Any]], checkpoint: Optional[Checkpoint] = None):
        rank_0_metrics = {}
        if isinstance(metrics, list) and len(metrics) > 0:
            rank_0_metrics = metrics[0].get("metrics", metrics[0])
        elif isinstance(metrics, dict):
            rank_0_metrics = metrics.get("rank_0", metrics)

        self._step_counter += 1
        step = rank_0_metrics.get("training_iteration", self._step_counter)
        
        metrics_to_log = []
        timestamp = int(time.time() * 1000)
        
        for k, v in rank_0_metrics.items():
            if isinstance(v, (int, float)):
                metrics_to_log.append(Metric(key=k, value=float(v), timestamp=timestamp, step=int(step)))

        if metrics_to_log or (checkpoint and self._save_checkpoints):
            if not self._run_id and self._experiment_name:
                exp = self.client.get_experiment_by_name(self._experiment_name)
                exp_id = exp.experiment_id if exp else self.client.create_experiment(self._experiment_name)
                self._run_id = self.client.create_run(exp_id).info.run_id

        if metrics_to_log and self._run_id:
            self.client.log_batch(self._run_id, metrics=metrics_to_log)

        if checkpoint and self._save_checkpoints and self._run_id:
            try:
                with checkpoint.as_directory() as checkpoint_dir:
                    self.client.log_artifacts(self._run_id, checkpoint_dir, artifact_path=f"checkpoint_step_{step}")
            except Exception as e:
                logger.warning(f"Failed to log checkpoint artifact to MLflow: {e}")

    def after_exception(self, run_context: TrainRunContext, worker_exceptions: Dict[int, Exception]):
        if getattr(self, "_run_id", None):
            try:
                self.client.set_terminated(self._run_id, status="FAILED")
            except Exception as e:
                logger.warning(f"Failed to terminate MLflow run as FAILED: {e}")
            self._run_id = None

    def close(self):
        if getattr(self, "_run_id", None):
            try:
                self.client.set_terminated(self._run_id, status="FINISHED")
            except Exception as e:
                logger.warning(f"Failed to terminate MLflow run: {e}")
            self._run_id = None

    def __del__(self):
        self.close()
