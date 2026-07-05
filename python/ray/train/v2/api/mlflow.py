import logging
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
    def __init__(self, run_id: str, tracking_uri: str = None):
        self._run_id = run_id
        self.client = MlflowClient(tracking_uri=tracking_uri)

    def after_report(self, context: TrainRunContext, metrics: Dict[str, Any], step: int):
        rank_0_metrics = metrics.get("rank_0", {})
        metrics_to_log = []
        timestamp = int(time.time() * 1000)
        
        for k, v in rank_0_metrics.items():
            if isinstance(v, (int, float)):
                metrics_to_log.append(Metric(key=k, value=float(v), timestamp=timestamp, step=step))

        if metrics_to_log:
            self.client.log_batch(self._run_id, metrics=metrics_to_log)

    def close(self):
        """Explicitly terminate the active MLflow run."""
        if getattr(self, "_run_id", None):
            try:
                self.client.set_terminated(self._run_id, status="FINISHED")
            except Exception as e:
                logger.warning(f"Failed to terminate MLflow run: {e}")
            self._run_id = None

    def __del__(self):
        """Ensure the run status is closed out."""
        if getattr(self, "_run_id", None) and getattr(self, "client", None):
            try:
                self.close()
            except Exception:
                pass
