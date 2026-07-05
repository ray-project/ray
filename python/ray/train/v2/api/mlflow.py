import logging
import time
from typing import Any, Dict, List, Optional
from mlflow.entities import Metric
from mlflow.tracking import MlflowClient
from ray.train import Checkpoint
from ray.train.v2.api.callback import UserCallback
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

@DeveloperAPI
class MLflowLoggerCallback(UserCallback):
    def __init__(self, experiment_name: str = None, run_id: str = None, tracking_uri: str = None):
        self.client = MlflowClient(tracking_uri=tracking_uri)
        self._experiment_name = experiment_name
        self._run_id = run_id

    def after_report(self, metrics: Dict[str, Any], checkpoint: Optional[Checkpoint] = None):
        # Ray Train v2 structure: metrics usually contain 'rank_0' or training progress
        rank_0_metrics = metrics.get("rank_0", metrics)
        step = metrics.get("step", 0)
        
        metrics_to_log = []
        timestamp = int(time.time() * 1000)
        
        for k, v in rank_0_metrics.items():
            if isinstance(v, (int, float, int)): # Allow standard numeric types
                metrics_to_log.append(Metric(key=k, value=float(v), timestamp=timestamp, step=step))

        if metrics_to_log:
            # Lazy init run_id if not provided
            if not self._run_id:
                exp = self.client.get_experiment_by_name(self._experiment_name)
                exp_id = exp.experiment_id if exp else self.client.create_experiment(self._experiment_name)
                self._run_id = self.client.create_run(exp_id).info.run_id
                
            self.client.log_batch(self._run_id, metrics=metrics_to_log)

    def close(self):
        if getattr(self, "_run_id", None):
            try:
                self.client.set_terminated(self._run_id, status="FINISHED")
            except Exception as e:
                logger.warning(f"Failed to terminate MLflow run: {e}")
            self._run_id = None

    def __del__(self):
        self.close()
