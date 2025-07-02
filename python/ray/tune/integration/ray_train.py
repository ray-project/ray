from typing import Any, Dict, List, Optional

import ray.tune
from ray.train import Checkpoint as RayTrainCheckpoint
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2.api.callback import UserCallback
from ray.util.annotations import DeveloperAPI

CHECKPOINT_PATH_KEY = "checkpoint_path"


@DeveloperAPI
class TuneReportCallback(UserCallback):
    """Propagate metrics and checkpoint paths from Ray Train workers to Ray Tune."""

    def after_report(
        self,
        run_context: TrainRunContext,
        metrics: List[Dict[str, Any]],
        checkpoint: Optional[RayTrainCheckpoint],
    ):
        # TODO: This can be changed to aggregate the metrics from all workers.
        # For now, just achieve feature parity with the old Tune+Train integration.
        metrics = metrics[0].copy()

        # If a checkpoint is provided, add the checkpoint path to the metrics.
        # Don't report the checkpoint again since it's already been uploaded
        # to storage.
        if checkpoint:
            metrics[CHECKPOINT_PATH_KEY] = checkpoint.path

        ray.tune.report(metrics=metrics)
