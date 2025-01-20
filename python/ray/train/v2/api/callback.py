from typing import Any, Dict, List, Optional

from ray.train import Checkpoint
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class RayTrainCallback:
    """Base Ray Train callback interface."""

    pass


@DeveloperAPI
class UserCallback(RayTrainCallback):
    """Callback interface for custom user-defined callbacks to handling events
    during training.

    This callback is called on the Ray Train controller process, not on the
    worker processes.
    """

    def after_report(
        self,
        run_context: TrainRunContext,
        metrics: List[Dict[str, Any]],
        checkpoint: Optional[Checkpoint],
    ):
        """Called after all workers have reported a metric + checkpoint
        via `ray.train.report`.

        Args:
            run_context: The `TrainRunContext` for the current training run.
            metrics: A list of metric dictionaries reported by workers,
                where metrics[i] is the metrics dict reported by worker i.
            checkpoint: A Checkpoint object that has been persisted to
                storage. This is None if no workers reported a checkpoint
                (e.g. `ray.train.report(metrics, checkpoint=None)`).
        """
        pass

    def after_exception(
        self, run_context: TrainRunContext, worker_exceptions: Dict[int, Exception]
    ):
        """Called after one or more workers have raised an exception.

        Args:
            run_context: The `TrainRunContext` for the current training run.
            worker_exceptions: A dict from worker world rank to the exception
                raised by that worker.
        """
        pass
