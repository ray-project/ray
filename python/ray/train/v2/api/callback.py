from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray.train import Checkpoint
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.train.v2.api.result import Result


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

    def before_run(self, run_context: TrainRunContext) -> None:
        """Called before the training run starts.

        This is called after the controller is initialized and before the
        worker group is created. Use this to set up experiment tracking,
        log training parameters, or initialize state.

        Args:
            run_context: The ``TrainRunContext`` for the current training run.
                Contains ``run_config``, ``run_id``, ``scaling_config``, etc.
        """
        pass

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

    def after_run(self, run_context: TrainRunContext, result: "Result") -> None:
        """Called after the training run completes (success or failure).

        This is called after the control loop exits and the final ``Result``
        is built. Use this to log final metrics, close experiment tracking
        runs, or perform cleanup.

        Division of labor with ``after_exception``:

        - ``after_exception`` fires first (during the control loop) when
          workers raise exceptions. The callback should mark the tracking
          run as FAILED.
        - ``after_run`` fires after the loop exits. If ``result.error`` is
          ``None``, the callback should mark the tracking run as FINISHED.
          If the tracking run was already marked FAILED by
          ``after_exception``, the callback should not override it.

        Args:
            run_context: The ``TrainRunContext`` for the current training run.
            result: The final ``Result`` containing metrics, checkpoint, and
                error (if any).
        """
        pass
