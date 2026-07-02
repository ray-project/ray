from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray.train import Checkpoint
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import ray


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

        This fires during the control loop, *before* the controller decides
        whether to retry or raise.  The callback should mark any tracking
        state as FAILED but should **not** close the tracking run here —
        closing is deferred to ``after_run`` so that retry scenarios work
        correctly.

        Args:
            run_context: The ``TrainRunContext`` for the current training run.
            worker_exceptions: A dict from worker world rank to the exception
                raised by that worker.
        """
        pass

    def after_run(
        self, run_context: TrainRunContext, result: "ray.train.Result"
    ) -> None:
        """Called after the training run completes (success or failure).

        This is called after the control loop exits and the final ``Result``
        is built. Use this to log final metrics, close experiment tracking
        runs, or perform cleanup.

        If the tracking state was already marked FAILED by
        ``after_exception``, the callback should close the run as FAILED
        rather than overriding to FINISHED.

        Args:
            run_context: The ``TrainRunContext`` for the current training run.
            result: The final :class:`ray.train.Result` containing metrics,
                checkpoint, and error (if any).
        """
        pass
