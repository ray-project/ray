import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

from ray.air.checkpoint import Checkpoint
from ray.air.config import CheckpointConfig
from ray.air._internal.util import StartTraceback
from ray.train._internal.backend_executor import (
    BackendExecutor,
    InactiveWorkerGroupError,
    TrainBackendError,
    TrainingWorkerError,
)
from ray.train._internal.checkpoint import (
    CheckpointManager,
)
from ray.train._internal.dataset_spec import RayDatasetSpec
from ray.train._internal.session import TrainingResultType

# Ray Train should be usable even if Tune is not installed.
from ray.train._internal.utils import ActorWrapper
from ray.train.backend import BackendConfig
from ray.train.base_trainer import (  # noqa: F401
    BaseTrainer,
    GenDataset,
    TrainingFailedError,
)
from ray.util.annotations import DeveloperAPI

T = TypeVar("T")
S = TypeVar("S")

logger = logging.getLogger(__name__)


@DeveloperAPI
class TrainingIterator:
    """An iterator over Train results. Returned by ``trainer.run_iterator``."""

    def __init__(
        self,
        backend_executor: Union[BackendExecutor, ActorWrapper],
        backend_config: BackendConfig,
        train_func: Union[Callable[[], T], Callable[[Dict[str, Any]], T]],
        dataset_spec: RayDatasetSpec,
        checkpoint_manager: CheckpointManager,
        checkpoint: Optional[Union[Dict, str, Path, Checkpoint]],
        checkpoint_strategy: Optional[CheckpointConfig],
        run_dir: Optional[Path] = None,
    ):
        self._backend_executor = backend_executor
        self._backend = backend_config.backend_cls()
        self._train_func = train_func
        self._dataset_spec = dataset_spec
        self._run_dir = run_dir
        self._checkpoint_manager = checkpoint_manager
        self._checkpoint_strategy = checkpoint_strategy
        self._start_training(
            train_func=train_func,
            run_dir=run_dir,
            dataset_spec=self._dataset_spec,
            checkpoint=checkpoint,
            checkpoint_strategy=checkpoint_strategy,
        )

        self._final_results = None
        self._finished_training = False

    def __iter__(self):
        return self

    def _start_training(
        self,
        train_func,
        run_dir,
        dataset_spec,
        checkpoint,
        checkpoint_strategy,
        latest_checkpoint_id=None,
    ):
        self._checkpoint_manager.on_start_training(
            checkpoint_strategy=checkpoint_strategy,
            run_dir=run_dir,
            latest_checkpoint_id=latest_checkpoint_id,
        )
        checkpoint = self._checkpoint_manager._load_checkpoint(checkpoint)
        self._run_with_error_handling(
            lambda: self._backend_executor.start_training(
                train_func=train_func,
                dataset_spec=dataset_spec,
                checkpoint=checkpoint,
            )
        )

    def _run_with_error_handling(self, func: Callable):
        try:
            return func()
        except TrainingWorkerError:
            # Workers have already been restarted.
            logger.info(
                "Workers have been successfully restarted. Resuming "
                "training from latest checkpoint."
            )
            logger.debug(
                f"Latest checkpoint: {self._checkpoint_manager.latest_checkpoint}"
            )
            self._start_training(
                self._train_func,
                self._run_dir,
                self._dataset_spec,
                self._checkpoint_manager.latest_checkpoint,
                self._checkpoint_strategy,
                latest_checkpoint_id=self._checkpoint_manager.latest_checkpoint_id,
            )
            return self._run_with_error_handling(func)
        except InactiveWorkerGroupError:
            raise RuntimeError(
                "This Trainer is not active. It is either shutdown "
                "already or never started in the first place. "
                "Either create a new Trainer or start this one."
            ) from None
        except TrainBackendError:
            raise RuntimeError(
                "Training failed. You should not be seeing "
                "this error and this is a bug. Please create "
                "a new issue at "
                "https://github.com/ray-project/ray."
            ) from None

    def __next__(self):
        if self.is_finished():
            raise StopIteration
        try:
            next_results = self._run_with_error_handling(self._fetch_next_result)
            if next_results is None:
                self._final_results = self._run_with_error_handling(
                    self._finish_training
                )
                self._finished_training = True
                raise StopIteration
            else:
                return next_results
        except StartTraceback:
            # If this is a StartTraceback, then this is a user error.
            # We raise it directly
            try:
                # Exception raised in at least one training worker. Immediately raise
                # this error to the user and do not attempt to terminate gracefully.
                self._backend_executor.shutdown(graceful_termination=False)
                self._finished_training = True
            except Exception:
                pass
            raise

    def _fetch_next_result(self) -> Optional[List[Dict]]:
        """Fetch next results produced by ``session.report()`` from each worker.

        Assumes ``start_training`` has already been called.

        Returns:
            A list of dictionaries of values passed to ``session.report()`` from
                each worker. Each item corresponds to an intermediate result
                a single worker. If there are no more items to fetch,
                returns None.
        """

        while True:
            results = self._backend_executor.get_next_results()
            if results is None:
                return None
            first_result = results[0]
            result_type = first_result.type
            if result_type is TrainingResultType.REPORT:
                result_data = [r.data for r in results]
                return result_data
            elif result_type is TrainingResultType.CHECKPOINT:
                self._checkpoint_manager._process_checkpoint(
                    results, decode_checkpoint_fn=self._backend._decode_data
                )
                # Iterate until next REPORT call or training has finished.
            else:
                raise TrainBackendError(
                    f"Unexpected result type: "
                    f"{result_type}. "
                    f"Expected one of "
                    f"{[type in TrainingResultType]}"
                )

    def _finish_checkpointing(self):
        while True:
            results = self._backend_executor.get_next_results()
            if results is None:
                break
            result_type = results[0].type
            # Process checkpoints and ignore other result types.
            if result_type is TrainingResultType.CHECKPOINT:
                self._checkpoint_manager._process_checkpoint(
                    results, decode_checkpoint_fn=self._backend._decode_data
                )

    def _finish_training(self):
        """Finish training and return final results. Propagate any exceptions.

        Blocks until training is finished on all workers.

        Assumes `start_training` has already been called.

        Returns:
            A list of return values from calling ``train_func`` on each worker.
                Each item corresponds to the return value from a single worker.
        """

        self._backend_executor.pause_reporting()
        # Finish up processing checkpoints. Reporting has been disabled.
        # Results will not be processed.
        self._finish_checkpointing()
        return self._backend_executor.finish_training()

    def is_finished(self) -> bool:
        return self._finished_training

    def get_final_results(self, force: bool = False) -> List[T]:
        """Gets the training func return values from each worker.

        If ``force`` is ``True``, then immediately finish training
        and return even if all the intermediate results have not
        been processed yet. Else, intermediate results must be
        processed before obtaining the final results. Defaults to
        False.
        """
        if not self.is_finished():
            assert self._final_results is None
            if force:
                try:
                    self._final_results = self._run_with_error_handling(
                        self._finish_training
                    )
                finally:
                    self._finished_training = True
            else:
                logger.info(
                    "Please finish iterating through the "
                    "intermediate results before getting the"
                    "final returns. If you would like "
                    "training to finish immediately and get "
                    "the final returns, then set "
                    "`force=True`."
                )

        return self._final_results
