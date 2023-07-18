import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

from ray.air.checkpoint import Checkpoint
from ray.air.config import CheckpointConfig
from ray.air import session
from ray.air._internal.uri_utils import URI
from ray.air._internal.util import StartTraceback
from ray.data import Dataset
from ray.train import DataConfig
from ray.train._internal.backend_executor import (
    BackendExecutor,
    InactiveWorkerGroupError,
    TrainBackendError,
    TrainingWorkerError,
)
from ray.train._internal.checkpoint import (
    CheckpointManager,
)
from ray.train._internal.session import TrainingResultType

# Ray Train should be usable even if Tune is not installed.
from ray.train._internal.utils import ActorWrapper
from ray.train.backend import BackendConfig
from ray.train.base_trainer import (  # noqa: F401
    BaseTrainer,
    GenDataset,
    TrainingFailedError,
)
from ray.tune.trainable.util import TrainableUtil
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
        datasets: Dict[str, Dataset],
        data_config: DataConfig,
        checkpoint_manager: CheckpointManager,
        checkpoint: Optional[Union[Dict, str, Path, Checkpoint]],
        checkpoint_strategy: Optional[CheckpointConfig],
        run_dir: Optional[Path] = None,
        storage_path: Optional[str] = None,
    ):
        self._backend_executor = backend_executor
        self._backend = backend_config.backend_cls()
        self._train_func = train_func
        self._datasets = datasets
        self._data_config = data_config
        self._run_dir = run_dir
        self._checkpoint_manager = checkpoint_manager
        self._checkpoint_strategy = checkpoint_strategy
        self._storage_path = storage_path
        self._start_training(
            train_func=train_func,
            run_dir=run_dir,
            datasets=self._datasets,
            data_config=self._data_config,
            checkpoint=checkpoint,
        )

        self._final_results = None
        self._finished_training = False

    def __iter__(self):
        return self

    def _start_training(
        self,
        train_func,
        run_dir,
        datasets,
        data_config,
        checkpoint,
        latest_checkpoint_id=None,
    ):
        self._checkpoint_manager.on_start_training(
            checkpoint_strategy=self._checkpoint_strategy,
            run_dir=run_dir,
            latest_checkpoint_id=latest_checkpoint_id,
        )
        checkpoint = self._checkpoint_manager._load_checkpoint(checkpoint)
        self._run_with_error_handling(
            lambda: self._backend_executor.start_training(
                train_func=train_func,
                datasets=datasets,
                data_config=data_config,
                checkpoint=checkpoint,
            )
        )

        # Session has started. Set current cloud checkpoint dir if necessary.
        if self._checkpoint_strategy._checkpoint_upload_from_workers:
            self._backend_executor._set_checkpoint_uri(
                self.__get_cloud_checkpoint_dir()
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
                self._datasets,
                self._data_config,
                self._checkpoint_manager.latest_checkpoint,
                self._checkpoint_manager.latest_checkpoint_id,
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
                self._checkpoint_manager._process_checkpoints(
                    results, decode_checkpoint_fn=self._backend._decode_data
                )

                # Note(jungong) : This is kinda funky. We update the cloud
                # checkpoint dir on every distributed worker right after
                # an existing checkpoint is processed. We must do this because
                # Trainers do not have the concept of iterations or steps,
                # which must be synced between Trainable driver and the trainers.
                # TODO(jungong) : It would be nicer if we find a cleaner way
                # to sync the current cloud checkpointing directory between
                # Tuner, Trainable, and Trainers.
                if self._checkpoint_strategy._checkpoint_upload_from_workers:
                    self._backend_executor._set_checkpoint_uri(
                        self.__get_cloud_checkpoint_dir()
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
                self._checkpoint_manager._process_checkpoints(
                    results, decode_checkpoint_fn=self._backend._decode_data
                )
                if self._checkpoint_strategy._checkpoint_upload_from_workers:
                    self._backend_executor._set_checkpoint_uri(
                        self.__get_cloud_checkpoint_dir()
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

    # This is extremely hacky and fragile.
    # TODO(jungong) : We should refactor things so Tuner, Trinable, and
    # Trainers have a consistent view of the current cloud checkpointing
    # directory.
    # We should probably also refactor things so Syncer and SyncConfig
    # are available everywhere session is available.
    def __get_cloud_checkpoint_dir(self):
        if not self._storage_path:
            # Can't run cloud upload if storage path is not set.
            return None

        base_dir = URI(self._storage_path)
        path = Path(session.get_trial_dir())
        trial_dir_name = path.name
        exp_dir_name = path.parent.name
        checkpoint_dir_name = TrainableUtil._make_checkpoint_dir_name(
            self._checkpoint_manager._latest_checkpoint_id
        )

        return str(base_dir / exp_dir_name / trial_dir_name / checkpoint_dir_name)
