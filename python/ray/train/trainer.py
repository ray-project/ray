import logging
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

from ray.air._internal.util import (
    StartTraceback,
    StartTracebackWithWorkerRank,
    skip_exceptions,
)
from ray.data import Dataset
from ray.train import Checkpoint, DataConfig
from ray.train._internal.backend_executor import (
    BackendExecutor,
    InactiveWorkerGroupError,
    TrainBackendError,
    TrainingWorkerError,
)
from ray.train._internal.session import _TrainingResult, _TrainSession, get_session
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
        datasets: Dict[str, Dataset],
        metadata: Dict[str, Any],
        data_config: DataConfig,
        checkpoint: Optional[Union[Dict, str, Path, Checkpoint]],
    ):
        self._backend_executor = backend_executor
        self._backend = backend_config.backend_cls()
        self._train_func = train_func
        self._datasets = datasets
        self._metadata = metadata
        self._data_config = data_config

        self._start_training(
            train_func=train_func,
            datasets=self._datasets,
            metadata=self._metadata,
            data_config=self._data_config,
            checkpoint=checkpoint,
        )

        self._finished_training = False

    def __iter__(self):
        return self

    def _start_training(
        self,
        train_func,
        datasets,
        metadata,
        data_config,
        checkpoint: Optional[Checkpoint] = None,
    ):
        tune_session: _TrainSession = get_session()
        assert tune_session, "`_start_training` should only be called from within Tune"
        storage = tune_session.storage

        self._run_with_error_handling(
            lambda: self._backend_executor.start_training(
                train_func=train_func,
                datasets=datasets,
                metadata=metadata,
                data_config=data_config,
                storage=storage,
                checkpoint=checkpoint,
            )
        )

    def _run_with_error_handling(self, func: Callable):
        try:
            return func()
        except TrainingWorkerError:
            # TODO(ml-team): This Train fault-tolerance code doesn't get used
            # since max_retries=0
            # Workers have already been restarted.
            logger.info(
                "Workers have been successfully restarted. Resuming "
                "training from latest checkpoint."
            )
            self._start_training(
                self._train_func,
                self._datasets,
                self._metadata,
                self._data_config,
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
            self._backend_executor.report_final_run_status(errored=False)
            raise StopIteration
        try:
            next_results = self._run_with_error_handling(self._fetch_next_result)
            if next_results is None:
                self._backend_executor.report_final_run_status(errored=False)
                self._run_with_error_handling(self._finish_training)
                self._finished_training = True
                raise StopIteration
            else:
                return next_results
        except StartTraceback as e:
            # If this is a StartTraceback, then this is a user error.
            # We raise it directly
            if isinstance(e, StartTracebackWithWorkerRank):
                failed_rank = e.worker_rank
            else:
                failed_rank = None

            # Extract the stack trace from the exception
            e = skip_exceptions(e)
            stack_trace = "".join(
                traceback.format_exception(type(e), e, e.__traceback__)
            )

            self._backend_executor.report_final_run_status(
                errored=True, stack_trace=stack_trace, failed_rank=failed_rank
            )
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
        results = self._backend_executor.get_next_results()
        if results is None:
            return None
        assert all(isinstance(result, _TrainingResult) for result in results)
        return results

    def _finish_training(self):
        """Finish training and return final results. Propagate any exceptions.

        Blocks until training is finished on all workers.

        Assumes `start_training` has already been called.

        Returns:
            A list of return values from calling ``train_func`` on each worker.
                Each item corresponds to the return value from a single worker.
        """
        return self._backend_executor.finish_training()

    def is_finished(self) -> bool:
        return self._finished_training
