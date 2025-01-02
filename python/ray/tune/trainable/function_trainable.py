import inspect
import logging
import os
import queue
from functools import partial
from numbers import Number
from typing import Any, Callable, Dict, Optional, Type

from ray.air._internal.util import RunnerThread, StartTraceback
from ray.air.constants import _ERROR_FETCH_TIMEOUT
from ray.train._internal.checkpoint_manager import _TrainingResult
from ray.train._internal.session import (
    TrialInfo,
    _TrainSession,
    get_session,
    init_session,
    shutdown_session,
)
from ray.tune.execution.placement_groups import PlacementGroupFactory
from ray.tune.result import DEFAULT_METRIC, RESULT_DUPLICATE, SHOULD_CHECKPOINT
from ray.tune.trainable.trainable import Trainable
from ray.tune.utils import _detect_config_single
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

# Time between FunctionTrainable checks when fetching
# new results after signaling the reporter to continue

NULL_MARKER = ".null_marker"
TEMP_MARKER = ".temp_marker"


@DeveloperAPI
class FunctionTrainable(Trainable):
    """Trainable that runs a user function reporting results.

    This mode of execution does not support checkpoint/restore."""

    _name = "func"

    def setup(self, config):
        init_session(
            training_func=lambda: self._trainable_func(self.config),
            trial_info=TrialInfo(
                name=self.trial_name,
                id=self.trial_id,
                resources=self.trial_resources,
                logdir=self._storage.trial_driver_staging_path,
                driver_ip=None,
                driver_node_id=None,
                experiment_name=self._storage.experiment_dir_name,
            ),
            storage=self._storage,
            synchronous_result_reporting=True,
            # Set all Train-specific properties to None.
            world_rank=None,
            local_rank=None,
            node_rank=None,
            local_world_size=None,
            world_size=None,
            dataset_shard=None,
            checkpoint=None,
        )
        self._last_training_result: Optional[_TrainingResult] = None

    def _trainable_func(self, config: Dict[str, Any]):
        """Subclasses can override this to set the trainable func."""

        raise NotImplementedError

    def _start(self):
        def entrypoint():
            try:
                return self._trainable_func(self.config)
            except Exception as e:
                raise StartTraceback from e

        # the runner thread is not started until the first call to _train
        self._runner = RunnerThread(
            target=entrypoint, error_queue=self._error_queue, daemon=True
        )
        # if not alive, try to start
        self._status_reporter._start()
        try:
            self._runner.start()
        except RuntimeError:
            # If this is reached, it means the thread was started and is
            # now done or has raised an exception.
            pass

    def step(self):
        """Implements train() for a Function API.

        If the RunnerThread finishes without reporting "done",
        Tune will automatically provide a magic keyword __duplicate__
        along with a result with "done=True". The TrialRunner will handle the
        result accordingly (see tune/tune_controller.py).
        """
        session: _TrainSession = get_session()
        if not session.training_started:
            session.start()

        training_result: Optional[_TrainingResult] = session.get_next()

        if not training_result:
            # The `RESULT_DUPLICATE` result should have been the last
            # result reported by the session, which triggers cleanup.
            raise RuntimeError(
                "Should not have reached here. The TuneController should not "
                "have scheduled another `train` remote call."
                "It should have scheduled a `stop` instead "
                "after the training function exits."
            )

        metrics = training_result.metrics
        # This keyword appears if the train_func using the Function API
        # finishes without "done=True". This duplicates the last result, but
        # the TuneController will not log this result again.
        # TuneController will also inject done=True to the result,
        # and proceed to queue up a STOP decision for the trial.
        if RESULT_DUPLICATE in metrics:
            metrics[SHOULD_CHECKPOINT] = False

        self._last_training_result = training_result
        if training_result.checkpoint is not None:
            # TODO(justinvyu): Result/checkpoint reporting can be combined.
            # For now, since result/checkpoint reporting is separate, this
            # special key will tell Tune to pull the checkpoint from
            # the `last_training_result`.
            metrics[SHOULD_CHECKPOINT] = True
        return metrics

    def execute(self, fn):
        return fn(self)

    def save_checkpoint(self, checkpoint_dir: str = ""):
        if checkpoint_dir:
            raise ValueError("Checkpoint dir should not be used with function API.")

        # TODO(justinvyu): This currently breaks the `save_checkpoint` interface.
        # TRAIN -> SAVE remote calls get processed sequentially,
        # so `_last_training_result.checkpoint` holds onto the latest ckpt.
        return self._last_training_result

    def load_checkpoint(self, checkpoint_result: _TrainingResult):
        # TODO(justinvyu): This currently breaks the `load_checkpoint` interface.
        session = get_session()
        session.loaded_checkpoint = checkpoint_result.checkpoint

    def cleanup(self):
        session = get_session()
        try:
            # session.finish raises any Exceptions from training.
            # Do not wait for thread termination here (timeout=0).
            session.finish(timeout=0)
        finally:
            # Check for any errors that might have been missed.
            session._report_thread_runner_error()
            # Shutdown session even if session.finish() raises an Exception.
            shutdown_session()

    def reset_config(self, new_config):
        session = get_session()

        # Wait for thread termination so it is save to re-use the same actor.
        thread_timeout = int(os.environ.get("TUNE_FUNCTION_THREAD_TIMEOUT_S", 2))
        session.finish(timeout=thread_timeout)
        if session.training_thread.is_alive():
            # Did not finish within timeout, reset unsuccessful.
            return False

        session.reset(
            training_func=lambda: self._trainable_func(self.config),
            trial_info=TrialInfo(
                name=self.trial_name,
                id=self.trial_id,
                resources=self.trial_resources,
                logdir=self._storage.trial_working_directory,
                driver_ip=None,
                driver_node_id=None,
                experiment_name=self._storage.experiment_dir_name,
            ),
            storage=self._storage,
        )

        self._last_result = {}
        return True

    def _report_thread_runner_error(self, block=False):
        try:
            e = self._error_queue.get(block=block, timeout=_ERROR_FETCH_TIMEOUT)
            raise StartTraceback from e
        except queue.Empty:
            pass


@DeveloperAPI
def wrap_function(
    train_func: Callable[[Any], Any], name: Optional[str] = None
) -> Type["FunctionTrainable"]:
    inherit_from = (FunctionTrainable,)

    if hasattr(train_func, "__mixins__"):
        inherit_from = train_func.__mixins__ + inherit_from

    func_args = inspect.getfullargspec(train_func).args
    use_config_single = _detect_config_single(train_func)

    if not use_config_single:
        raise ValueError(
            "Unknown argument found in the Trainable function. "
            "The function args must include a single 'config' positional parameter.\n"
            "Found: {}".format(func_args)
        )

    resources = getattr(train_func, "_resources", None)

    class ImplicitFunc(*inherit_from):
        _name = name or (
            train_func.__name__ if hasattr(train_func, "__name__") else "func"
        )

        def __repr__(self):
            return self._name

        def _trainable_func(self, config):
            fn = partial(train_func, config)

            def handle_output(output):
                if not output:
                    return
                elif isinstance(output, dict):
                    get_session().report(output)
                elif isinstance(output, Number):
                    get_session().report({DEFAULT_METRIC: output})
                else:
                    raise ValueError(
                        "Invalid return or yield value. Either return/yield "
                        "a single number or a dictionary object in your "
                        "trainable function."
                    )

            output = None
            if inspect.isgeneratorfunction(train_func):
                for output in fn():
                    handle_output(output)
            else:
                output = fn()
                handle_output(output)

            # If train_func returns, we need to notify the main event loop
            # of the last result while avoiding double logging. This is done
            # with the keyword RESULT_DUPLICATE -- see tune/tune_controller.py.
            get_session().report({RESULT_DUPLICATE: True})
            return output

        @classmethod
        def default_resource_request(
            cls, config: Dict[str, Any]
        ) -> Optional[PlacementGroupFactory]:
            if not isinstance(resources, PlacementGroupFactory) and callable(resources):
                return resources(config)
            return resources

    return ImplicitFunc
