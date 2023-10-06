import inspect
import logging
import os
import shutil
import sys
import threading
import time
import uuid
from functools import partial
from numbers import Number
from typing import Any, Callable, Dict, Optional, Type

from ray.air._internal.util import StartTraceback, RunnerThread
import queue

from ray.air.constants import (
    _ERROR_FETCH_TIMEOUT,
    TIME_THIS_ITER_S,
)
import ray.train
from ray.train import Checkpoint
from ray.train._internal.checkpoint_manager import _TrainingResult
from ray.train._internal.session import (
    init_session,
    get_session,
    shutdown_session,
    _TrainSession,
    TrialInfo,
)
from ray.tune.execution.placement_groups import PlacementGroupFactory
from ray.tune.result import (
    DEFAULT_METRIC,
    RESULT_DUPLICATE,
    SHOULD_CHECKPOINT,
)
from ray.tune.trainable import Trainable, TrainableUtil
from ray.tune.utils import (
    _detect_checkpoint_function,
    _detect_config_single,
    _detect_reporter,
)
from ray.util.annotations import DeveloperAPI


logger = logging.getLogger(__name__)

# Time between FunctionTrainable checks when fetching
# new results after signaling the reporter to continue

NULL_MARKER = ".null_marker"
TEMP_MARKER = ".temp_marker"


_CHECKPOINT_DIR_ARG_DEPRECATION_MSG = """Accepting a `checkpoint_dir` argument in your training function is deprecated.
Please use `ray.train.get_checkpoint()` to access your checkpoint as a
`ray.train.Checkpoint` object instead. See below for an example:

Before
------

from ray import tune

def train_fn(config, checkpoint_dir=None):
    if checkpoint_dir:
        torch.load(os.path.join(checkpoint_dir, "checkpoint.pt"))
    ...

tuner = tune.Tuner(train_fn)
tuner.fit()

After
-----

from ray import train, tune

def train_fn(config):
    checkpoint: train.Checkpoint = train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            torch.load(os.path.join(checkpoint_dir, "checkpoint.pt"))
    ...

tuner = tune.Tuner(train_fn)
tuner.fit()"""  # noqa: E501

_REPORTER_ARG_DEPRECATION_MSG = """Accepting a `reporter` in your training function is deprecated.
Please use `ray.train.report()` to report results instead. See below for an example:

Before
------

from ray import tune

def train_fn(config, reporter):
    reporter(metric=1)

tuner = tune.Tuner(train_fn)
tuner.fit()

After
-----

from ray import train, tune

def train_fn(config):
    train.report({"metric": 1})

tuner = tune.Tuner(train_fn)
tuner.fit()"""  # noqa: E501


# TODO(justinvyu): [code_removal]
@DeveloperAPI
class FuncCheckpointUtil:
    """Utility class holding various function-checkpointing mechanisms.

    The two special modes are "null" and "temporary" checkpoints.

    *Null Checkpoints*
    -------------------

    Null checkpoints are generated when a trial is being saved
    but a checkpoint has not been created. In this case,
    a marker is set, indicating that the checkpoint is null.

    When restoring from an null checkpoint, the FunctionTrainable
    will detect this and *not* restore from any checkpoint at all.

    *Temporary Checkpoints*
    -----------------------

    Temporary checkpoints are generated when a trial is being
    restored from a prior in-memory checkpoint. In this case, a marker
    will be set indicating that a checkpoint is temporary.

    Upon termination of the trial, temporary checkpoints
    will be removed. We cannot remove them any earlier because
    the loading of checkpoints is non-deterministic.

    If "save" is called on a trial whose most recent checkpoint
    is temporary, "create_perm_checkpoint" will be called. This
    copies the temporary checkpoint to a permanent checkpoint directory.
    """

    @staticmethod
    def mk_null_checkpoint_dir(logdir):
        """Indicate that the given checkpoint doesn't have state."""
        checkpoint_dir = TrainableUtil.make_checkpoint_dir(
            logdir, index=-1, override=True
        )
        open(os.path.join(checkpoint_dir, NULL_MARKER), "a").close()
        return checkpoint_dir

    @staticmethod
    def mk_temp_checkpoint_dir(logdir):
        """Indicate that the checkpoint is only for restoration."""
        temporary_checkpoint_dir = TrainableUtil.make_checkpoint_dir(
            logdir, index="tmp" + uuid.uuid4().hex[:6], override=True
        )
        open(os.path.join(temporary_checkpoint_dir, TEMP_MARKER), "a").close()
        return temporary_checkpoint_dir

    @staticmethod
    def is_temp_checkpoint_dir(checkpoint_dir):
        """Checks for the temp checkpoint marker."""
        return os.path.exists(os.path.join(checkpoint_dir, TEMP_MARKER))

    @staticmethod
    def is_null_checkpoint(checkpoint_dir):
        """Checks for the empty checkpoint marker."""
        return os.path.exists(os.path.join(checkpoint_dir, NULL_MARKER))

    @staticmethod
    def create_perm_checkpoint(checkpoint_dir, logdir, step):
        """Copies temporary checkpoint to a permanent checkpoint directory."""
        checkpoint_dir = os.path.abspath(checkpoint_dir)
        temporary_marker = os.path.join(checkpoint_dir, TEMP_MARKER)
        assert os.path.exists(
            temporary_marker
        ), "Should not be calling this method on a permanent checkpoint."
        os.remove(temporary_marker)
        perm_checkpoint_dir = TrainableUtil.make_checkpoint_dir(
            logdir, index=step, override=True
        )
        shutil.rmtree(perm_checkpoint_dir)

        shutil.copytree(checkpoint_dir, perm_checkpoint_dir)
        assert not os.path.exists(os.path.join(perm_checkpoint_dir, TEMP_MARKER))
        return perm_checkpoint_dir


# TODO(justinvyu): [code_removal]
@DeveloperAPI
class _StatusReporter:
    def __init__(
        self,
        result_queue: queue.Queue,
        continue_semaphore: threading.Semaphore,
        end_event: threading.Event,
        training_iteration_func: Callable[[], int],
        experiment_name: Optional[str] = None,
        trial_name: Optional[str] = None,
        trial_id: Optional[str] = None,
        logdir: Optional[str] = None,
        trial_resources: Optional[PlacementGroupFactory] = None,
    ):
        self._queue = result_queue
        self._last_report_time = None
        self._continue_semaphore = continue_semaphore
        self._end_event = end_event
        self._get_training_iteration = training_iteration_func
        self._experiment_name = experiment_name
        self._trial_name = trial_name
        self._trial_id = trial_id
        self._logdir = logdir
        self._last_checkpoint = None
        self._fresh_checkpoint = False
        self._trial_resources = trial_resources
        # Mark whether the `ray.air.session.report()` API is being used,
        # to throw an error if `tune.report()` is called as well
        self._air_session_has_reported = False

    def reset(self, trial_name=None, trial_id=None, logdir=None, trial_resources=None):
        self._trial_name = trial_name
        self._trial_id = trial_id
        self._logdir = logdir
        self._last_checkpoint = None
        self._latest_checkpoint_result = None
        self._fresh_checkpoint = False
        self._trial_resources = trial_resources
        self._air_session_has_reported = False

    def __call__(self, _metric=None, **kwargs):
        """Report updated training status.

        Pass in `done=True` when the training job is completed.

        Args:
            kwargs: Latest training result status.

        Raises:
            StopIteration: A StopIteration exception is raised if the trial has
                been signaled to stop.
        """

        assert self._last_report_time is not None, (
            "_StatusReporter._start() must be called before the first "
            "report __call__ is made to ensure correct runtime metrics."
        )

        if _metric:
            kwargs[DEFAULT_METRIC] = _metric

        # time per iteration is recorded directly in the reporter to ensure
        # any delays in logging results aren't counted
        report_time = time.time()
        if TIME_THIS_ITER_S not in kwargs:
            kwargs[TIME_THIS_ITER_S] = report_time - self._last_report_time
        self._last_report_time = report_time

        # add results to a thread-safe queue
        self._queue.put(kwargs.copy(), block=True)

        # This blocks until notification from the FunctionRunner that the last
        # result has been returned to Tune and that the function is safe to
        # resume training.
        self._continue_semaphore.acquire()

        # If the trial should be terminated, exit gracefully.
        if self._end_event.is_set():
            self._end_event.clear()
            sys.exit(0)

    def make_checkpoint_dir(self, step):
        checkpoint_dir = TrainableUtil.make_checkpoint_dir(self.logdir, index=step)
        logger.debug("Making checkpoint dir at %s", checkpoint_dir)
        return checkpoint_dir

    def set_checkpoint(self, checkpoint, is_new=True):
        """Sets the checkpoint to be returned upon get_checkpoint.

        If this is a "new" checkpoint, it will notify Tune
        (via has_new_checkpoint). Otherwise, it will NOT notify Tune.
        """
        if isinstance(checkpoint, str):
            try:
                TrainableUtil.find_checkpoint_dir(checkpoint)
            except FileNotFoundError:
                logger.error(
                    "Checkpoint must be created with path given from "
                    "make_checkpoint_dir."
                )
                raise
        previous_checkpoint = self._last_checkpoint
        self._last_checkpoint = checkpoint
        if is_new:
            self._fresh_checkpoint = True

        # Delete temporary checkpoint folder from `restore_from_object`
        if previous_checkpoint and FuncCheckpointUtil.is_temp_checkpoint_dir(
            previous_checkpoint
        ):
            previous_checkpoint_dir = TrainableUtil.find_checkpoint_dir(
                previous_checkpoint
            )
            shutil.rmtree(previous_checkpoint_dir, ignore_errors=True)

    def has_new_checkpoint(self):
        return self._fresh_checkpoint

    def get_checkpoint(self):
        # NOTE: This is not the same as `train.get_checkpoint`.
        # This is used internally by `FunctionTrainable.save_checkpoint`.
        # `loaded_checkpoint` is the checkpoint accessible by the user.
        self._fresh_checkpoint = False
        return self._last_checkpoint

    def _start(self):
        self._last_report_time = time.time()

    def report(self, metrics: Dict, *, checkpoint: Optional[Checkpoint] = None) -> None:
        # TODO(xwjiang): Tons of optimizations.
        self._air_session_has_reported = True
        if checkpoint:
            training_iteration = self._get_training_iteration()
            checkpoint_dir = self.make_checkpoint_dir(step=training_iteration)
            self.set_checkpoint(checkpoint_dir)
            checkpoint.to_directory(checkpoint_dir)
            # TODO(krfricke): Remove this once support is added in Checkpoint.
            open(os.path.join(checkpoint_dir, ".is_checkpoint"), "a").close()
        self.__call__(**metrics)

    @property
    def loaded_checkpoint(self) -> Optional[Checkpoint]:
        if self._last_checkpoint:
            assert isinstance(self._last_checkpoint, str)
            return Checkpoint.from_directory(self._last_checkpoint)
        return None

    @property
    def logdir(self):
        return self._logdir

    @property
    def experiment_name(self):
        """Trial name for the corresponding trial of this Trainable."""
        return self._experiment_name

    @property
    def trial_name(self):
        """Trial name for the corresponding trial of this Trainable."""
        return self._trial_name

    @property
    def trial_id(self):
        """Trial id for the corresponding trial of this Trainable."""
        return self._trial_id

    @property
    def trial_resources(self):
        """Resources assigned to the trial of this Trainable."""
        return self._trial_resources

    @property
    def trial_dir(self) -> str:
        """Trial-level log directory for the corresponding trial."""
        return self._logdir


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
                logdir=self._storage.trial_local_path,
                driver_ip=None,
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

    def _create_checkpoint_dir(
        self, checkpoint_dir: Optional[str] = None
    ) -> Optional[str]:
        return None

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
                logdir=self._storage.trial_local_path,
                driver_ip=None,
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
    train_func: Callable[[Any], Any], warn: bool = True, name: Optional[str] = None
) -> Type["FunctionTrainable"]:
    inherit_from = (FunctionTrainable,)

    if hasattr(train_func, "__mixins__"):
        inherit_from = train_func.__mixins__ + inherit_from

    func_args = inspect.getfullargspec(train_func).args
    use_checkpoint = _detect_checkpoint_function(train_func)
    use_config_single = _detect_config_single(train_func)
    use_reporter = _detect_reporter(train_func)

    if use_checkpoint:
        raise DeprecationWarning(_CHECKPOINT_DIR_ARG_DEPRECATION_MSG)

    if use_reporter:
        raise DeprecationWarning(_REPORTER_ARG_DEPRECATION_MSG)

    if not use_config_single:
        # use_reporter is hidden
        raise ValueError(
            "Unknown argument found in the Trainable function. "
            "The function args must include a 'config' positional parameter."
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
                    ray.train.report(output)
                elif isinstance(output, Number):
                    ray.train.report({DEFAULT_METRIC: output})
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
            ray.train.report({RESULT_DUPLICATE: True})
            return output

        @classmethod
        def default_resource_request(
            cls, config: Dict[str, Any]
        ) -> Optional[PlacementGroupFactory]:
            if not isinstance(resources, PlacementGroupFactory) and callable(resources):
                return resources(config)
            return resources

    return ImplicitFunc
