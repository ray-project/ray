import logging
import os
import sys
import time
import inspect
import shutil
import threading
import traceback
import uuid
from functools import partial
from numbers import Number

from typing import Any, Callable, Optional

from six.moves import queue

from ray.util.debug import log_once
from ray.tune import TuneError, session
from ray.tune.trainable import Trainable, TrainableUtil
from ray.tune.result import (DEFAULT_METRIC, TIME_THIS_ITER_S,
                             RESULT_DUPLICATE, SHOULD_CHECKPOINT)
from ray.tune.utils import (detect_checkpoint_function, detect_config_single,
                            detect_reporter)
from ray.tune.utils.trainable import with_parameters  # noqa: F401

logger = logging.getLogger(__name__)

# Time between FunctionRunner checks when fetching
# new results after signaling the reporter to continue
RESULT_FETCH_TIMEOUT = 0.2

ERROR_REPORT_TIMEOUT = 10
ERROR_FETCH_TIMEOUT = 1

NULL_MARKER = ".null_marker"
TEMP_MARKER = ".temp_marker"


class FuncCheckpointUtil:
    """Utility class holding various function-checkpointing mechanisms.

    The two special modes are "null" and "temporary" checkpoints.

    *Null Checkpoints*
    -------------------

    Null checkpoints are generated when a trial is being saved
    but a checkpoint has not been created. In this case,
    a marker is set, indicating that the checkpoint is null.

    When restoring from an null checkpoint, the FunctionRunner
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
            logdir, index=-1, override=True)
        open(os.path.join(checkpoint_dir, NULL_MARKER), "a").close()
        return checkpoint_dir

    @staticmethod
    def mk_temp_checkpoint_dir(logdir):
        """Indicate that the checkpoint is only for restoration."""
        temporary_checkpoint_dir = TrainableUtil.make_checkpoint_dir(
            logdir, index="tmp" + uuid.uuid4().hex[:6], override=True)
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
        assert os.path.exists(temporary_marker), (
            "Should not be calling this method on a permanent checkpoint.")
        os.remove(temporary_marker)
        perm_checkpoint_dir = TrainableUtil.make_checkpoint_dir(
            logdir, index=step, override=True)
        shutil.rmtree(perm_checkpoint_dir)

        shutil.copytree(checkpoint_dir, perm_checkpoint_dir)
        assert not os.path.exists(
            os.path.join(perm_checkpoint_dir, TEMP_MARKER))
        return perm_checkpoint_dir


class StatusReporter:
    """Object passed into your function that you can report status through.

    Example:
        >>> def trainable_function(config, reporter):
        >>>     assert isinstance(reporter, StatusReporter)
        >>>     reporter(timesteps_this_iter=1)
    """

    def __init__(self,
                 result_queue,
                 continue_semaphore,
                 end_event,
                 trial_name=None,
                 trial_id=None,
                 logdir=None,
                 trial_resources=None):
        self._queue = result_queue
        self._last_report_time = None
        self._continue_semaphore = continue_semaphore
        self._end_event = end_event
        self._trial_name = trial_name
        self._trial_id = trial_id
        self._logdir = logdir
        self._last_checkpoint = None
        self._fresh_checkpoint = False
        self._trial_resources = trial_resources

    def reset(self,
              trial_name=None,
              trial_id=None,
              logdir=None,
              trial_resources=None):
        self._trial_name = trial_name
        self._trial_id = trial_id
        self._logdir = logdir
        self._last_checkpoint = None
        self._fresh_checkpoint = False
        self._trial_resources = trial_resources

    def __call__(self, _metric=None, **kwargs):
        """Report updated training status.

        Pass in `done=True` when the training job is completed.

        Args:
            kwargs: Latest training result status.

        Example:
            >>> reporter(mean_accuracy=1, training_iteration=4)
            >>> reporter(mean_accuracy=1, training_iteration=4, done=True)

        Raises:
            StopIteration: A StopIteration exception is raised if the trial has
                been signaled to stop.
        """

        assert self._last_report_time is not None, (
            "StatusReporter._start() must be called before the first "
            "report __call__ is made to ensure correct runtime metrics.")

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
        checkpoint_dir = TrainableUtil.make_checkpoint_dir(
            self.logdir, index=step)
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
                logger.error("Checkpoint must be created with path given from "
                             "make_checkpoint_dir.")
                raise
        self._last_checkpoint = checkpoint
        if is_new:
            self._fresh_checkpoint = True

    def has_new_checkpoint(self):
        return self._fresh_checkpoint

    def get_checkpoint(self):
        self._fresh_checkpoint = False
        return self._last_checkpoint

    def _start(self):
        self._last_report_time = time.time()

    @property
    def logdir(self):
        return self._logdir

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


class _RunnerThread(threading.Thread):
    """Supervisor thread that runs your script."""

    def __init__(self, entrypoint, error_queue):
        threading.Thread.__init__(self)
        self._entrypoint = entrypoint
        self._error_queue = error_queue
        self.daemon = True

    def run(self):
        try:
            self._entrypoint()
        except StopIteration:
            logger.debug(
                ("Thread runner raised StopIteration. Interperting it as a "
                 "signal to terminate the thread without error."))
        except Exception as e:
            logger.exception("Runner Thread raised error.")
            try:
                # report the error but avoid indefinite blocking which would
                # prevent the exception from being propagated in the unlikely
                # case that something went terribly wrong
                err_tb_str = traceback.format_exc()
                self._error_queue.put(
                    err_tb_str, block=True, timeout=ERROR_REPORT_TIMEOUT)
            except queue.Full:
                logger.critical(
                    ("Runner Thread was unable to report error to main "
                     "function runner thread. This means a previous error "
                     "was not processed. This should never happen."))
            raise e


class FunctionRunner(Trainable):
    """Trainable that runs a user function reporting results.

    This mode of execution does not support checkpoint/restore."""

    _name = "func"

    def setup(self, config):
        # Semaphore for notifying the reporter to continue with the computation
        # and to generate the next result.
        self._continue_semaphore = threading.Semaphore(0)

        # Event for notifying the reporter to exit gracefully, terminating
        # the thread.
        self._end_event = threading.Event()

        # Queue for passing results between threads
        self._results_queue = queue.Queue(1)

        # Queue for passing errors back from the thread runner. The error queue
        # has a max size of one to prevent stacking error and force error
        # reporting to block until finished.
        self._error_queue = queue.Queue(1)

        self._status_reporter = StatusReporter(
            self._results_queue,
            self._continue_semaphore,
            self._end_event,
            trial_name=self.trial_name,
            trial_id=self.trial_id,
            logdir=self.logdir,
            trial_resources=self.trial_resources)
        self._last_result = {}

        session.init(self._status_reporter)
        self._runner = None
        self._restore_tmpdir = None
        self.temp_checkpoint_dir = None

    def _trainable_func(self, config, reporter, checkpoint_dir):
        """Subclasses can override this to set the trainable func."""

        raise NotImplementedError

    def _start(self):
        def entrypoint():
            return self._trainable_func(self.config, self._status_reporter,
                                        self._status_reporter.get_checkpoint())

        # the runner thread is not started until the first call to _train
        self._runner = _RunnerThread(entrypoint, self._error_queue)
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
        result accordingly (see tune/trial_runner.py).
        """
        if self._runner and self._runner.is_alive():
            # if started and alive, inform the reporter to continue and
            # generate the next result
            self._continue_semaphore.release()
        else:
            self._start()

        result = None
        while result is None and self._runner.is_alive():
            # fetch the next produced result
            try:
                result = self._results_queue.get(
                    block=True, timeout=RESULT_FETCH_TIMEOUT)
            except queue.Empty:
                pass

        # if no result were found, then the runner must no longer be alive
        if result is None:
            # Try one last time to fetch results in case results were reported
            # in between the time of the last check and the termination of the
            # thread runner.
            try:
                result = self._results_queue.get(block=False)
            except queue.Empty:
                pass

        # check if error occurred inside the thread runner
        if result is None:
            # only raise an error from the runner if all results are consumed
            self._report_thread_runner_error(block=True)

            # Under normal conditions, this code should never be reached since
            # this branch should only be visited if the runner thread raised
            # an exception. If no exception were raised, it means that the
            # runner thread never reported any results which should not be
            # possible when wrapping functions with `wrap_function`.
            raise TuneError(
                ("Wrapped function ran until completion without reporting "
                 "results or raising an exception."))

        else:
            if not self._error_queue.empty():
                logger.warning(
                    ("Runner error waiting to be raised in main thread. "
                     "Logging all available results first."))

        # This keyword appears if the train_func using the Function API
        # finishes without "done=True". This duplicates the last result, but
        # the TrialRunner will not log this result again.
        if RESULT_DUPLICATE in result:
            new_result = self._last_result.copy()
            new_result.update(result)
            result = new_result

        self._last_result = result
        if self._status_reporter.has_new_checkpoint():
            result[SHOULD_CHECKPOINT] = True
        return result

    def execute(self, fn):
        return fn(self)

    def save(self, checkpoint_path=None) -> str:
        if checkpoint_path:
            raise ValueError(
                "Checkpoint path should not be used with function API.")

        checkpoint = self._status_reporter.get_checkpoint()
        state = self.get_state()

        if not checkpoint:
            state.update(iteration=0, timesteps_total=0, episodes_total=0)
            # We drop a marker here to indicate that the checkpoint is empty
            checkpoint = FuncCheckpointUtil.mk_null_checkpoint_dir(self.logdir)
            parent_dir = checkpoint
        elif isinstance(checkpoint, dict):
            parent_dir = TrainableUtil.make_checkpoint_dir(
                self.logdir, index=self.training_iteration)
        elif isinstance(checkpoint, str):
            parent_dir = TrainableUtil.find_checkpoint_dir(checkpoint)
            # When the trainable is restored, a temporary checkpoint
            # is created. However, when saved, it should become permanent.
            # Ideally, there are no save calls upon a temporary
            # checkpoint, but certain schedulers might.
            if FuncCheckpointUtil.is_temp_checkpoint_dir(parent_dir):
                relative_path = os.path.relpath(checkpoint, parent_dir)
                parent_dir = FuncCheckpointUtil.create_perm_checkpoint(
                    checkpoint_dir=parent_dir,
                    logdir=self.logdir,
                    step=self.training_iteration)
                checkpoint = os.path.abspath(
                    os.path.join(parent_dir, relative_path))
        else:
            raise ValueError("Provided checkpoint was expected to have "
                             "type (str, dict). Got {}.".format(
                                 type(checkpoint)))

        checkpoint_path = TrainableUtil.process_checkpoint(
            checkpoint, parent_dir, state)

        self._maybe_save_to_cloud(parent_dir)

        return checkpoint_path

    def save_to_object(self):
        checkpoint_path = self.save()
        obj = TrainableUtil.checkpoint_to_object(checkpoint_path)
        return obj

    def load_checkpoint(self, checkpoint):
        # This should be removed once Trainables are refactored.
        if "tune_checkpoint_path" in checkpoint:
            del checkpoint["tune_checkpoint_path"]
        # If there does not exist a checkpoint, we will not restore
        # from it and will remove the marker.
        if FuncCheckpointUtil.is_null_checkpoint(checkpoint):
            return
        # By informing that this checkpoint is not new,
        # we will not return the checkpoint path
        # as a new checkpoint.
        self._status_reporter.set_checkpoint(checkpoint, is_new=False)

    def restore_from_object(self, obj):
        self.temp_checkpoint_dir = (FuncCheckpointUtil.mk_temp_checkpoint_dir(
            self.logdir))
        checkpoint_path = TrainableUtil.create_from_pickle(
            obj, self.temp_checkpoint_dir)
        self.restore(checkpoint_path)

    def cleanup(self):
        # Trigger thread termination
        self._end_event.set()
        self._continue_semaphore.release()
        # Do not wait for thread termination here.

        # If everything stayed in synch properly, this should never happen.
        if not self._results_queue.empty():
            logger.warning(
                ("Some results were added after the trial stop condition. "
                 "These results won't be logged."))

        # Check for any errors that might have been missed.
        self._report_thread_runner_error()
        session.shutdown()

        if self.temp_checkpoint_dir is not None and os.path.exists(
                self.temp_checkpoint_dir):
            shutil.rmtree(self.temp_checkpoint_dir)
            logger.debug("Clearing temporary checkpoint: %s",
                         self.temp_checkpoint_dir)

    def reset_config(self, new_config):
        if self._runner and self._runner.is_alive():
            self._end_event.set()
            self._continue_semaphore.release()
            # Wait for thread termination so it is save to re-use the same
            # actor.
            thread_timeout = int(
                os.environ.get("TUNE_FUNCTION_THREAD_TIMEOUT_S", 2))
            self._runner.join(timeout=thread_timeout)
            if self._runner.is_alive():
                # Did not finish within timeout, reset unsuccessful.
                return False

        self._runner = None
        self._last_result = {}

        self._status_reporter.reset(
            trial_name=self.trial_name,
            trial_id=self.trial_id,
            logdir=self.logdir,
            trial_resources=self.trial_resources)

        return True

    def _report_thread_runner_error(self, block=False):
        try:
            err_tb_str = self._error_queue.get(
                block=block, timeout=ERROR_FETCH_TIMEOUT)
            raise TuneError(
                ("Trial raised an exception. Traceback:\n{}".format(err_tb_str)
                 ))
        except queue.Empty:
            pass


def wrap_function(train_func: Callable[[Any], Any],
                  warn: bool = True,
                  name: Optional[str] = None):
    inherit_from = (FunctionRunner, )

    if hasattr(train_func, "__mixins__"):
        inherit_from = train_func.__mixins__ + inherit_from

    func_args = inspect.getfullargspec(train_func).args
    use_checkpoint = detect_checkpoint_function(train_func)
    use_config_single = detect_config_single(train_func)
    use_reporter = detect_reporter(train_func)

    if not any([use_checkpoint, use_config_single, use_reporter]):
        # use_reporter is hidden
        raise ValueError(
            "Unknown argument found in the Trainable function. "
            "The function args must include a 'config' positional "
            "parameter. Any other args must be 'checkpoint_dir'. "
            "Found: {}".format(func_args))

    if use_config_single and not use_checkpoint:
        if log_once("tune_function_checkpoint") and warn:
            logger.warning(
                "Function checkpointing is disabled. This may result in "
                "unexpected behavior when using checkpointing features or "
                "certain schedulers. To enable, set the train function "
                "arguments to be `func(config, checkpoint_dir=None)`.")

    class ImplicitFunc(*inherit_from):
        _name = name or (train_func.__name__
                         if hasattr(train_func, "__name__") else "func")

        def __repr__(self):
            return self._name

        def _trainable_func(self, config, reporter, checkpoint_dir):
            if not use_checkpoint and not use_reporter:
                fn = partial(train_func, config)
            elif use_checkpoint:
                fn = partial(train_func, config, checkpoint_dir=checkpoint_dir)
            else:
                fn = partial(train_func, config, reporter)

            def handle_output(output):
                if not output:
                    return
                elif isinstance(output, dict):
                    reporter(**output)
                elif isinstance(output, Number):
                    reporter(_metric=output)
                else:
                    raise ValueError(
                        "Invalid return or yield value. Either return/yield "
                        "a single number or a dictionary object in your "
                        "trainable function.")

            output = None
            if inspect.isgeneratorfunction(train_func):
                for output in fn():
                    handle_output(output)
            else:
                output = fn()
                handle_output(output)

            # If train_func returns, we need to notify the main event loop
            # of the last result while avoiding double logging. This is done
            # with the keyword RESULT_DUPLICATE -- see tune/trial_runner.py.
            reporter(**{RESULT_DUPLICATE: True})
            return output

    return ImplicitFunc
