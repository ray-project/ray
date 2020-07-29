import logging
import os
import time
import inspect
import shutil
import threading
import traceback

from six.moves import queue

from ray.tune import TuneError, session
from ray.tune.trainable import Trainable, TrainableUtil
from ray.tune.result import (TIME_THIS_ITER_S, RESULT_DUPLICATE,
                             SHOULD_CHECKPOINT)

logger = logging.getLogger(__name__)

# Time between FunctionRunner checks when fetching
# new results after signaling the reporter to continue
RESULT_FETCH_TIMEOUT = 0.2

ERROR_REPORT_TIMEOUT = 10
ERROR_FETCH_TIMEOUT = 1


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
                 trial_name=None,
                 trial_id=None,
                 logdir=None):
        self._queue = result_queue
        self._last_report_time = None
        self._continue_semaphore = continue_semaphore
        self._trial_name = trial_name
        self._trial_id = trial_id
        self._logdir = logdir
        self._last_checkpoint = {}
        self._fresh_checkpoint = False

    def __call__(self, **kwargs):
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

    def make_checkpoint_dir(self, step=None):
        checkpoint_dir = TrainableUtil.make_checkpoint_dir(
            self.logdir, index=step)
        logger.debug("Making checkpoint dir at %s", checkpoint_dir)
        return checkpoint_dir

    def save_checkpoint(self, checkpoint):
        if isinstance(checkpoint, str):
            try:
                TrainableUtil.find_checkpoint_dir(checkpoint)
            except FileNotFoundError:
                logger.error("Checkpoint must be created with path given from "
                             "make_checkpoint_dir.")
                raise
        self._last_checkpoint = checkpoint
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

        # Queue for passing results between threads
        self._results_queue = queue.Queue(1)

        # Queue for passing errors back from the thread runner. The error queue
        # has a max size of one to prevent stacking error and force error
        # reporting to block until finished.
        self._error_queue = queue.Queue(1)

        self._status_reporter = StatusReporter(
            self._results_queue,
            self._continue_semaphore,
            trial_name=self.trial_name,
            trial_id=self.trial_id,
            logdir=self.logdir)
        self._last_result = {}

        session.init(self._status_reporter)
        self._runner = None
        self._restore_tmpdir = None
        self.default_checkpoint_dir = None

    def _trainable_func(self):
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

        # check if error occured inside the thread runner
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
        if "__duplicate__" in result:
            new_result = self._last_result.copy()
            new_result.update(result)
            result = new_result

        self._last_result = result
        if self._status_reporter.has_new_checkpoint():
            result[SHOULD_CHECKPOINT] = True
        return result

    def execute(self, fn):
        return fn(self)

    def create_default_checkpoint_dir(self):
        self.default_checkpoint_dir = TrainableUtil.make_checkpoint_dir(
            self.logdir, index="default")
        return self.default_checkpoint_dir

    def save(self, checkpoint_path=None):
        if checkpoint_path:
            raise ValueError(
                "Checkpoint path should not be used with function API.")

        checkpoint = self._status_reporter.get_checkpoint()
        state = self.get_state()

        if not checkpoint:
            state.update(iteration=0, timesteps_total=0, episodes_total=0)
            parent_dir = self.create_default_checkpoint_dir()
        elif isinstance(checkpoint, dict):
            parent_dir = TrainableUtil.make_checkpoint_dir(
                self.logdir, index=self.training_iteration)
        else:
            parent_dir = TrainableUtil.find_checkpoint_dir(checkpoint)
        checkpoint_path = TrainableUtil.process_checkpoint(
            checkpoint, parent_dir, state)
        return checkpoint_path

    def save_to_object(self):
        checkpoint_path = self.save()
        obj = TrainableUtil.checkpoint_to_object(checkpoint_path)
        return obj

    def load_checkpoint(self, checkpoint):
        # This should be removed once Trainables are refactored.
        if "tune_checkpoint_path" in checkpoint:
            del checkpoint["tune_checkpoint_path"]
        self._status_reporter.save_checkpoint(checkpoint)

    def restore_from_object(self, obj):
        if self.default_checkpoint_dir is not None and os.exists(
                self.default_checkpoint_dir):
            shutil.rmtree(self.default_checkpoint_dir)
            logger.debug("Clearing default checkpoint: %s",
                         self.default_checkpoint_dir)

        checkpoint_dir = self.create_default_checkpoint_dir()
        checkpoint_path = TrainableUtil.create_from_pickle(obj, checkpoint_dir)
        self.restore(checkpoint_path)

    def cleanup(self):
        # If everything stayed in synch properly, this should never happen.
        if not self._results_queue.empty():
            logger.warning(
                ("Some results were added after the trial stop condition. "
                 "These results won't be logged."))

        # Check for any errors that might have been missed.
        self._report_thread_runner_error()
        session.shutdown()

    def _report_thread_runner_error(self, block=False):
        try:
            err_tb_str = self._error_queue.get(
                block=block, timeout=ERROR_FETCH_TIMEOUT)
            raise TuneError(("Trial raised an exception. Traceback:\n{}"
                             .format(err_tb_str)))
        except queue.Empty:
            pass


def detect_checkpoint_function(train_func):
    func_args = inspect.getfullargspec(train_func).args
    use_checkpoint = "checkpoint" in func_args
    return use_checkpoint


def wrap_function(train_func):
    class ImplicitFunc(FunctionRunner):
        def _trainable_func(self, config, reporter, checkpoint):
            func_args = inspect.getfullargspec(train_func).args
            if len(func_args) > 1:  # more arguments than just the config
                if "reporter" not in func_args and (
                        "checkpoint" not in func_args):
                    raise ValueError(
                        "Unknown argument found in the Trainable function. "
                        "Arguments other than the 'config' arg must be one "
                        "of ['reporter', 'checkpoint']. Found: {}".format(
                            func_args))
            use_reporter = "reporter" in func_args
            use_checkpoint = "checkpoint" in func_args
            if not use_checkpoint and not use_reporter:
                logger.warning(
                    "Function checkpointing is disabled. This may result in "
                    "unexpected behavior when using checkpointing features or "
                    "certain schedulers. To enable, set the train function "
                    "arguments to be `func(config, checkpoint)`.")
                output = train_func(config)
            elif use_checkpoint:
                output = train_func(config, checkpoint=checkpoint)
            else:
                output = train_func(config, reporter)

            # If train_func returns, we need to notify the main event loop
            # of the last result while avoiding double logging. This is done
            # with the keyword RESULT_DUPLICATE -- see tune/trial_runner.py.
            reporter(**{RESULT_DUPLICATE: True})
            return output

    return ImplicitFunc
