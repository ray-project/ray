import inspect
import logging
import threading
import time
import traceback
from six.moves import queue

from ray.tune import track
from ray.tune import TuneError
from ray.tune.trainable import Trainable
from ray.tune.result import (FUNCTION_SAVE, RESULT_DUPLICATE, TIME_THIS_ITER_S)

logger = logging.getLogger(__name__)

# Time between FunctionRunner checks when fetching
# new results after signaling the reporter to continue.
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
                 logdir=None,
                 restore=None):
        """Initializes a StatusReporter.

        Args:
            result_queue (queue.Queue): Thread-safe message queue.
            continue_semaphore (threading.Semaphore):
            logdir (str): Trial logdir.
        """
        self._queue = result_queue
        self._last_report_time = None
        self._continue_semaphore = continue_semaphore
        self._logdir = logdir
        self._restore_from = restore

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

        # Time per iteration is recorded directly in the reporter to ensure
        # any delays in logging results aren't counted.
        report_time = time.time()
        if TIME_THIS_ITER_S not in kwargs:
            kwargs[TIME_THIS_ITER_S] = report_time - self._last_report_time
        self._last_report_time = report_time

        # Add result to a thread-safe queue.
        self._queue.put(kwargs.copy(), block=True)
        self._wait_for_notification()

    def on_result(self, result):
        self.__call__(**result)

    def start(self):
        self._last_report_time = time.time()

    def is_pending_restore(self):
        """Whether the reporter was initialized with a checkpoint."""
        return self._restore_from is not None

    def pop_checkpoint(self):
        """Returns the checkpoint the reporter was initialized with."""
        checkpoint = self._restore_from
        self._restore_from = None
        return checkpoint

    @property
    def logdir(self):
        return self._logdir

    def _wait_for_notification(self):
        """Blocks until notification from FunctionRunner received.

        This blocks until notification from the FunctionRunner that the last
        result has been received by Tune and that the function is safe to
        resume training.
        """
        self._continue_semaphore.acquire()


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
                ("Thread runner raised StopIteration. Interpreting it as a "
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

    Saves are triggered during training steps.
    """

    _name = "func"

    def train(self):
        """Overrides Trainable.train to call `save()` if necessary."""
        result = super(FunctionRunner, self).train()
        if self._last_result.get(FUNCTION_SAVE) is not None:
            result[FUNCTION_SAVE] = self.save()
        return result

    def _setup(self, config):
        # Semaphore for notifying the reporter to continue with the computation
        # and to generate the next result.
        self._continue_semaphore = threading.Semaphore(0)

        # Queue for passing results between threads.
        self._result_queue = queue.Queue(1)

        # Queue for passing errors back from the thread runner. The error queue
        # has a max size of one to prevent stacking error and force error
        # reporting to block until finished.
        self._error_queue = queue.Queue(1)

        # Last training result.
        self._last_result = {}

        # Thread in which function runs.
        self._runner_thread = None

    def _init_runner_thread(self, restore=None):
        """Initializes the runner thread and its reporter."""
        self._status_reporter = StatusReporter(
            self._result_queue, self._continue_semaphore, self.logdir, restore)

        def entrypoint():
            return self._trainable_func(self.config.copy(),
                                        self._status_reporter)

        # The runner thread is not started until the first call to _train.
        self._runner_thread = _RunnerThread(entrypoint, self._error_queue)

    def _trainable_func(self, config, reporter):
        """Subclasses can override this to set the Trainable function."""
        raise NotImplementedError

    def _train(self):
        """Implements train() for a Function API.

        Monitors queue for results.

        If the RunnerThread finishes without reporting "done",
        Tune will automatically provide a magic keyword __duplicate__
        along with a result with "done=True". The TrialRunner will handle the
        result accordingly (see tune/trial_runner.py).

        Raises:
            TuneError: This is raised if the Trainable ends up in a bad state.
        """
        if self._runner_thread is None:
            # Initialize thread here at trial start time, rather than in
            # `_setup` to avoid double-initialization during restores.
            self._init_runner_thread()

        if self._runner_thread.is_alive():
            # If started and alive, inform the reporter to continue and
            # generate the next result.
            self._continue_semaphore.release()
        else:
            # If not alive, try to start.
            self._status_reporter.start()
            try:
                self._runner_thread.start()
            except RuntimeError:
                # If this is reached, it means the thread was started and is
                # now done or has raised an exception.
                pass

        result = self._fetch_result()

        # Check if error occurred inside the thread runner.
        if result is None:
            # Only raise an error from the runner if all results are consumed.
            self._report_thread_runner_error(block=True)
            # Under normal conditions, this code should never be reached since
            # this branch should only be visited if the runner thread raised
            # an exception. If no exception was raised, it means that the
            # runner thread never reported any results which should not be
            # possible when wrapping functions with `wrap_function`.
            raise TuneError(
                "Wrapped function ran until completion without reporting "
                "results or raising an exception.")
        elif not self._error_queue.empty():
            logger.warning("Runner error waiting to be raised in main thread. "
                           "Logging all available results first.")

        # This keyword appears if the train_func using the Function API
        # finishes without "done=True". This duplicates the last result, but
        # the TrialRunner will not log this result again.
        if RESULT_DUPLICATE in result:
            new_result = self._last_result.copy()
            new_result.update(result)
            result = new_result

        self._last_result = result
        return result

    def _save(self, checkpoint_dir):
        """Returns the checkpoint last taken."""
        return self._last_result.get(FUNCTION_SAVE)

    def _restore(self, checkpoint):
        """Prepares the runner thread for restoration."""
        self._init_runner_thread(checkpoint)

    def _stop(self):
        if not self._result_queue.empty():
            # If everything stayed in sync properly, this should never happen.
            logger.warning("Some results were added after the trial stop "
                           "condition. They will not be logged.")
        # Check for any errors that might have been missed.
        self._report_thread_runner_error()

    def _fetch_result(self):
        """Blocks and fetches a result from the queue.

        Returns:
            Result, or None if the runner is dead and the queue is empty.
        """
        result = None
        while result is None and self._runner_thread.is_alive():
            # Fetch the next result.
            try:
                result = self._result_queue.get(
                    block=True, timeout=RESULT_FETCH_TIMEOUT)
            except queue.Empty:
                pass

        # If no result received, then the runner must no longer be alive.
        if result is None:
            # Try one last time to fetch results, in case results were
            # reported in between the time of the last check and the
            # termination of the thread runner.
            try:
                result = self._result_queue.get(block=False)
            except queue.Empty:
                pass
        return result

    def _report_thread_runner_error(self, block=False):
        try:
            err_tb_str = self._error_queue.get(
                block=block, timeout=ERROR_FETCH_TIMEOUT)
            raise TuneError(("Trial raised an exception. Traceback:\n{}"
                             .format(err_tb_str)))
        except queue.Empty:
            pass

    def _cleanup_object_restore(self):
        """Do not cleanup checkpoint dir at the end of `restore_from_object`.

        This prevents the temporary checkpoint dir from being deleted before
        the runner thread has a chance to restore from it.
        """
        return False


def wrap_function(train_func):

    use_track = False
    try:
        func_args = inspect.getfullargspec(train_func).args
        use_track = ("reporter" not in func_args and len(func_args) == 1)
        if use_track:
            logger.info("tune.track signature detected.")
    except Exception:
        logger.info(
            "Function inspection failed - assuming reporter signature.")

    class WrappedFunc(FunctionRunner):
        def _trainable_func(self, config, reporter):
            output = train_func(config, reporter)
            # If train_func returns, we need to notify the main event loop
            # of the last result while avoiding double logging. This is done
            # with the keyword RESULT_DUPLICATE -- see tune/trial_runner.py.
            reporter(**{RESULT_DUPLICATE: True})
            return output

    class WrappedTrackFunc(FunctionRunner):
        def _trainable_func(self, config, reporter):
            track.init(_tune_reporter=reporter)
            output = train_func(config)
            reporter(**{RESULT_DUPLICATE: True})
            track.shutdown()
            return output

    return WrappedTrackFunc if use_track else WrappedFunc
