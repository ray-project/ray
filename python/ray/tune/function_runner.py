from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time
import inspect
import threading
import traceback
from six.moves import queue

from ray.tune import track
from ray.tune import TuneError
from ray.tune.trainable import Trainable
from ray.tune.result import TIME_THIS_ITER_S, RESULT_DUPLICATE, FUNCTION_CHECKPOINT_PATH

logger = logging.getLogger(__name__)

# Time between FunctionRunner checks when fetching
# new events after signaling the reporter to continue
EVENT_FETCH_TIMEOUT = 0.2

ERROR_REPORT_TIMEOUT = 10
ERROR_FETCH_TIMEOUT = 1


class Event(object):

    RESULT = "result"
    CHECKPOINT = "checkpoint"

    def __init__(self, event_type, value):
        self.type = event_type
        self.value = value

    @property
    def is_result(self):
        return self.type == self.RESULT

    @property
    def is_checkpoint(self):
        return self.type == self.CHECKPOINT


class StatusReporter(object):
    """Object passed into your function that you can report status through.

    Example:
        >>> def trainable_function(config, reporter):
        >>>     assert isinstance(reporter, StatusReporter)
        >>>     reporter(timesteps_this_iter=1)
    """

    def __init__(self,
                 event_queue,
                 continue_semaphore,
                 logdir=None,
                 checkpoint_to_restore=None):
        self._queue = event_queue
        self._last_report_time = None
        self._continue_semaphore = continue_semaphore
        self._logdir = logdir
        self.checkpoint_to_restore = checkpoint_to_restore

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
        # any delays in logging results aren't counted
        report_time = time.time()
        if TIME_THIS_ITER_S not in kwargs:
            kwargs[TIME_THIS_ITER_S] = report_time - self._last_report_time
        self._last_report_time = report_time

        # Add results to a thread-safe queue
        event = Event(Event.RESULT, kwargs.copy())
        self._queue.put(event, block=True)
        self._wait_for_notification()

    def save(self, checkpoint):
        # Add checkpoint to a thread-safe queue
        event = Event(Event.CHECKPOINT, checkpoint)
        self._queue.put(event, block=True)
        self._wait_for_notification()

    def start(self):
        self._last_report_time = time.time()

    @property
    def logdir(self):
        return self._logdir

    def _wait_for_notification(self):
        """Blocks until notification from FunctionRunner received.

        This blocks until notification from the FunctionRunner that the last
        event has been received by Tune and that the function is safe to
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
    """Trainable that runs a user function reporting results."""

    _name = "func"

    def _setup(self, config):
        # Semaphore for notifying the reporter to continue with the computation
        # and to generate the next event.
        self._continue_semaphore = threading.Semaphore(0)
        # Queue for passing events between threads
        self._event_queue = queue.Queue(1)
        # Queue for passing errors back from the thread runner. The error queue
        # has a max size of one to prevent stacking error and force error
        # reporting to block until finished.
        self._error_queue = queue.Queue(1)
        self._status_reporter = StatusReporter(self._event_queue,
                                               self._continue_semaphore,
                                               self.logdir)
        self._last_result = {}
        self._last_checkpoint = None
        config = config.copy()

        def entrypoint():
            return self._trainable_func(config, self._status_reporter)
        # The runner thread is not started until the first call to _train
        self._runner = _RunnerThread(entrypoint, self._error_queue)

    def _trainable_func(self, config, reporter):
        """Subclasses can override this to set the trainable func."""
        raise NotImplementedError

    def _train(self):
        """Implements train() for a Function API.

        Monitors for result and checkpoint events. Only returns after a
        result event.

        If the RunnerThread finishes without reporting "done",
        Tune will automatically provide a magic keyword __duplicate__
        along with a result with "done=True". The TrialRunner will handle the
        result accordingly (see tune/trial_runner.py).

        Raises:
            TuneError: This is raised if the Trainable ends up in a bad state.
        """
        result = None
        checkpoint_path = None
        for event_index in range(2):

            if self._runner.is_alive():
                # If started and alive, inform the reporter to continue and
                # generate the next event.
                self._continue_semaphore.release()
            else:
                # If not alive, try to start
                self._status_reporter.start()
                try:
                    self._runner.start()
                except RuntimeError:
                    # If this is reached, it means the thread was started and
                    # is now done or has raised an exception.
                    pass

            event = None
            while event is None and self._runner.is_alive():
                # Fetch the next event
                try:
                    event = self._event_queue.get(
                        block=True, timeout=EVENT_FETCH_TIMEOUT)
                except queue.Empty:
                    pass

            # If no events were found, then the runner must no longer be alive.
            if event is None:
                # Try one last time to fetch events in case events were
                # reported in between the time of the last check and the
                # termination of the thread runner.
                try:
                    event = self._event_queue.get(block=False)
                except queue.Empty:
                    pass

            # Check if error occurred inside the thread runner
            if event is None:
                # Only raise an error from the runner if all events consumed.
                self._report_thread_runner_error(block=True)
                # Under normal conditions, this code should never be reached
                # since this branch should only be visited if the runner thread
                # raised an exception. If no exception were raised, it means
                # that the runner thread never reported any results or
                # checkpoints which should not be possible when wrapping
                # functions with `wrap_function`.
                raise TuneError(
                    "Wrapped function ran until completion without reporting "
                    "results, checkpoints or raising an exception.")
            else:
                if not self._error_queue.empty():
                    logger.warning(
                        "Runner error waiting to be raised in main thread. "
                        "Logging all available results first.")

            if event.is_checkpoint:
                if event_index == 0:
                    # First event consumed in this step is a checkpoint.
                    self._last_checkpoint = event.value
                    checkpoint_path = self.save()
                else:
                    # If the 2nd event is a checkpoint, the 1st must also be a
                    # checkpoint, since if it wasn't we would've broken out of
                    # the loop.
                    raise TuneError(
                        "Checkpoint taken twice in a single iteration. Only "
                        "a single call to tune.track.checkpoint is allowed "
                        "for every reporter or log call.")
            else:
                result = event.value
                break

        if checkpoint_path:
            result[FUNCTION_CHECKPOINT_PATH] = checkpoint_path
        # This keyword appears if the train_func using the Function API
        # finishes without "done=True". This duplicates the last
        # result, but the TrialRunner will not log this result again.
        if "__duplicate__" in result:
            new_result = self._last_result.copy()
            new_result.update(result)
            result = new_result
        self._last_result = result
        return result

    def _save(self, checkpoint_dir):
        # write any FunctionRunner metadata necessary here? TODO

        # Since the checkpoint has already been taken by the user we simply
        # return the value set prior to the local save() call. The assumption
        # here is that the checkpoint was taken in the correct directory, or
        # is a dictionary.
        return self._last_checkpoint

    def _restore(self, checkpoint):
        # restore any FunctionRunner metadata here? TODO
        self._status_reporter.checkpoint_to_restore = checkpoint

    def _stop(self):
        # If everything stayed in sync properly, this should never happen.
        if not self._event_queue.empty():
            if self._event_queue.get().event_type == Event.RESULT:
                event_type = "results"
            else:
                event_type = "checkpoints"
            logger.warning(
                "Some %s were added after the trial stop condition. "
                "These %s won't be logged.", event_type, event_type)
        # Check for any errors that might have been missed.
        self._report_thread_runner_error()

    def _report_thread_runner_error(self, block=False):
        try:
            err_tb_str = self._error_queue.get(
                block=block, timeout=ERROR_FETCH_TIMEOUT)
            raise TuneError(("Trial raised an exception. Traceback:\n{}"
                             .format(err_tb_str)))
        except queue.Empty:
            pass


def wrap_function(train_func):

    use_track = False
    try:
        func_args = inspect.getargspec(train_func).args
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
