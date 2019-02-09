from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import sys
import time
import threading
from six.moves import queue

from ray.tune import TuneError
from ray.tune.trainable import Trainable
from ray.tune.result import TIMESTEPS_TOTAL, TIME_THIS_ITER_S

logger = logging.getLogger(__name__)

# how many secondes between check does the FunctionRunner wait when fetching
# new results after signalling the reporter to continue
RESULT_FETCH_TIMEOUT = 0.2

ERROR_REPORT_TIMEOUT = 10
MAX_THREAD_STOP_WAIT_TIME = 5


class StatusReporter(object):
    """Object passed into your function that you can report status through.

    Example:
        >>> def trainable_function(config, reporter):
        >>>     assert isinstance(reporter, StatusReporter)
        >>>     reporter(timesteps_total=1)
    """

    def __init__(self, result_queue, stop_event, continue_semaphore):
        self._queue = result_queue
        self._last_report_time = None
        self._stop_event = stop_event
        self._continue_semaphore = continue_semaphore

    def __call__(self, **kwargs):
        """Report updated training status.

        Pass in `done=True` when the training job is completed.

        Args:
            kwargs: Latest training result status.

        Example:
            >>> reporter(mean_accuracy=1, training_iteration=4)
            >>> reporter(mean_accuracy=1, training_iteration=4, done=True)
        """

        assert self._last_report_time is not None, (
                "StatusReporter._start() must be called before the first "
                "report __call__ is made to ensure correct runtime metrics."
                )

        # time per iteration is recorded directly in the reporter to ensure
        # any delays in logging results aren't counted
        report_time = time.time()
        if TIME_THIS_ITER_S not in kwargs:
            kwargs[TIME_THIS_ITER_S] = self._last_report_time - report_time
        self._last_report_time = report_time

        # add results to a thread-safe queue
        self._queue.put(kwargs, block=True)

        # don't wait if the thread should stop
        if self.should_stop():
            raise StopIteration()

        # this ensures the FunctionRunner's _train method was called and that
        # the next results are requested
        self._continue_semaphore.acquire()

        # check if the stop signal was sent during the semaphore wait
        if self.should_stop():
            raise StopIteration()

    def _start(self):
        self._last_report_time = time.time()

    def should_stop(self):
        return self._stop_event.is_set()


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
            logger.debug((
                    "Thread runner raised StopIteration. Interperting it as a "
                    "signal to terminate the thread without error."))
            pass
        except Exception as e:
            logger.exception("Runner Thread raised error.")
            try:
                # report the error but avoid indefinite blocking which would
                # prevent the exception from being propagated in the unlikely
                # case that something went terribly wrong
                self._error_queue.put(sys.exc_info(),
                                      block=True,
                                      timeout=ERROR_REPORT_TIMEOUT)
            except queue.Full:
                logger.critical((
                        "Runner Thread was unable to report error to main "
                        "function runner thread. This means a previous error "
                        "was not processed. This should never happen."))
            raise e


class FunctionRunner(Trainable):
    """Trainable that runs a user function reporting results.

    This mode of execution does not support checkpoint/restore."""

    _name = "func"

    def _setup(self, config):
        # a thread-safe event to notify the status reporter to stop
        self._stop_event = threading.Event()

        # Semaphore for notifying the reporter to continue with the computation
        # and to generate the next result.
        self._continue_semaphore = threading.Semaphore(0)

        # Queue for passing results between threads
        self._results_queue = queue.Queue(1)

        # Queue for passing errors back from the thread runner. The error queue
        # has a max size of one to prevent stacking error and force error
        # reporting to block until finished.
        self._error_queue = queue.Queue(1)

        self._status_reporter = StatusReporter(self._results_queue,
                                               self._stop_event,
                                               self._continue_semaphore)

        def entrypoint():
            return self._trainable_func(config, self._status_reporter)

        config = config.copy()

        # the runner thread is not started until the first call to _train
        self._runner = _RunnerThread(entrypoint, self._error_queue)
        self._started = False

    def _trainable_func(self):
        """Subclasses can override this to set the trainable func."""

        raise NotImplementedError

    def _train(self):
        if not self._started:
            # if not started, start
            self._status_reporter._start()
            self._runner.start()
            self._started = True
        else:
            # if started and alive, inform the reporter to continue and
            # generate the next result
            self._continue_semaphore.release()

        result = None
        while result is None and self._runner.is_alive():
            # fetch the next produced result
            try:
                result = self._results_queue.get(block=True,
                                                 timeout=RESULT_FETCH_TIMEOUT)
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
            self._report_thread_runner_error()

            # If no results were found at this point and no errors were
            # reported, the thread runner should be stopped and we can report
            # done.
            result = {TIME_THIS_ITER_S: 0,
                      "done": True}
        else:
            if not self._error_queue.empty():
                logger.warning((
                        "Runner error waiting to be raised in main thread. "
                        "Logging all available results first."
                        ))

        return result

    def _stop(self):
        # notify the status reporter to stop if still running
        self._stop_event.set()

        # Allow the reporter to proceed, this should cause a StopIteration
        # exception if the reporter was waiting for this signal.
        self._continue_semaphore.release()

        # Give some time for the runner to stop.
        self._runner.join(timeout=MAX_THREAD_STOP_WAIT_TIME)
        if self._runner.is_alive():
            logger.warning("Thread runner still running after stop event.")

        # If everything stayed in synch properly, this should never happen.
        if not self._status_reporter.empty():
            logger.warning((
                    "Some results were added after the trial stop condition. "
                    "These results won't be logged."))

        # Check for any errors that might have been missed.
        self._report_thread_runner_error()

    def _report_thread_runner_error(self):
        try:
            err_type, err_value, err_tb = self._error_queue.get(block=False)
            raise TuneError("Trial raised a {} error with value: {}\nWith traceback:\n{}".format(err_type, err_value, err_tb))
        except queue.Empty:
            pass
