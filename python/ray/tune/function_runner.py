from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time
import threading

from ray.tune import TuneError
from ray.tune.trainable import Trainable
from ray.tune.result import TIMESTEPS_TOTAL

logger = logging.getLogger(__name__)


class StatusReporter(object):
    """Object passed into your function that you can report status through.

    Example:
        >>> def trainable_function(config, reporter):
        >>>     assert isinstance(reporter, StatusReporter)
        >>>     reporter(timesteps_total=1)
    """

    def __init__(self):
        self._latest_result = None
        self._last_result = None
        self._lock = threading.Lock()
        self._error = None
        self._done = False

    def __call__(self, **kwargs):
        """Report updated training status.

        Pass in `done=True` when the training job is completed.

        Args:
            kwargs: Latest training result status.

        Example:
            >>> reporter(mean_accuracy=1, training_iteration=4)
            >>> reporter(mean_accuracy=1, training_iteration=4, done=True)
        """

        with self._lock:
            self._latest_result = self._last_result = kwargs.copy()

    def _get_and_clear_status(self):
        if self._error:
            raise TuneError("Error running trial: " + str(self._error))
        if self._done and not self._latest_result:
            if not self._last_result:
                raise TuneError("Trial finished without reporting result!")
            logger.warning("Trial detected as completed; re-reporting "
                           "last result. To avoid this, include done=True "
                           "upon the last reporter call.")
            self._last_result.update(done=True)
            return self._last_result
        with self._lock:
            res = self._latest_result
            self._latest_result = None
            return res

    def _stop(self):
        self._error = "Agent stopped"


DEFAULT_CONFIG = {
    # batch results to at least this granularity
    "script_min_iter_time_s": 1,
}


class _RunnerThread(threading.Thread):
    """Supervisor thread that runs your script."""

    def __init__(self, entrypoint, config, status_reporter):
        self._entrypoint = entrypoint
        self._entrypoint_args = [config, status_reporter]
        self._status_reporter = status_reporter
        threading.Thread.__init__(self)
        self.daemon = True

    def run(self):
        try:
            self._entrypoint(*self._entrypoint_args)
        except Exception as e:
            self._status_reporter._error = e
            logger.exception("Runner Thread raised error.")
            raise e
        finally:
            self._status_reporter._done = True


class FunctionRunner(Trainable):
    """Trainable that runs a user function returning training results.

    This mode of execution does not support checkpoint/restore."""

    _name = "func"
    _default_config = DEFAULT_CONFIG

    def _setup(self, config):
        entrypoint = self._trainable_func()
        self._status_reporter = StatusReporter()
        scrubbed_config = config.copy()
        for k in self._default_config:
            if k in scrubbed_config:
                del scrubbed_config[k]
        self._runner = _RunnerThread(entrypoint, scrubbed_config,
                                     self._status_reporter)
        self._start_time = time.time()
        self._last_reported_timestep = 0
        self._runner.start()

    def _trainable_func(self):
        """Subclasses can override this to set the trainable func."""

        raise NotImplementedError

    def _train(self):
        time.sleep(
            self.config.get("script_min_iter_time_s",
                            self._default_config["script_min_iter_time_s"]))
        result = self._status_reporter._get_and_clear_status()
        while result is None:
            time.sleep(1)
            result = self._status_reporter._get_and_clear_status()

        curr_ts_total = result.get(TIMESTEPS_TOTAL)
        if curr_ts_total is not None:
            result.update(
                timesteps_this_iter=(
                    curr_ts_total - self._last_reported_timestep))
            self._last_reported_timestep = curr_ts_total

        return result

    def _stop(self):
        self._status_reporter._stop()
