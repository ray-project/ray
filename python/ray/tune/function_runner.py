from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
import threading
import traceback

from ray.tune import TuneError
from ray.tune.trainable import Trainable
from ray.tune.result import TIMESTEPS_TOTAL
from ray.tune.util import _serve_get_pin_requests


class StatusReporter(object):
    """Object passed into your main() that you can report status through."""

    def __init__(self):
        self._latest_result = None
        self._last_result = None
        self._lock = threading.Lock()
        self._error = None
        self._done = False

    def __call__(self, **kwargs):
        """Report updated training status.

        Args:
            kwargs: Latest training result status.
        """

        with self._lock:
            self._latest_result = self._last_result = kwargs.copy()

    def _get_and_clear_status(self):
        if self._error:
            raise TuneError("Error running trial: " + str(self._error))
        if self._done and not self._latest_result:
            if not self._last_result:
                raise TuneError("Trial finished without reporting result!")
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
            print("Runner thread raised: {}".format(traceback.format_exc()))
            raise e
        finally:
            self._status_reporter._done = True


class FunctionRunner(Trainable):
    """Trainable that runs a user function returning training results.

    This mode of execution does not support checkpoint/restore."""

    _name = "func"
    _default_config = DEFAULT_CONFIG

    def _setup(self):
        entrypoint = self._trainable_func()
        self._status_reporter = StatusReporter()
        scrubbed_config = self.config.copy()
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
            _serve_get_pin_requests()
            time.sleep(1)
            result = self._status_reporter._get_and_clear_status()

        curr_ts_total = result.get(TIMESTEPS_TOTAL,
                                   self._last_reported_timestep)
        result.update(
            timesteps_this_iter=(curr_ts_total - self._last_reported_timestep))
        self._last_reported_timestep = curr_ts_total

        return result

    def _stop(self):
        self._status_reporter._stop()
