from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import importlib
import os
import sys
import time
import threading
import traceback

from ray.rllib.agent import Agent
from ray.tune import TuneError
from ray.tune.result import TrainingResult


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
            kwargs (TrainingResult): Latest training result status. You must
                at least define `timesteps_total`, but probably want to report
                some of the other metrics as well.
        """

        with self._lock:
            self._latest_result = self._last_result = TrainingResult(**kwargs)

    def _get_and_clear_status(self):
        if self._error:
            raise TuneError("Error running trial: " + str(self._error))
        if self._done and not self._latest_result:
            if not self._last_result:
                raise TuneError("Trial finished without reporting result!")
            return self._last_result._replace(done=True)
        with self._lock:
            res = self._latest_result
            self._latest_result = None
            return res

    def _stop(self):
        self._error = "Agent stopped"


DEFAULT_CONFIG = {
    # path of the script to run
    "script_file_path": "/path/to/file.py",

    # name of train function in the file, e.g. train(config, status_reporter)
    "script_entrypoint": "train",

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


def import_function(file_path, function_name):
    # strong assumption here that we're in a new process
    file_path = os.path.expanduser(file_path)
    sys.path.insert(0, os.path.dirname(file_path))
    if hasattr(importlib, "util"):
        # Python 3.4+
        spec = importlib.util.spec_from_file_location(
            "external_file", file_path)
        external_file = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(external_file)
    elif hasattr(importlib, "machinery"):
        # Python 3.3
        from importlib.machinery import SourceFileLoader
        external_file = SourceFileLoader(
            "external_file", file_path).load_module()
    else:
        # Python 2.x
        import imp
        external_file = imp.load_source("external_file", file_path)
    if not external_file:
        raise TuneError("Unable to import file at {}".format(file_path))
    return getattr(external_file, function_name)


class ScriptRunner(Agent):
    """Agent that runs a user script returning training results."""

    _agent_name = "script"
    _default_config = DEFAULT_CONFIG
    _allow_unknown_configs = True

    def _init(self):
        entrypoint = self._trainable_func()
        if not entrypoint:
            entrypoint = import_function(
                self.config["script_file_path"],
                self.config["script_entrypoint"])
        self._status_reporter = StatusReporter()
        scrubbed_config = self.config.copy()
        for k in self._default_config:
            del scrubbed_config[k]
        self._runner = _RunnerThread(
            entrypoint, scrubbed_config, self._status_reporter)
        self._start_time = time.time()
        self._last_reported_time = self._start_time
        self._last_reported_timestep = 0
        self._runner.start()

    # Subclasses can override this to set the trainable func
    # TODO(ekl) this isn't a very clean layering, we should refactor it
    def _trainable_func(self):
        return None

    def train(self):
        if not self._initialize_ok:
            raise ValueError(
                "Agent initialization failed, see previous errors")

        now = time.time()
        time.sleep(self.config["script_min_iter_time_s"])

        result = self._status_reporter._get_and_clear_status()
        while result is None:
            time.sleep(1)
            result = self._status_reporter._get_and_clear_status()
        if result.timesteps_total is None:
            raise TuneError("Must specify timesteps_total in result", result)

        # Include the negative loss to use as a stopping condition
        if result.mean_loss is not None:
            neg_loss = -result.mean_loss
        else:
            neg_loss = result.neg_mean_loss

        result = result._replace(
            experiment_id=self._experiment_id,
            neg_mean_loss=neg_loss,
            training_iteration=self.iteration,
            time_this_iter_s=now - self._last_reported_time,
            timesteps_this_iter=(
                result.timesteps_total - self._last_reported_timestep),
            time_total_s=now - self._start_time,
            pid=os.getpid(),
            hostname=os.uname()[1])

        if result.timesteps_total:
            self._last_reported_timestep = result.timesteps_total
        self._last_reported_time = now
        self._iteration += 1

        self._result_logger.on_result(result)

        return result

    def _stop(self):
        self._status_reporter._stop()
