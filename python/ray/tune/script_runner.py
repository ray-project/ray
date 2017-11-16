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


class StatusReporter(object):
    """Object passed into your main() that you can report status through."""

    def __init__(self):
        self._latest_result = None
        self._lock = threading.Lock()
        self._error = None

    def report(self, result):
        """Report updated training status.

        Args:
            result (TrainingResult): Latest training result status. You must
                at least define `timesteps_total`, but probably want to report
                some of the other metrics as well.
        """

        with self._lock:
            self._latest_result = result

    def set_error(self, error):
        """Report an error.

        Args:
            error (obj): Error object or string.
        """

        self._error = error

    def _get_and_clear_status(self):
        if self._error:
            raise Exception("Error running script: " + str(self._error))
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
    "script_min_iter_time_s": 5,
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
            self._status_reporter.set_error(e)
            print("Runner thread raised: {}".format(traceback.format_exc()))
            raise e


class ScriptRunner(Agent):
    """Agent that runs a user script returning training results."""

    _agent_name = "script"
    _default_config = DEFAULT_CONFIG
    _allow_unknown_configs = True

    def _init(self):
        # strong assumption here that we're in a new process
        file_path = os.path.expanduser(self.config["script_file_path"])
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
            raise Exception(
                "Unable to import file at {}".format(
                    self.config["script_file_path"]))
        entrypoint = getattr(external_file, self.config["script_entrypoint"])
        self._status_reporter = StatusReporter()
        self._runner = _RunnerThread(
            entrypoint, self.config, self._status_reporter)
        self._start_time = time.time()
        self._last_reported_time = self._start_time
        self._last_reported_timestep = 0
        self._runner.start()

    def train(self):
        if not self._initialize_ok:
            raise ValueError(
                "Agent initialization failed, see previous errors")

        poll_start = time.time()
        result = self._status_reporter._get_and_clear_status()
        while result is None or \
                time.time() - poll_start < \
                self.config["script_min_iter_time_s"]:
            time.sleep(1)
            result = self._status_reporter._get_and_clear_status()

        now = time.time()

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

    def stop(self):
        self._status_reporter._stop()
        Agent.stop(self)
