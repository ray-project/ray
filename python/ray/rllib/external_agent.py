from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import importlib.util
import os
import sys
import time
import threading

from ray.rllib.common import Agent, TrainingResult


DEFAULT_CONFIG = {
    "file_path": "/path/to/file.py",
    "entrypoint": "rllib_main",
    "min_iter_time_s": 10,
}


class StatusReporter(object):
    def __init__(self):
        self.latest_result = None
        self.lock = threading.Lock()

    def report(self, result):
        with self.lock:
            self.latest_result = result

    def get_and_clear_status(self):
        with self.lock:
            res = self.latest_result
            self.latest_result = None
            return res


class RunnerThread(threading.Thread):
    def __init__(self, entrypoint, entrypoint_args):
        self.entrypoint = entrypoint
        self.entrypoint_args = entrypoint_args
        threading.Thread.__init__(self)

    def run(self):
        self.entrypoint(*self.entrypoint_args)
        

class ExternalAgent(Agent):
    _agent_name = "CUSTOM"
    _default_config = DEFAULT_CONFIG

    def _init(self):
        # strong assumption here that we're in a new process
        sys.path.insert(0, os.path.dirname(os.path.expanduser(
            self.config["file_path"])))
        spec = importlib.util.spec_from_file_location(
            "external_file", self.config["file_path"])
        foo = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(foo)
        entrypoint = getattr(foo, self.config["entrypoint"])
        self.status_reporter = StatusReporter()
        self.runner = RunnerThread(
            entrypoint, [self.config, self.status_reporter])
        self.start_time = time.time()
        self.last_reported_time = self.start_time
        self.last_reported_timestep = 0
        self.runner.start()

    def train(self):
        poll_start = time.time()
        result = self.status_reporter.get_and_clear_status()
        while result is None or \
                time.time() - poll_start < self.config["_min_iter_time_s"]:
            time.sleep(1)
            result = self.status_reporter.get_and_clear_status()

        now = time.time()

        result = result._replace(
            experiment_id=self.experiment_id,
            training_iteration=self.iteration,
            time_this_iter_s=now - self.last_reported_time,
            timesteps_this_iter=(
                result.timesteps_total - self.last_reported_timestep),
            time_total_s=now - self.start_time)

        self.last_reported_timestep = result.timesteps_total
        self.last_reported_time = now
        self.iteration += 1

        for field in result:
            assert field is not None, result

        self._log_result(result)

        return result
