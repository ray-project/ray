#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict
import glob
import os
import yaml

import ray
from ray import tune


CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))


class Regressions():
    _file = os.join(CONFIG_DIR, "cartpole-ppo.yaml")
    _results = {}

    def setup_cache(self):
        # load data from a file
        with open(self._file) as f:
            experiments = yaml.load(f)
        ray.init()
        trials = tune.run_experiments(experiments)
        assert len(trials) == 1

        self._results[self._file] = trials[0].last_result

    def teardown(self, *args):
        ray.worker.cleanup()

    def track_time(self):
        return self._results[self._file].time_total_s

    def track_reward(self):
        return self._results[self._file].episode_reward_mean

    def track_iterations(self):
        return self._results[self._file].training_iteration
