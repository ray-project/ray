#!/usr/bin/env python
"""
This class runs the regression YAMLs in the ASV format.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict
import numpy as np
import os
import yaml

import ray
from ray import tune


CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))


def _evaulate_config(filename):
    with open(os.path.join(CONFIG_DIR, filename)) as f:
        experiments = yaml.load(f)
    ray.init()
    trials = tune.run_experiments(experiments)
    results = defaultdict(list)
    for t in trials:
        results["time_total_s"] += [t.last_result.time_total_s]
        results["episode_reward_mean"] += [t.last_result.episode_reward_mean]
        results["training_iteration"] += [t.last_result.training_iteration]

    return {k: np.median(v) for k, v in results.items()}


class Regression():
    def setup_cache(self):
        # We need to implement this in separate classes
        # below so that ASV will register the setup/class
        # as a separate test.
        raise NotImplementedError

    def teardown(self, *args):
        ray.worker.cleanup()

    def track_time(self, result):
        return result["time_total_s"]

    def track_reward(self, result):
        return result["episode_reward_mean"]

    def track_iterations(self, result):
        return result["training_iteration"]


class TestCartPolePPO(Regression):
    _file = "cartpole-ppo.yaml"

    def setup_cache(self):
        return _evaulate_config(self._file)


class TestCartPolePG(Regression):
    _file = "cartpole-pg.yaml"

    def setup_cache(self):
        return _evaulate_config(self._file)


class TestPendulumDDPG(Regression):
    _file = "pendulum-ddpg.yaml"

    def setup_cache(self):
        return _evaulate_config(self._file)


class TestCartPoleES(Regression):
    _file = "cartpole-es.yaml"

    def setup_cache(self):
        return _evaulate_config(self._file)


class TestCartPoleDQN(Regression):
    _file = "cartpole-dqn.yaml"

    def setup_cache(self):
        return _evaulate_config(self._file)


class TestCartPoleA3C(Regression):
    _file = "cartpole-a3c.yaml"

    def setup_cache(self):
        return _evaulate_config(self._file)


class TestCartPoleA3CPyTorch(Regression):
    _file = "cartpole-a3c-pytorch.yaml"

    def setup_cache(self):
        return _evaulate_config(self._file)
