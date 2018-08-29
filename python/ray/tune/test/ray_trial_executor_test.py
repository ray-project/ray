# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import ray
from ray.rllib import _register_all
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.suggest import BasicVariantGenerator
from ray.tune.trial import Trial, Checkpoint


class RayTrialExecutorTest(unittest.TestCase):
    def setUp(self):
        self.trial_executor = RayTrialExecutor(queue_trials=False)
        ray.init()

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def _get_trials(self):
        trials = self.generate_trials({
            "run": "PPO",
            "config": {
                "bar": {
                    "grid_search": [True, False]
                },
                "foo": {
                    "grid_search": [1, 2, 3]
                },
            },
        }, "grid_search")
        return list(trials)

    def testStartStop(self):
        trial = Trial("__fake")
        self.trial_executor.start_trial(trial)
        running = self.trial_executor.get_running_trials()
        self.assertEqual(1, len(running))
        self.trial_executor.stop_trial(trial)

    def testSaveRestore(self):
        trial = Trial("__fake")
        self.trial_executor.start_trial(trial)
        self.assertEqual(Trial.RUNNING, trial.status)
        self.trial_executor.save(trial, Checkpoint.DISK)
        self.trial_executor.restore(trial)
        self.trial_executor.stop_trial(trial)
        self.assertEqual(Trial.TERMINATED, trial.status)

    def generate_trials(self, spec, name):
        suggester = BasicVariantGenerator({name: spec})
        return suggester.next_trials()


if __name__ == "__main__":
    unittest.main(verbosity=2)
