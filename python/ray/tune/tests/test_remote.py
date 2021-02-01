import os
import unittest

import ray
from ray.tune import register_trainable, run_experiments, run
from ray.tune.result import TIMESTEPS_TOTAL
from ray.tune.experiment import Experiment
from ray.tune.trial import Trial


class RemoteTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testRemoteRunExperiments(self):
        def train(config, reporter):
            for i in range(100):
                reporter(timesteps_total=i)

        register_trainable("f1", train)
        exp1 = Experiment(**{
            "name": "foo",
            "run": "f1",
        })
        [trial] = run_experiments(exp1, _remote=True)
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 99)

    def testRemoteRun(self):
        def train(config, reporter):
            for i in range(100):
                reporter(timesteps_total=i)

        analysis = run(train, _remote=True)
        [trial] = analysis.trials
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 99)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
