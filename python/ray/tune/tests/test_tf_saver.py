from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import unittest

import ray
from ray.tune import sample_from
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.examples.tune_mnist_ray_hyperband import TrainMNIST


class FrequentPausesScheduler(FIFOScheduler):
    def on_trial_result(self, trial_runner, trial, result):
        if result["training_iteration"] % 3 == 0:
            return TrialScheduler.PAUSE
        return TrialScheduler.CONTINUE


class TFSaverTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=1)

    def tearDown(self):
        ray.shutdown()

    def test_saving(self):
        mnist_spec = {
            "stop": {
                "mean_accuracy": 0.99,
                "time_total_s": 600,
                "training_iteration": 10,
            },
            "config": {
                "learning_rate": sample_from(
                    lambda spec: 10**np.random.uniform(-5, -3)),
                "activation": "relu",
            },
            "num_samples": 1,
        }

        tune.run(
            TrainMNIST,
            name="mnist_hyperband_test",
            scheduler=FrequentPausesScheduler(),
            **mnist_spec)


if __name__ == "__main__":
    unittest.main(verbosity=2)
