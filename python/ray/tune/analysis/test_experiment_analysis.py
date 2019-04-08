from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import subprocess
import shutil
import random

from experiment_analysis import *
import ray
from ray.tune import run, sample_from
from ray.tune.examples.async_hyperband_example import MyTrainableClass
from ray.tune.schedulers import AsyncHyperBandScheduler

test_dir = "~/analysis_test"

def run_test_exp():
    ray.init()

    ahb = AsyncHyperBandScheduler(
        time_attr="training_iteration",
        reward_attr="episode_reward_mean",
        grace_period=5,
        max_t=100)

    run(MyTrainableClass,
        name="analysis_exp",
        scheduler=ahb,
        local_dir=test_dir,
        **{
            "stop": {
                "training_iteration": 100
            },
            "num_samples": 20,
            "resources_per_trial": {
                "cpu": 1,
                "gpu": 0
            },
            "config": {
                "width": sample_from(
                    lambda spec: 10 + int(90 * random.random())),
                "height": sample_from(lambda spec: int(100 * random.random())),
            },
        })

def remove_test_exp():
    shutil.rmtree(os.path.expanduser(test_dir))

class ExperimentAnalysisSuite(unittest.TestCase):
    def setup(self):
        run_test_exp()

    def tearDown(self):
        remove_test_exp()

    def testDataframe(self):
        pass

    def testTrialDataframe(self):
        pass

    def testBestTrainable(self):
        pass

    def testBestConfig(self):
        pass

    def testBestTrial(self):
        pass

    def testCheckpoints(self):
        pass

    def testStats(self):
        pass

    def testRunnerData(self):
        pass

if __name__ == "__main__":
    remove_test_exp()

"""nevergrad_analysis = ExperimentAnalysis("~adizim/ray_results/nevergrad")
#print(nevergrad_analysis.checkpoints()[0])
print(nevergrad_analysis.dataframe())
print(nevergrad_analysis.trial_dataframe("f5234953"))
#print(nevergrad_analysis.get_best_trainable("neg_mean_loss"))
#print(nevergrad_analysis.trials())
#print(nevergrad_analysis.stats())"""