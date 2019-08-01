from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import shutil
import tempfile
import random
import os
import pandas as pd

import ray
from ray.tune import run, sample_from, Analysis
from ray.tune.examples.async_hyperband_example import MyTrainableClass


class ExperimentAnalysisSuite(unittest.TestCase):
    def setUp(self):
        ray.init(local_mode=True)
        self.test_dir = tempfile.mkdtemp()
        self.test_name = "analysis_exp"
        self.num_samples = 10
        self.metric = "episode_reward_mean"
        self.test_path = os.path.join(self.test_dir, self.test_name)
        self.run_test_exp()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)
        ray.shutdown()

    def run_test_exp(self):
        self.ea = run(
            MyTrainableClass,
            global_checkpoint_period=0,
            name=self.test_name,
            local_dir=self.test_dir,
            stop={"training_iteration": 1},
            num_samples=self.num_samples,
            config={
                "width": sample_from(
                    lambda spec: 10 + int(90 * random.random())),
                "height": sample_from(lambda spec: int(100 * random.random())),
            })

    def testDataframe(self):
        df = self.ea.dataframe()

        self.assertTrue(isinstance(df, pd.DataFrame))
        self.assertEquals(df.shape[0], self.num_samples)

    def testTrialDataframe(self):
        checkpoints = self.ea._checkpoints
        idx = random.randint(0, len(checkpoints) - 1)
        trial_df = self.ea.trial_dataframes[checkpoints[idx]["logdir"]]

        self.assertTrue(isinstance(trial_df, pd.DataFrame))
        self.assertEqual(trial_df.shape[0], 1)

    def testBestConfig(self):
        best_config = self.ea.get_best_config(self.metric)

        self.assertTrue(isinstance(best_config, dict))
        self.assertTrue("width" in best_config)
        self.assertTrue("height" in best_config)

    def testBestLogdir(self):
        logdir = self.ea.get_best_logdir(self.metric)
        self.assertTrue(logdir.startswith(self.test_path))
        logdir2 = self.ea.get_best_logdir(self.metric, mode="min")
        self.assertTrue(logdir2.startswith(self.test_path))
        self.assertNotEquals(logdir, logdir2)

    def testAllDataframes(self):
        dataframes = self.ea.trial_dataframes
        self.assertTrue(len(dataframes) == self.num_samples)

        self.assertTrue(isinstance(dataframes, dict))
        for df in dataframes.values():
            self.assertEqual(df.training_iteration.max(), 1)

    def testIgnoreOtherExperiment(self):
        analysis = run(
            MyTrainableClass,
            global_checkpoint_period=0,
            name="test_example",
            local_dir=self.test_dir,
            return_trials=False,
            stop={"training_iteration": 1},
            num_samples=1,
            config={
                "width": sample_from(
                    lambda spec: 10 + int(90 * random.random())),
                "height": sample_from(lambda spec: int(100 * random.random())),
            })
        df = analysis.dataframe()
        self.assertEquals(df.shape[0], 1)


class AnalysisSuite(unittest.TestCase):
    def setUp(self):
        ray.init(local_mode=True)
        self.test_dir = tempfile.mkdtemp()
        self.num_samples = 10
        self.metric = "episode_reward_mean"
        self.run_test_exp(test_name="analysis_exp1")
        self.run_test_exp(test_name="analysis_exp2")

    def run_test_exp(self, test_name=None):
        run(MyTrainableClass,
            global_checkpoint_period=0,
            name=test_name,
            local_dir=self.test_dir,
            return_trials=False,
            stop={"training_iteration": 1},
            num_samples=self.num_samples,
            config={
                "width": sample_from(
                    lambda spec: 10 + int(90 * random.random())),
                "height": sample_from(lambda spec: int(100 * random.random())),
            })

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)
        ray.shutdown()

    def testDataframe(self):
        analysis = Analysis(self.test_dir)
        df = analysis.dataframe()
        self.assertTrue(isinstance(df, pd.DataFrame))
        self.assertEquals(df.shape[0], self.num_samples * 2)

    def testBestLogdir(self):
        analysis = Analysis(self.test_dir)
        logdir = analysis.get_best_logdir(self.metric)
        self.assertTrue(logdir.startswith(self.test_dir))
        logdir2 = analysis.get_best_logdir(self.metric, mode="min")
        self.assertTrue(logdir2.startswith(self.test_dir))
        self.assertNotEquals(logdir, logdir2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
