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
from ray.tune import run, sample_from
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
            return_trials=False,
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
        cs = self.ea._checkpoints
        idx = random.randint(0, len(cs) - 1)
        trial_df = self.ea.trial_dataframe(
            cs[idx]["trial_id"])  # random trial df

        self.assertTrue(isinstance(trial_df, pd.DataFrame))
        self.assertEqual(trial_df.shape[0], 1)

    def testBestTrainable(self):
        best_trainable = self.ea.get_best_trainable(self.metric,
                                                    MyTrainableClass)

        self.assertTrue(isinstance(best_trainable, MyTrainableClass))

    def testBestConfig(self):
        best_config = self.ea.get_best_config(self.metric)

        self.assertTrue(isinstance(best_config, dict))
        self.assertTrue("width" in best_config)
        self.assertTrue("height" in best_config)

    def testBestTrial(self):
        best_trial = self.ea.get_best_info(self.metric, flatten=False)

        self.assertTrue(isinstance(best_trial, dict))
        self.assertTrue("local_dir" in best_trial)
        self.assertEqual(best_trial["local_dir"],
                         os.path.expanduser(self.test_path))
        self.assertTrue("config" in best_trial)
        self.assertTrue("width" in best_trial["config"])
        self.assertTrue("height" in best_trial["config"])
        self.assertTrue("last_result" in best_trial)
        self.assertTrue(self.metric in best_trial["last_result"])

        min_trial = self.ea.get_best_info(
            self.metric, mode="min", flatten=False)

        self.assertTrue(isinstance(min_trial, dict))
        self.assertLess(min_trial["last_result"][self.metric],
                        best_trial["last_result"][self.metric])

        flat_trial = self.ea.get_best_info(self.metric, flatten=True)

        self.assertTrue(isinstance(min_trial, dict))
        self.assertTrue(self.metric in flat_trial)

    def testCheckpoints(self):
        checkpoints = self.ea._checkpoints

        self.assertTrue(isinstance(checkpoints, list))
        self.assertTrue(isinstance(checkpoints[0], dict))
        self.assertEqual(len(checkpoints), self.num_samples)

    def testStats(self):
        stats = self.ea.stats()

        self.assertTrue(isinstance(stats, dict))
        self.assertTrue("start_time" in stats)
        self.assertTrue("timestamp" in stats)

    def testRunnerData(self):
        runner_data = self.ea.runner_data()

        self.assertTrue(isinstance(runner_data, dict))
        self.assertTrue("_local_checkpoint_dir" in runner_data)
        self.assertEqual(runner_data["_local_checkpoint_dir"],
                         os.path.expanduser(self.test_path))

    def testBestLogdir(self):
        logdir = self.ea.get_best_logdir(self.metric)
        self.assertTrue(logdir.startswith(self.test_path))
        logdir2 = self.ea.get_best_logdir(self.metric, mode="min")
        self.assertTrue(logdir2.startswith(self.test_path))
        self.assertNotEquals(logdir, logdir2)

    def testAllDataframes(self):
        dataframes = self.ea.get_all_trial_dataframes()
        self.assertTrue(len(dataframes) == self.num_samples)

        self.assertTrue(isinstance(dataframes, dict))
        for df in dataframes.values():
            self.assertEqual(df.training_iteration.max(), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
