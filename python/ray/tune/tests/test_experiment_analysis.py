import unittest
import shutil
import tempfile
import random
import os
import pandas as pd
from numpy import nan

import ray
from ray import tune
from ray.tune.utils.mock import MyTrainableClass


class ExperimentAnalysisSuite(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(
            num_cpus=4, num_gpus=0, local_mode=True, include_dashboard=False)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_name = "analysis_exp"
        self.num_samples = 10
        self.metric = "episode_reward_mean"
        self.test_path = os.path.join(self.test_dir, self.test_name)
        self.run_test_exp()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def run_test_exp(self):
        self.ea = tune.run(
            MyTrainableClass,
            name=self.test_name,
            local_dir=self.test_dir,
            stop={"training_iteration": 1},
            checkpoint_freq=1,
            num_samples=self.num_samples,
            config={
                "width": tune.sample_from(
                    lambda spec: 10 + int(90 * random.random())),
                "height": tune.sample_from(
                    lambda spec: int(100 * random.random())),
            })

    def nan_test_exp(self):
        nan_ea = tune.run(
            lambda x: nan,
            name="testing_nan",
            local_dir=self.test_dir,
            stop={"training_iteration": 1},
            checkpoint_freq=1,
            num_samples=self.num_samples,
            config={
                "width": tune.sample_from(
                    lambda spec: 10 + int(90 * random.random())),
                "height": tune.sample_from(
                    lambda spec: int(100 * random.random())),
            })
        return nan_ea

    def testDataframe(self):
        df = self.ea.dataframe(self.metric, mode="max")

        self.assertTrue(isinstance(df, pd.DataFrame))
        self.assertEquals(df.shape[0], self.num_samples)

    def testStats(self):
        assert self.ea.stats()
        assert self.ea.runner_data()

    def testTrialDataframe(self):
        checkpoints = self.ea._checkpoints
        idx = random.randint(0, len(checkpoints) - 1)
        trial_df = self.ea.trial_dataframes[checkpoints[idx]["logdir"]]

        self.assertTrue(isinstance(trial_df, pd.DataFrame))
        self.assertEqual(trial_df.shape[0], 1)

    def testBestConfig(self):
        best_config = self.ea.get_best_config(self.metric, mode="max")
        self.assertTrue(isinstance(best_config, dict))
        self.assertTrue("width" in best_config)
        self.assertTrue("height" in best_config)

    def testBestConfigNan(self):
        nan_ea = self.nan_test_exp()
        best_config = nan_ea.get_best_config(self.metric, mode="max")
        self.assertIsNone(best_config)

    def testBestLogdir(self):
        logdir = self.ea.get_best_logdir(self.metric, mode="max")
        self.assertTrue(logdir.startswith(self.test_path))
        logdir2 = self.ea.get_best_logdir(self.metric, mode="min")
        self.assertTrue(logdir2.startswith(self.test_path))
        self.assertNotEquals(logdir, logdir2)

    def testBestLogdirNan(self):
        nan_ea = self.nan_test_exp()
        logdir = nan_ea.get_best_logdir(self.metric, mode="max")
        self.assertIsNone(logdir)

    def testGetTrialCheckpointsPathsByTrial(self):
        best_trial = self.ea.get_best_trial(self.metric, mode="max")
        checkpoints_metrics = self.ea.get_trial_checkpoints_paths(best_trial)
        logdir = self.ea.get_best_logdir(self.metric, mode="max")
        expected_path = os.path.join(logdir, "checkpoint_000001", "checkpoint")
        assert checkpoints_metrics[0][0] == expected_path
        assert checkpoints_metrics[0][1] == 1

    def testGetTrialCheckpointsPathsByPath(self):
        logdir = self.ea.get_best_logdir(self.metric, mode="max")
        checkpoints_metrics = self.ea.get_trial_checkpoints_paths(logdir)
        expected_path = os.path.join(logdir, "checkpoint_000001/",
                                     "checkpoint")
        assert checkpoints_metrics[0][0] == expected_path
        assert checkpoints_metrics[0][1] == 1

    def testGetTrialCheckpointsPathsWithMetricByTrial(self):
        best_trial = self.ea.get_best_trial(self.metric, mode="max")
        paths = self.ea.get_trial_checkpoints_paths(best_trial, self.metric)
        logdir = self.ea.get_best_logdir(self.metric, mode="max")
        expected_path = os.path.join(logdir, "checkpoint_000001", "checkpoint")
        assert paths[0][0] == expected_path
        assert paths[0][1] == best_trial.metric_analysis[self.metric]["last"]

    def testGetTrialCheckpointsPathsWithMetricByPath(self):
        best_trial = self.ea.get_best_trial(self.metric, mode="max")
        logdir = self.ea.get_best_logdir(self.metric, mode="max")
        paths = self.ea.get_trial_checkpoints_paths(best_trial, self.metric)
        expected_path = os.path.join(logdir, "checkpoint_000001", "checkpoint")
        assert paths[0][0] == expected_path
        assert paths[0][1] == best_trial.metric_analysis[self.metric]["last"]

    def testGetBestCheckpoint(self):
        best_trial = self.ea.get_best_trial(self.metric, mode="max")
        checkpoints_metrics = self.ea.get_trial_checkpoints_paths(best_trial)
        expected_path = max(checkpoints_metrics, key=lambda x: x[1])[0]
        best_checkpoint = self.ea.get_best_checkpoint(
            best_trial, self.metric, mode="max")
        assert expected_path == best_checkpoint

    def testAllDataframes(self):
        dataframes = self.ea.trial_dataframes
        self.assertTrue(len(dataframes) == self.num_samples)

        self.assertTrue(isinstance(dataframes, dict))
        for df in dataframes.values():
            self.assertEqual(df.training_iteration.max(), 1)

    def testIgnoreOtherExperiment(self):
        analysis = tune.run(
            MyTrainableClass,
            name="test_example",
            local_dir=self.test_dir,
            stop={"training_iteration": 1},
            num_samples=1,
            config={
                "width": tune.sample_from(
                    lambda spec: 10 + int(90 * random.random())),
                "height": tune.sample_from(
                    lambda spec: int(100 * random.random())),
            })
        df = analysis.dataframe(self.metric, mode="max")
        self.assertEquals(df.shape[0], 1)


class ExperimentAnalysisPropertySuite(unittest.TestCase):
    def testBestProperties(self):
        def train(config):
            for i in range(10):
                with tune.checkpoint_dir(i):
                    pass
                tune.report(res=config["base"] + i)

        ea = tune.run(
            train,
            config={"base": tune.grid_search([100, 200, 300])},
            metric="res",
            mode="max")

        trials = ea.trials

        self.assertEquals(ea.best_trial, trials[2])
        self.assertEquals(ea.best_config, trials[2].config)
        self.assertEquals(ea.best_logdir, trials[2].logdir)
        self.assertEquals(ea.best_checkpoint, trials[2].checkpoint.value)
        self.assertTrue(
            all(ea.best_dataframe["trial_id"] == trials[2].trial_id))
        self.assertEquals(ea.results_df.loc[trials[2].trial_id, "res"], 309)
        self.assertEquals(ea.best_result["res"], 309)
        self.assertEquals(ea.best_result_df.loc[trials[2].trial_id, "res"],
                          309)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
