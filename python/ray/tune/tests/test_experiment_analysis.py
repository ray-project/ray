import unittest
import shutil
import tempfile
import random
import os
import pickle
import pandas as pd
from numpy import nan

import ray
from ray import tune
from ray.tune import ExperimentAnalysis
import ray.tune.registry
from ray.tune.utils.mock_trainable import MyTrainableClass
from ray.tune.utils.util import is_nan


class ExperimentAnalysisSuite(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=4, num_gpus=0, local_mode=True, include_dashboard=False)

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
                "width": tune.sample_from(lambda spec: 10 + int(90 * random.random())),
                "height": tune.sample_from(lambda spec: int(100 * random.random())),
            },
        )

    def nan_test_exp(self):
        nan_ea = tune.run(
            lambda x: nan,
            name="testing_nan",
            local_dir=self.test_dir,
            stop={"training_iteration": 1},
            checkpoint_freq=1,
            num_samples=self.num_samples,
            config={
                "width": tune.sample_from(lambda spec: 10 + int(90 * random.random())),
                "height": tune.sample_from(lambda spec: int(100 * random.random())),
            },
        )
        return nan_ea

    def testDataframe(self):
        df = self.ea.dataframe(self.metric, mode="max")

        self.assertTrue(isinstance(df, pd.DataFrame))
        self.assertEqual(df.shape[0], self.num_samples)

    def testLoadJson(self):
        all_dataframes_via_csv = self.ea.fetch_trial_dataframes()

        self.ea.set_filetype("json")
        all_dataframes_via_json = self.ea.fetch_trial_dataframes()

        assert set(all_dataframes_via_csv) == set(all_dataframes_via_json)

        with self.assertRaises(ValueError):
            self.ea.set_filetype("bad")

        self.ea.set_filetype("csv")
        all_dataframes_via_csv2 = self.ea.fetch_trial_dataframes()
        assert set(all_dataframes_via_csv) == set(all_dataframes_via_csv2)

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
        self.assertNotEqual(logdir, logdir2)

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
        expected_path = os.path.join(logdir, "checkpoint_000001/", "checkpoint")
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
        checkpoints_metrics = self.ea.get_trial_checkpoints_paths(
            best_trial, metric=self.metric
        )
        expected_path = max(checkpoints_metrics, key=lambda x: x[1])[0]
        best_checkpoint = self.ea.get_best_checkpoint(
            best_trial, self.metric, mode="max"
        )
        assert expected_path == best_checkpoint

    def testGetBestCheckpointNan(self):
        """Tests if nan values are excluded from best checkpoint."""
        metric = "loss"

        def train(config):
            for i in range(config["steps"]):
                if i == 0:
                    value = float("nan")
                else:
                    value = i
                result = {metric: value}
                with tune.checkpoint_dir(step=i):
                    pass
                tune.report(**result)

        ea = tune.run(train, local_dir=self.test_dir, config={"steps": 3})
        best_trial = ea.get_best_trial(metric, mode="min")
        best_checkpoint = ea.get_best_checkpoint(best_trial, metric, mode="min")
        checkpoints_metrics = ea.get_trial_checkpoints_paths(best_trial, metric=metric)
        expected_checkpoint_no_nan = min(
            [
                checkpoint_metric
                for checkpoint_metric in checkpoints_metrics
                if not is_nan(checkpoint_metric[1])
            ],
            key=lambda x: x[1],
        )[0]
        assert best_checkpoint == expected_checkpoint_no_nan

    def testGetLastCheckpoint(self):
        # one more experiment with 2 iterations
        new_ea = tune.run(
            MyTrainableClass,
            name=self.test_name,
            local_dir=self.test_dir,
            stop={"training_iteration": 2},
            checkpoint_freq=1,
            config={
                "width": tune.sample_from(lambda spec: 10 + int(90 * random.random())),
                "height": tune.sample_from(lambda spec: int(100 * random.random())),
            },
        )

        # check if it's loaded correctly
        last_checkpoint = new_ea.get_last_checkpoint().local_path
        assert self.test_path in last_checkpoint
        assert "checkpoint_000002" in last_checkpoint

        # test restoring the checkpoint and running for another iteration
        tune.run(
            MyTrainableClass,
            name=self.test_name,
            local_dir=self.test_dir,
            restore=last_checkpoint,
            stop={"training_iteration": 3},
            checkpoint_freq=1,
            config={
                "width": tune.sample_from(lambda spec: 10 + int(90 * random.random())),
                "height": tune.sample_from(lambda spec: int(100 * random.random())),
            },
        )

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
                "width": tune.sample_from(lambda spec: 10 + int(90 * random.random())),
                "height": tune.sample_from(lambda spec: int(100 * random.random())),
            },
        )
        df = analysis.dataframe(self.metric, mode="max")
        self.assertEqual(df.shape[0], 1)

    def testGetTrialCheckpointsPathsByPathWithSpecialCharacters(self):
        analysis = tune.run(
            MyTrainableClass,
            name="test_example",
            local_dir=self.test_dir,
            stop={"training_iteration": 1},
            num_samples=1,
            config={"test": tune.grid_search([[1, 2], [3, 4]])},
            checkpoint_at_end=True,
        )
        logdir = analysis.get_best_logdir(self.metric, mode="max")
        checkpoints_metrics = analysis.get_trial_checkpoints_paths(logdir)
        expected_path = os.path.join(logdir, "checkpoint_000001/", "checkpoint")
        assert checkpoints_metrics[0][0] == expected_path
        assert checkpoints_metrics[0][1] == 1

    def testGetTrialCheckpointsPathsWithTemporaryCheckpoints(self):
        analysis = tune.run(
            MyTrainableClass,
            name="test_example",
            local_dir=self.test_dir,
            stop={"training_iteration": 2},
            num_samples=1,
            config={"test": tune.grid_search([[1, 2], [3, 4]])},
            checkpoint_at_end=True,
        )
        logdir = analysis.get_best_logdir(self.metric, mode="max")

        shutil.copytree(
            os.path.join(logdir, "checkpoint_000002"),
            os.path.join(logdir, "checkpoint_tmpxxx"),
        )

        checkpoints_metrics = analysis.get_trial_checkpoints_paths(logdir)
        expected_path = os.path.join(logdir, "checkpoint_000002/", "checkpoint")

        assert len(checkpoints_metrics) == 1

        assert checkpoints_metrics[0][0] == expected_path
        assert checkpoints_metrics[0][1] == 2


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
            mode="max",
        )

        trials = ea.trials

        self.assertEqual(ea.best_trial, trials[2])
        self.assertEqual(ea.best_config, trials[2].config)
        self.assertEqual(ea.best_logdir, trials[2].logdir)
        self.assertEqual(ea.best_checkpoint._local_path, trials[2].checkpoint.value)
        self.assertTrue(all(ea.best_dataframe["trial_id"] == trials[2].trial_id))
        self.assertEqual(ea.results_df.loc[trials[2].trial_id, "res"], 309)
        self.assertEqual(ea.best_result["res"], 309)
        self.assertEqual(ea.best_result_df.loc[trials[2].trial_id, "res"], 309)

    def testDataframeBestResult(self):
        def train(config):
            if config["var"] == 1:
                tune.report(loss=9)
                tune.report(loss=7)
                tune.report(loss=5)
            else:
                tune.report(loss=10)
                tune.report(loss=4)
                tune.report(loss=10)

        analysis = tune.run(
            train, config={"var": tune.grid_search([1, 2])}, metric="loss", mode="min"
        )

        self.assertEqual(analysis.best_config["var"], 1)

        with self.assertRaises(ValueError):
            # Should raise because we didn't pass a metric
            df = analysis.dataframe(mode="max")

        # If we specify `min`, we expect the lowest ever observed result
        df = analysis.dataframe(metric="loss", mode="min")
        var = df[df.loss == df.loss.min()]["config/var"].values[0]
        self.assertEqual(var, 2)

        # If we don't pass a mode, we just fetch the last result
        df = analysis.dataframe(metric="loss")
        var = df[df.loss == df.loss.min()]["config/var"].values[0]
        self.assertEqual(var, 1)

        df = analysis.dataframe()
        var = df[df.loss == df.loss.min()]["config/var"].values[0]
        self.assertEqual(var, 1)


class ExperimentAnalysisStubSuite(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_name = "analysis_exp"
        self.num_samples = 2
        self.metric = "episode_reward_mean"
        self.test_path = os.path.join(self.test_dir, self.test_name)
        self.run_test_exp()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)
        ray.shutdown()

    def run_test_exp(self):
        def training_function(config, checkpoint_dir=None):
            tune.report(episode_reward_mean=config["alpha"])

        return tune.run(
            training_function,
            name=self.test_name,
            local_dir=self.test_dir,
            stop={"training_iteration": 1},
            num_samples=self.num_samples,
            config={
                "alpha": tune.sample_from(lambda spec: 10 + int(90 * random.random())),
            },
        )

    def testPickling(self):
        analysis = self.run_test_exp()
        pickle_path = os.path.join(self.test_dir, "analysis.pickle")
        with open(pickle_path, "wb") as f:
            pickle.dump(analysis, f)

        self.assertTrue(analysis.get_best_trial(metric=self.metric, mode="max"))

        ray.shutdown()
        ray.tune.registry._global_registry = ray.tune.registry._Registry(
            prefix="global"
        )

        with open(pickle_path, "rb") as f:
            analysis = pickle.load(f)

        self.assertTrue(analysis.get_best_trial(metric=self.metric, mode="max"))

    def testFromPath(self):
        self.run_test_exp()
        analysis = ExperimentAnalysis(self.test_path)

        self.assertTrue(analysis.get_best_trial(metric=self.metric, mode="max"))

        ray.shutdown()
        ray.tune.registry._global_registry = ray.tune.registry._Registry(
            prefix="global"
        )

        analysis = ExperimentAnalysis(self.test_path)

        # This will be None if validate_trainable during loading fails
        self.assertTrue(analysis.get_best_trial(metric=self.metric, mode="max"))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
