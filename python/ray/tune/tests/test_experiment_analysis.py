import unittest
import shutil
import tempfile
import random
import os
from pathlib import Path
import pickle
import pandas as pd
from numpy import nan

import ray
from ray import tune
from ray.air._internal.remote_storage import upload_to_uri
from ray.air.config import CheckpointConfig
from ray.tune.analysis.experiment_analysis import (
    NewExperimentAnalysis as ExperimentAnalysis,
)
import ray.tune.registry
from ray.tune.tests.utils.experiment import create_test_experiment_checkpoint
from ray.tune.utils.mock_trainable import MyTrainableClass
from ray.tune.utils.util import is_nan


class ExperimentAnalysisSuite(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=4, num_gpus=0, local_mode=True, include_dashboard=False)

    @classmethod
    def tearDownClass(cls):
        if ray.is_initialized:
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
            storage_path=self.test_dir,
            stop={"training_iteration": 1},
            checkpoint_config=CheckpointConfig(checkpoint_frequency=1),
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
            storage_path=self.test_dir,
            stop={"training_iteration": 1},
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

    def testLoadJsonAndCSV(self):
        all_dataframes_via_json = self.ea.fetch_trial_dataframes()

        # Delete all json files so that we can test fallback to csv loading
        experiment_path = Path(self.ea.experiment_path)
        for json_path in experiment_path.glob("*/*.json"):
            json_path.unlink()

        all_dataframes_via_csv = self.ea.fetch_trial_dataframes()

        assert set(all_dataframes_via_csv) == set(all_dataframes_via_json)

    def testTrialDataframe(self):
        trial = self.ea.best_trial
        trial_df = self.ea.trial_dataframes[trial.trial_id]

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

    def testGetTrialCheckpointsWithMetric(self):
        best_trial = self.ea.get_best_trial(self.metric, mode="max")
        checkpoints_with_metric = self.ea._get_trial_checkpoints_with_metric(
            best_trial, "training_iteration"
        )
        sorted_by_metric = sorted(checkpoints_with_metric, key=lambda x: x[1])
        metric = sorted_by_metric[-1][1]
        assert metric == 1

    def testGetBestCheckpoint(self):
        best_trial = self.ea.get_best_trial(self.metric, mode="max")
        assert (
            best_trial.run_metadata.checkpoint_manager.best_checkpoint_result.checkpoint
            == self.ea.get_best_checkpoint(best_trial, self.metric, mode="max")
        )

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

        ea = tune.run(train, storage_path=self.test_dir, config={"steps": 3})
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
        assert best_checkpoint._local_path == expected_checkpoint_no_nan

    def testGetLastCheckpoint(self):
        # one more experiment with 2 iterations
        new_ea = tune.run(
            MyTrainableClass,
            name=self.test_name,
            storage_path=self.test_dir,
            stop={"training_iteration": 2},
            checkpoint_config=CheckpointConfig(checkpoint_frequency=1),
            config={
                "width": tune.sample_from(lambda spec: 10 + int(90 * random.random())),
                "height": tune.sample_from(lambda spec: int(100 * random.random())),
            },
        )

        # check if it's loaded correctly
        last_checkpoint = new_ea.get_last_checkpoint()._local_path
        assert self.test_path in last_checkpoint
        assert "checkpoint_000002" in last_checkpoint

        # test restoring the checkpoint and running for another iteration
        tune.run(
            MyTrainableClass,
            name=self.test_name,
            storage_path=self.test_dir,
            restore=last_checkpoint,
            stop={"training_iteration": 3},
            checkpoint_config=CheckpointConfig(checkpoint_frequency=1),
            config={
                "width": tune.sample_from(lambda spec: 10 + int(90 * random.random())),
                "height": tune.sample_from(lambda spec: int(100 * random.random())),
            },
        )

    def testAllDataframes(self):
        dataframes = self.ea.trial_dataframes
        self.assertEqual(len(dataframes), self.num_samples)

        self.assertTrue(isinstance(dataframes, dict))
        for df in dataframes.values():
            self.assertEqual(df.training_iteration.max(), 1)


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
        self.assertEqual(ea.best_logdir, trials[2].local_path)
        self.assertEqual(
            ea.best_checkpoint._local_path, trials[2].checkpoint.dir_or_data
        )
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


def run_test_exp(path: str) -> ExperimentAnalysis:
    with create_test_experiment_checkpoint(path) as creator:
        for i in range(10):
            trial = creator.create_trial(f"trial_{i}", config={"id": i, "hparam": 1})
            creator.trial_result(
                trial,
                {
                    "training_iteration": 1,
                    "episode_reward_mean": 10 + int(90 * random.random()),
                },
            )

    return ExperimentAnalysis(path, trials=creator.get_trials())


class ExperimentAnalysisStubSuite(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_name = "analysis_exp"
        self.num_samples = 2
        self.metric = "episode_reward_mean"
        self.test_path = os.path.join(self.test_dir, self.test_name)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def testPickling(self):
        analysis = run_test_exp(self.test_path)
        pickle_path = os.path.join(self.test_dir, "analysis.pickle")
        with open(pickle_path, "wb") as f:
            pickle.dump(analysis, f)

        self.assertTrue(analysis.get_best_trial(metric=self.metric, mode="max"))

        with open(pickle_path, "rb") as f:
            analysis = pickle.load(f)

        self.assertTrue(analysis.get_best_trial(metric=self.metric, mode="max"))

    def testFromLocalPath(self):
        run_test_exp(self.test_path)
        analysis = ExperimentAnalysis(self.test_path)

        self.assertTrue(analysis.get_best_trial(metric=self.metric, mode="max"))

        analysis = ExperimentAnalysis(self.test_path)

        # This will be None if validate_trainable during loading fails
        self.assertTrue(analysis.get_best_trial(metric=self.metric, mode="max"))

    def testEmptyCheckpoint(self):
        """Test that empty checkpoints can still be loaded in experiment analysis.

        Background: If restore from a checkpoint fails, we overwrite the checkpoint
        data with ``None`` (because we assume the current contents are invalid, e.g.
        an invalid object ref, or a corrupted directory). But ExperimentAnalysis
        currently previously failed loading if it is None. This tests makes
        sure we can still load the checkpoint.

        """
        with create_test_experiment_checkpoint(self.test_path) as creator:
            for i in range(10):
                trial = creator.create_trial(f"trial_{i}", config={})
                creator.trial_result(
                    trial,
                    {
                        "training_iteration": 1,
                        "episode_reward_mean": 10 + int(90 * random.random()),
                    },
                )
                creator.trial_checkpoint(trial, "first")
                creator.trial_checkpoint(trial, None)
                creator.trial_checkpoint(trial, "third")

        ea = ExperimentAnalysis(self.test_dir)
        assert len(ea.trials) == 10


def test_create_from_remote_path(tmp_path, mock_s3_bucket_uri):
    run_test_exp(str(tmp_path))
    upload_to_uri(str(tmp_path), mock_s3_bucket_uri)

    local_analysis = ExperimentAnalysis(str(tmp_path))
    remote_analysis = ExperimentAnalysis(mock_s3_bucket_uri)

    metric = "episode_reward_mean"
    mode = "max"

    # Tracked metric data is the same
    assert (
        local_analysis.get_best_trial(metric=metric, mode=mode).trial_id
        == remote_analysis.get_best_trial(metric=metric, mode=mode).trial_id
    )

    # Trial result dataframes are the same
    assert all(
        local_df.equals(remote_df)
        for local_df, remote_df in zip(
            local_analysis.trial_dataframes.values(),
            remote_analysis.trial_dataframes.values(),
        )
    )

    # Trial configs are the same
    assert list(local_analysis.get_all_configs().values()) == list(
        remote_analysis.get_all_configs().values()
    )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
