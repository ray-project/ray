import pandas as pd
import pandas.testing as pd_testing
from pathlib import Path
import shutil
import sys
import unittest
import uuid

import ray
from ray.rllib.agents.ppo import ppo
from ray import tune
from ray.tune.trial import Trial


class TrialRelativeLogdirTest(unittest.TestCase):
    def setUp(self):
        # Make the data frame equality assertion available
        # via `assertEqual`.
        self.addTypeEqualityFunc(pd.DataFrame, self.assertDataFrameEqual)
        ray.init(num_cpus=1, num_gpus=0)

    def tearDown(self):
        ray.shutdown()

    # Compare pandas data frames inside of the test suite.
    def assertDataFrameEqual(self, df1, df2, msg):
        try:
            pd_testing.assert_frame_equal(df1, df2)
        except AssertionError as e:
            raise self.failureException(msg) from e

    def testDotsInLogdir(self):
        config = ppo.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0
        config["num_envs_per_worker"] = 1
        config["env"] = "CartPole-v0"

        local_dir_path = Path("/tmp/test_rel_dots")
        local_dir = str(local_dir_path)
        if local_dir_path.exists():
            local_dir += "_" + uuid.uuid4().hex[:4]
        trial = Trial(trainable_name="PPO", local_dir=local_dir)

        with self.assertRaises(ValueError):
            trial.logdir = "/tmp/test_rel/../dots"
        with self.assertRaises(ValueError):
            trial.logdir = local_dir + "/../"

        if shutil.rmtree.avoids_symlink_attacks:
            if local_dir_path.exists():
                shutil.rmtree(local_dir)

    def testRelativeLogdir(self):
        config = ppo.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0
        config["num_envs_per_worker"] = 1
        config["env"] = "CartPole-v0"

        local_dir_path = Path("/tmp/test_rel")
        if local_dir_path.exists():
            local_dir = str(local_dir_path) + "_" + uuid.uuid4().hex[:4]

        tune.run("PPO", config=config, stop={"episodes_total": 5}, local_dir=local_dir)

        # Copy the folder
        local_dir_moved = local_dir + "_moved"
        shutil.copytree(local_dir, local_dir_moved)

        # Load the moved trials
        analysis = tune.ExperimentAnalysis(local_dir)
        analysis_moved = tune.ExperimentAnalysis(local_dir_moved)

        configs = analysis.get_all_configs()
        configs_moved = analysis_moved.get_all_configs()
        config = configs[next(iter(configs))]
        config_moved = configs_moved[next(iter(configs_moved))]

        # Check, if the trial attributes can be loaded.
        self.assertEqual(len(configs), 1)
        self.assertEqual(len(configs_moved), 1)

        # Check, if the two configs are equal.
        self.assertDictEqual(config, config_moved)

        metric = "episode_reward_mean"
        mode = "max"
        analysis_df = analysis.dataframe(metric, mode)
        analysis_moved_df = analysis_moved.dataframe(metric, mode)

        self.assertEqual(analysis_df.shape[0], 1)
        self.assertEqual(analysis_moved_df.shape[0], 1)

        # Drop the `logdir` column as this should be different
        # between the two trials.
        analysis_df.drop(columns="logdir", inplace=True)
        analysis_moved_df.drop(columns="logdir", inplace=True)
        self.assertEqual(analysis_df, analysis_moved_df)

        # Remove the files and directories.
        if shutil.rmtree.avoids_symlink_attacks:
            if local_dir_path.exists():
                shutil.rmtree(local_dir)
            shutil.rmtree(local_dir_moved)

    def testRelativeLogdirWithJson(self):
        config = ppo.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0
        config["num_envs_per_worker"] = 1
        config["env"] = "CartPole-v0"

        local_dir_path = Path("/tmp/test_rel")
        if local_dir_path.exists():
            local_dir = str(local_dir_path) + "_" + uuid.uuid4().hex[:4]

        tune.run("PPO", config=config, stop={"episodes_total": 5}, local_dir=local_dir)

        # Copy the folder.
        local_dir_moved = local_dir + "_moved"
        shutil.copytree(local_dir, local_dir_moved)
        checkpoint_dir = Path(local_dir).joinpath("PPO")
        checkpoint_dir_moved = Path(local_dir_moved).joinpath("PPO")
        experiment_state = [
            f
            for f in checkpoint_dir.iterdir()
            if f.is_file() and f.name.startswith("experiment")
        ][0]
        experiment_state_moved = [
            f
            for f in checkpoint_dir_moved.iterdir()
            if f.is_file() and f.name.startswith("experiment")
        ][0]

        # Load the copied trials
        analysis = tune.ExperimentAnalysis(str(experiment_state))
        analysis_moved = tune.ExperimentAnalysis(str(experiment_state_moved))

        configs = analysis.get_all_configs()
        configs_moved = analysis_moved.get_all_configs()
        config = configs[next(iter(configs))]
        config_moved = configs_moved[next(iter(configs_moved))]

        # Check, if the trial attributes can be loaded.
        self.assertEqual(len(configs), 1)
        self.assertEqual(len(configs_moved), 1)

        # Check, if the two configs are equal.
        self.assertDictEqual(config, config_moved)

        metric = "episode_reward_mean"
        mode = "max"
        analysis_df = analysis.dataframe(metric, mode)
        analysis_moved_df = analysis_moved.dataframe(metric, mode)

        self.assertEqual(analysis_df.shape[0], 1)
        self.assertEqual(analysis_moved_df.shape[0], 1)

        # Drop the `logdir` column as this should be different
        # between the two trials.
        analysis_df.drop(columns="logdir", inplace=True)
        analysis_moved_df.drop(columns="logdir", inplace=True)
        self.assertEqual(analysis_df, analysis_moved_df)

        # Remove the files and directories.
        if shutil.rmtree.avoids_symlink_attacks:
            if local_dir_path.exists():
                shutil.rmtree(local_dir)
            shutil.rmtree(local_dir_moved)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
