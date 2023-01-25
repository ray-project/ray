import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd
import pandas.testing as pd_testing
import pytest

import ray
from ray import tune
from ray.air._internal.checkpoint_manager import CheckpointStorage, _TrackedCheckpoint
from ray.tune.experiment import Trial


def train(config):
    tune.report(metric1=2, metric2=3)


class TrialRelativeLogdirTest(unittest.TestCase):
    def setUp(self):
        # Make the data frame equality assertion available
        # via `assertEqual`.
        self.addTypeEqualityFunc(pd.DataFrame, self.assertDataFrameEqual)
        if ray.is_initialized:
            ray.shutdown()
        ray.init(num_cpus=1, num_gpus=0)

        tune.register_trainable("rel_logdir", train)

    def tearDown(self):
        ray.shutdown()

    # Compare pandas data frames inside of the test suite.
    def assertDataFrameEqual(self, df1, df2, msg):
        try:
            pd_testing.assert_frame_equal(df1, df2)
        except AssertionError as e:
            raise self.failureException(msg) from e

    def testDotsInLogdir(self):
        """This should result in errors as dots in paths are not allowed."""
        local_dir_path = Path("/tmp/test_rel_dots")
        local_dir = str(local_dir_path)
        if local_dir_path.exists():
            local_dir = tempfile.mkdtemp(prefix=str(local_dir_path) + "_")
        trial = Trial(trainable_name="rel_logdir", local_dir=local_dir)

        with self.assertRaises(ValueError):
            trial.logdir = "/tmp/test_rel/../dots"
        with self.assertRaises(ValueError):
            trial.logdir = local_dir + "/../"

        if shutil.rmtree.avoids_symlink_attacks:
            if local_dir_path.exists():
                shutil.rmtree(local_dir)

    def testRelativeLogdir(self):
        """Moving the experiment folder into another location.

        This should still work and is an important use case,
        because training in the cloud usually requests such
        relocations.
        """
        local_dir_path = Path("/tmp/test_rel")
        if local_dir_path.exists():
            local_dir = tempfile.mkdtemp(prefix=str(local_dir_path) + "_")
        else:
            local_dir = str(local_dir_path)

        tune.run("rel_logdir", config={"a": tune.randint(0, 10)}, local_dir=local_dir)

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

        metric = "metric1"
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

    def testRelativeLogdirWithNestedDir(self):
        """Using a nested directory for experiment name."

        This should raise an error as nested structures are not
        supported. It should work, however, to provide a nested
        path to the `ExperimentAnalysis` class or relocate the
        folder out of the nested structure.
        """
        local_dir_path = Path("/tmp/test_rel")
        if local_dir_path.exists():
            local_dir = tempfile.mkdtemp(prefix=str(local_dir_path) + "_")
        else:
            local_dir = str(local_dir_path)

        tune.run(
            "rel_logdir",
            config={"a": tune.randint(0, 10)},
            local_dir=local_dir,
            # Create a nested experiment directory.
            name="exp_dir/deep_exp_dir",
        )

        # Copy the folder
        local_dir_moved = local_dir + "_moved"
        shutil.copytree(local_dir + "/exp_dir", local_dir_moved)

        # Load the trials.
        with self.assertRaises(ValueError):
            analysis = tune.ExperimentAnalysis(local_dir)

        # Using the subdir should work, however.
        analysis = tune.ExperimentAnalysis(local_dir + "/exp_dir")
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

        metric = "metric1"
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
        """Relative paths with experiment states should work.

        Providing an experiment state is the other way to create
        an `ExperimentAnalysis` object. Note, if there are multiple
        experiments in a folder only the one with the experiment
        state provided to the `ExperimentAnalysis` constructor will
        be considered.
        """
        local_dir_path = Path("/tmp/test_rel")
        if local_dir_path.exists():
            local_dir = tempfile.mkdtemp(prefix=str(local_dir_path) + "_")
        else:
            local_dir = str(local_dir_path)

        tune.run(
            "rel_logdir",
            config={"a": tune.randint(0, 10)},
            local_dir=local_dir,
        )

        # Copy the folder.
        local_dir_moved = local_dir + "_moved"
        shutil.copytree(local_dir, local_dir_moved)

        # Load the experiment states directly by providing
        # JSON files.
        checkpoint_dir = Path(local_dir).joinpath("rel_logdir")
        checkpoint_dir_moved = Path(local_dir_moved).joinpath("rel_logdir")
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

        metric = "metric1"
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


def test_load_trial_from_json_state(tmpdir):
    """Check that serializing a trial to a JSON string with `Trial.get_json_state`
    and then creating a new trial using the `Trial.from_json_state` alternate
    constructor loads the trial with equivalent state."""
    trial = Trial(
        "MockTrainable", stub=True, trial_id="abcd1234", local_dir=str(tmpdir)
    )
    trial.init_logdir()
    trial.status = Trial.TERMINATED

    checkpoint_logdir = os.path.join(trial.logdir, "checkpoint_00000")
    trial.checkpoint_manager.on_checkpoint(
        _TrackedCheckpoint(
            dir_or_data=checkpoint_logdir,
            storage_mode=CheckpointStorage.PERSISTENT,
            metrics={"training_iteration": 1},
        )
    )

    # After loading, the trial state should be the same
    json_state = trial.get_json_state()
    new_trial = Trial.from_json_state(json_state, stub=True)
    assert new_trial.get_json_state() == json_state


def test_change_trial_local_dir(tmpdir):
    trial = Trial(
        "MockTrainable", stub=True, trial_id="abcd1234", local_dir=str(tmpdir)
    )
    trial.init_logdir()
    trial.status = Trial.TERMINATED

    checkpoint_logdir = os.path.join(trial.logdir, "checkpoint_00000")
    trial.checkpoint_manager.on_checkpoint(
        _TrackedCheckpoint(
            dir_or_data=checkpoint_logdir,
            storage_mode=CheckpointStorage.PERSISTENT,
            metrics={"training_iteration": 1},
        )
    )

    assert trial.logdir.startswith(str(tmpdir))
    assert trial.get_trial_checkpoints()[0].dir_or_data.startswith(str(tmpdir))

    # Specify a new local dir, and the logdir/checkpoint path should be updated
    with tempfile.TemporaryDirectory() as new_local_dir:
        trial.local_dir = new_local_dir

        assert trial.logdir.startswith(new_local_dir)
        assert trial.get_trial_checkpoints()[0].dir_or_data.startswith(new_local_dir)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
