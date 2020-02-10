# coding: utf-8
import os
import pickle
import shutil
import tempfile
import unittest

import ray
from ray import tune
from ray.rllib import _register_all
from ray.tune import Trainable


class WarmStartTest(unittest.TestCase):
    local_mode = True
    prefix = "Warmstart"

    class MockTrainable(Trainable):
        _name = "MockTrainable"

        def _setup(self, config):
            self.state = {"hi": 1}

        def _train(self):
            return {"done": True}

        def _save(self, checkpoint_dir):
            checkpoint_path = os.path.join(
                checkpoint_dir, "checkpoint-{}".format(self._iteration))
            with open(checkpoint_path, "wb") as f:
                pickle.dump(self.state, f)
            return checkpoint_path

        def _restore(self, checkpoint_path):
            with open(checkpoint_path, "rb") as f:
                extra_data = pickle.load(f)
            self.state.update(extra_data)

    def setUp(self):
        ray.init(num_cpus=1, num_gpus=0, local_mode=self.local_mode)

    def tearDown(self):
        shutil.rmtree(self.absolute_local_dir, ignore_errors=True)
        self.absolute_local_dir = None
        ray.shutdown()
        # Without this line, test_tune_server.testAddTrial would fail.
        _register_all()

    def _train(self, exp_name, absolute_local_dir, num_samples=1,
               restore=None):
        trials = tune.run(
            self.MockTrainable,
            num_samples=num_samples,
            restore=restore,
            name=exp_name,
            stop={
                "training_iteration": 1
            },
            checkpoint_freq=1,
            local_dir=absolute_local_dir,
            config={
                "env": "CartPole-v0",
                "log_level": "DEBUG"
            }).trials

        exp_dir = os.path.join(absolute_local_dir, exp_name)

        checkpoint_paths = []

        for trial in trials:
            self.assertIsNone(trial.error_file)
            self.assertEqual(trial.local_dir, exp_dir)
            checkpoint_path = os.path.join(trial.logdir,
                                           "checkpoint_1/checkpoint-1")
            self.assertTrue(os.path.isfile(checkpoint_path))
            checkpoint_paths.append(checkpoint_path)

        return trials, checkpoint_paths

    def testSimpleRestore(self):
        # test base functionality of restore=<path_to_checkpoint>
        exp_name = self.prefix + "SimpleRestore"
        local_dir = "test_simple_restore"
        absolute_local_dir = os.path.abspath(
            os.path.join(tempfile.gettempdir(), local_dir))
        self.absolute_local_dir = absolute_local_dir
        self.assertFalse(os.path.exists(absolute_local_dir))
        trials, paths_to_checkpoints = self._train(
            exp_name, absolute_local_dir=absolute_local_dir)
        new_trials, new_paths_to_checkpoints = self._train(
            exp_name,
            absolute_local_dir=absolute_local_dir,
            restore=paths_to_checkpoints[0])
        # ensure the restored trial was given a new ID
        self.assertNotEqual(trials[0].trial_id, new_trials[0].trial_id)
        # the new trial's name should have
        # restore=<reference_to_restored_trial>
        self.assertTrue("restore=" in new_paths_to_checkpoints[0])
        self.assertTrue(trials[0].trial_id in new_paths_to_checkpoints[0])

    def testWarmStartGridSearch(self):
        exp_name = self.prefix + "WarmStartGridSearch"
        local_dir = "warm_start_grid_search"
        absolute_local_dir = os.path.abspath(
            os.path.join(tempfile.gettempdir(), local_dir))
        self.absolute_local_dir = absolute_local_dir
        self.assertFalse(os.path.exists(absolute_local_dir))
        # train a small number of trials
        trials, paths_to_checkpoints = self._train(
            exp_name, absolute_local_dir=absolute_local_dir, num_samples=4)
        # start a new set of trials using the checkpoints
        # as a starting point
        new_trials, new_checkpoints = self._train(
            exp_name,
            absolute_local_dir=absolute_local_dir,
            restore=tune.grid_search(paths_to_checkpoints))
        for old_trial, old_checkpoint in zip(trials, paths_to_checkpoints):
            used = False
            for new_trial, new_checkpoint in zip(new_trials, new_checkpoints):
                self.assertNotEqual(old_trial.trial_id, new_trial.trial_id)
                self.assertTrue("restore=" in new_checkpoint)
                if old_trial.trial_id in new_checkpoint:
                    used = True
                    break
            self.assertTrue(
                used, "checkpoint {} not used in grid_search" % old_checkpoint)


class SerialTuneRelativeLocalDirTest(unittest.TestCase):
    local_mode = True
    prefix = "Serial"

    class MockTrainable(Trainable):
        _name = "MockTrainable"

        def _setup(self, config):
            self.state = {"hi": 1}

        def _train(self):
            return {"timesteps_this_iter": 1, "done": True}

        def _save(self, checkpoint_dir):
            checkpoint_path = os.path.join(
                checkpoint_dir, "checkpoint-{}".format(self._iteration))
            with open(checkpoint_path, "wb") as f:
                pickle.dump(self.state, f)
            return checkpoint_path

        def _restore(self, checkpoint_path):
            with open(checkpoint_path, "rb") as f:
                extra_data = pickle.load(f)
            self.state.update(extra_data)

    def setUp(self):
        ray.init(num_cpus=1, num_gpus=0, local_mode=self.local_mode)

    def tearDown(self):
        shutil.rmtree(self.absolute_local_dir, ignore_errors=True)
        self.absolute_local_dir = None
        ray.shutdown()
        # Without this line, test_tune_server.testAddTrial would fail.
        _register_all()

    def _get_trial_dir(self, absoulte_exp_dir):
        print("looking for", self.MockTrainable._name)
        print("in", os.listdir(absoulte_exp_dir))
        trial_dirname = next(
            (child_dir for child_dir in os.listdir(absoulte_exp_dir)
             if (os.path.isdir(os.path.join(absoulte_exp_dir, child_dir))
                 and child_dir.startswith(self.MockTrainable._name))))

        trial_absolute_dir = os.path.join(absoulte_exp_dir, trial_dirname)

        return trial_dirname, trial_absolute_dir

    def _train(self, exp_name, local_dir, absolute_local_dir):
        trial, = tune.run(
            self.MockTrainable,
            name=exp_name,
            stop={
                "training_iteration": 1
            },
            checkpoint_freq=1,
            local_dir=local_dir,
            config={
                "env": "CartPole-v0",
                "log_level": "DEBUG"
            }).trials

        exp_dir = os.path.join(absolute_local_dir, exp_name)
        _, abs_trial_dir = self._get_trial_dir(exp_dir)

        self.assertIsNone(trial.error_file)
        self.assertEqual(trial.local_dir, exp_dir)
        self.assertEqual(trial.logdir, abs_trial_dir)

        self.assertTrue(os.path.isdir(absolute_local_dir), absolute_local_dir)
        self.assertTrue(os.path.isdir(exp_dir))
        self.assertTrue(os.path.isdir(abs_trial_dir))
        self.assertTrue(
            os.path.isfile(
                os.path.join(abs_trial_dir, "checkpoint_1/checkpoint-1")))

    def _restore(self, exp_name, local_dir, absolute_local_dir):
        trial_name, abs_trial_dir = self._get_trial_dir(
            os.path.join(absolute_local_dir, exp_name))

        checkpoint_path = os.path.join(
            local_dir, exp_name, trial_name,
            "checkpoint_1/checkpoint-1")  # Relative checkpoint path

        # The file tune would find. The absolute checkpoint path.
        tune_find_file = os.path.abspath(os.path.expanduser(checkpoint_path))
        self.assertTrue(
            os.path.isfile(tune_find_file),
            "{} is not exist!".format(tune_find_file))

        trial, = tune.run(
            self.MockTrainable,
            name=exp_name,
            stop={
                "training_iteration": 2
            },  # train one more iteration.
            restore=checkpoint_path,  # Restore the checkpoint
            config={
                "env": "CartPole-v0",
                "log_level": "DEBUG"
            }).trials
        self.assertIsNone(trial.error_file)

    def testDottedRelativePath(self):
        local_dir = "./test_dotted_relative_local_dir"
        exp_name = self.prefix + "DottedRelativeLocalDir"
        absolute_local_dir = os.path.abspath(local_dir)
        self.absolute_local_dir = absolute_local_dir
        self.assertFalse(os.path.exists(absolute_local_dir))
        self._train(exp_name, local_dir, absolute_local_dir)
        self._restore(exp_name, local_dir, absolute_local_dir)

    def testRelativePath(self):
        local_dir = "test_relative_local_dir"
        exp_name = self.prefix + "RelativePath"
        absolute_local_dir = os.path.abspath(local_dir)
        self.absolute_local_dir = absolute_local_dir
        self.assertFalse(os.path.exists(absolute_local_dir))
        self._train(exp_name, local_dir, absolute_local_dir)
        self._restore(exp_name, local_dir, absolute_local_dir)

    def testTildeAbsolutePath(self):
        local_dir = "~/test_tilde_absolute_local_dir"
        exp_name = self.prefix + "TildeAbsolutePath"
        absolute_local_dir = os.path.abspath(os.path.expanduser(local_dir))
        self.absolute_local_dir = absolute_local_dir
        self.assertFalse(os.path.exists(absolute_local_dir))
        self._train(exp_name, local_dir, absolute_local_dir)
        self._restore(exp_name, local_dir, absolute_local_dir)

    def testTempfile(self):
        local_dir = tempfile.mkdtemp()
        exp_name = self.prefix + "Tempfile"
        self.absolute_local_dir = local_dir
        self._train(exp_name, local_dir, local_dir)
        self._restore(exp_name, local_dir, local_dir)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
