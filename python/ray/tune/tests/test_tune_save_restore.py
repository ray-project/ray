# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pickle
import shutil
import tempfile
import unittest

import ray
from ray import tune
from ray.tune import Trainable


class SerialTuneRelativeLocalDirTest(unittest.TestCase):
    local_mode = True
    prefix = "Serial"
    current_dir = os.path.abspath(".")

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
        os.chdir(self.current_dir)  # Turn the current_dir back
        ray.init(num_cpus=1, num_gpus=0, local_mode=self.local_mode)

    def tearDown(self):
        ray.shutdown()

    def _get_trial_dir(self, exp_dir):
        for i in os.listdir(exp_dir):
            if i.startswith(self.MockTrainable._name) and os.path.isdir(
                    os.path.join(exp_dir, i)):
                return i, os.path.join(exp_dir, i)
        return None, None

    def _train(self, exp_name, local_dir, absolute_local_dir):
        shutil.rmtree(absolute_local_dir, ignore_errors=True)
        trial, = tune.run(
            self.MockTrainable,
            name=exp_name,
            stop={"training_iteration": 1},
            checkpoint_freq=1,
            local_dir=local_dir,
            config={
                "env": "CartPole-v0",
                "log_level": "DEBUG"
            })

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
        tune_found_file = os.path.abspath(os.path.expanduser(checkpoint_path))
        self.assertTrue(
            os.path.isfile(tune_found_file),
            "{} is not exist!".format(tune_found_file))

        trial, = tune.run(
            self.MockTrainable,
            name=exp_name,
            stop={"training_iteration": 2},  # train one more iteration.
            restore=checkpoint_path,  # Restore the checkpoint
            config={
                "env": "CartPole-v0",
                "log_level": "DEBUG"
            })
        self.assertIsNone(trial.error_file)
        shutil.rmtree(absolute_local_dir, ignore_errors=True)

    def testDottedRelativePath(self):
        local_dir = "./test_dotted_relative_local_dir"
        exp_name = self.prefix + "DottedRelativeLocalDir"
        absolute_local_dir = os.path.abspath(local_dir)
        self._train(exp_name, local_dir, absolute_local_dir)
        os.chdir(self.current_dir)
        self._restore(exp_name, local_dir, absolute_local_dir)

    def testRelativePath(self):
        local_dir = "test_relative_local_dir"
        exp_name = self.prefix + "RelativePath"
        absolute_local_dir = os.path.abspath(local_dir)
        self._train(exp_name, local_dir, absolute_local_dir)
        os.chdir(self.current_dir)
        self._restore(exp_name, local_dir, absolute_local_dir)

    def testTildeAbsolutePath(self):
        local_dir = "~/test_tilde_absolute_local_dir"
        exp_name = self.prefix + "TildeAbsolutePath"
        absolute_local_dir = os.path.expanduser(local_dir)
        self._train(exp_name, local_dir, absolute_local_dir)
        os.chdir(self.current_dir)
        self._restore(exp_name, local_dir, absolute_local_dir)

    def testAbsolutePath(self):
        local_dir = "~/test_absolute_local_dir"
        local_dir = os.path.expanduser(local_dir)
        exp_name = self.prefix + "AbsolutePath"
        self._train(exp_name, local_dir, local_dir)
        os.chdir(self.current_dir)
        self._restore(exp_name, local_dir, local_dir)

    def testTempfile(self):
        local_dir = tempfile.mkdtemp()
        exp_name = self.prefix + "Tempfile"
        self._train(exp_name, local_dir, local_dir)
        os.chdir(self.current_dir)
        self._restore(exp_name, local_dir, local_dir)


class ParallelTuneRelativeLocalDirTest(SerialTuneRelativeLocalDirTest):
    local_mode = False
    prefix = "Parallel"


if __name__ == "__main__":
    unittest.main(verbosity=2)
