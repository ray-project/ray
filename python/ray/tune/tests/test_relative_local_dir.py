# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import shutil
import unittest

import ray
from ray import tune
from ray.rllib import _register_all
from ray.tune.util import recursive_fnmatch


class TuneRelativeLocalDirTest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.current_dir = os.path.abspath(".")
        ray.init(num_cpus=1, num_gpus=0, local_mode=True)
        # _register_all()

    @classmethod
    def tearDownClass(self):
        ray.shutdown()

    def setUp(self):
        os.chdir(self.current_dir)  # Turn the current_dir back

    def tearDown(self):
        shutil.rmtree(self.absolute_local_dir)
        _register_all()

    def _get_trial_dir(self, exp_dir):
        for i in os.listdir(exp_dir):
            if i.startswith("PG") and os.path.isdir(os.path.join(exp_dir, i)):
                return i, os.path.join(exp_dir, i)
        return None, None

    def _train(self, exp_name, local_dir, absolute_local_dir):
        self.absolute_local_dir = absolute_local_dir

        tune.run(
            "PG",
            name=exp_name,
            stop={"training_iteration": 1},
            checkpoint_freq=1,
            local_dir=local_dir,
            config={
                "env": "CartPole-v0",
                "log_level": "FATAL"
            })

        exp_dir = os.path.join(absolute_local_dir, exp_name)
        _, abs_trial_dir = self._get_trial_dir(exp_dir)

        self.assertTrue(os.path.isdir(absolute_local_dir), absolute_local_dir)
        self.assertTrue(os.path.isdir(exp_dir))
        self.assertTrue(os.path.isdir(abs_trial_dir))
        self.assertTrue(
            os.path.isfile(
                os.path.join(abs_trial_dir, "checkpoint_1/checkpoint-1")))
        os.chdir(self.current_dir)

    def _restore(self, exp_name, local_dir, absolute_local_dir):
        trial_name, abs_trial_dir = self._get_trial_dir(
            os.path.join(absolute_local_dir, exp_name))

        checkpoint_path = os.path.join(
            local_dir, exp_name, trial_name,
            "checkpoint_1/checkpoint-1")  # Relative!

        # The file tune would found.
        tune_found_file = os.path.abspath(os.path.expanduser(checkpoint_path))
        self.assertTrue(
            os.path.isfile(tune_found_file),
            "{} is not exist!".format(tune_found_file))

        tune.run(
            "PG",
            name=exp_name,
            stop={"training_iteration": 2},  # train one more iteration.
            checkpoint_freq=1,
            restore=checkpoint_path,  # Restore the checkpoint
            config={
                "env": "CartPole-v0",
            },
        )

    def testDottedRelativePath(self):
        local_dir = "./test_dotted_relative_local_dir"
        exp_name = "DottedRelativeLocalDir"
        absolute_local_dir = os.path.abspath(local_dir)
        self._train(exp_name, local_dir, absolute_local_dir)
        self._restore(exp_name, local_dir, absolute_local_dir)

    def testRelativePath(self):
        local_dir = "test_relative_local_dir"
        exp_name = "RelativePath"
        absolute_local_dir = os.path.abspath(local_dir)
        self._train(exp_name, local_dir, absolute_local_dir)
        self._restore(exp_name, local_dir, absolute_local_dir)

    def testTildeAbsolutePath(self):
        local_dir = "~/test_tilde_absolute_local_dir"
        exp_name = "TildeAbsolutePath"
        absolute_local_dir = os.path.expanduser(local_dir)
        self._train(exp_name, local_dir, absolute_local_dir)
        self._restore(exp_name, local_dir, absolute_local_dir)

    def testAbsolutePath(self):
        local_dir = "~/test_absolute_local_dir"
        local_dir = os.path.expanduser(local_dir)
        exp_name = "AbsolutePath"
        self._train(exp_name, local_dir, local_dir)
        self._restore(exp_name, local_dir, local_dir)


if __name__ == "__main__":
    unittest.main(verbosity=2)
