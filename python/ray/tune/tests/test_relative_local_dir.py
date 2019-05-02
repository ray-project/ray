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


class TuneRelativeLocalDirTest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.current_dir = os.path.abspath(".")
        ray.init(num_cpus=1, num_gpus=0, local_mode=True)
        _register_all()

    @classmethod
    def tearDownClass(self):
        ray.shutdown()

    def tearDown(self):
        os.chdir(self.current_dir) # Turn the current_dir back

    def testDottedRelativePath(self):
        local_dir = "test_dotted_relative_local_dir"
        local_dir = os.path.join(self.current_dir, local_dir)

        tune.run(
            "PG",
            name="TuneDottedRelativeLocalDirTest",
            stop={"training_iteration": 1},
            checkpoint_freq=1,
            local_dir="./test_dotted_relative_local_dir",
            config={
                "env": "CartPole-v0",
            })

        self.assertTrue(os.path.isdir(local_dir))
        expdir = os.path.join(local_dir, "TuneDottedRelativeLocalDirTest")
        self.assertTrue(os.path.isdir(expdir))
        trial_dir = None
        for i in os.listdir(expdir):
            if i.startswith("PG") and os.path.isdir(
                    os.path.join(expdir, i)):
                trial_dir = os.path.join(expdir, i)
                break
        self.assertTrue(
            os.path.isfile(
                os.path.join(trial_dir, "checkpoint_1/checkpoint-1")))
        shutil.rmtree(local_dir)

    def testRelativePath(self):
        local_dir = "test_relative_local_dir"
        local_dir = os.path.join(self.current_dir, local_dir)

        tune.run(
            "PG",
            name="TuneRelativeLocalDirTest",
            stop={"training_iteration": 1},
            checkpoint_freq=1,
            local_dir="test_relative_local_dir",
            config={
                "env": "CartPole-v0",
            })

        self.assertTrue(os.path.isdir(local_dir))
        expdir = os.path.join(local_dir, "TuneRelativeLocalDirTest")
        self.assertTrue(os.path.isdir(expdir))

        for i in os.listdir(expdir):
            if i.startswith("PG") and os.path.isdir(
                    os.path.join(expdir, i)):
                expdir = os.path.join(expdir, i)
                break
        self.assertTrue(
            os.path.isfile(os.path.join(expdir, "checkpoint_1/checkpoint-1")))
        shutil.rmtree(local_dir)

    def testAbsolutePath(self):
        local_dir = "~/test_tilde_absolute_local_dir"
        local_dir = os.path.expanduser(local_dir)

        tune.run(
            "PG",
            name="TuneAbsoluteLocalDirTest",
            stop={"training_iteration": 1},
            checkpoint_freq=1,
            local_dir=local_dir,
            config={
                "env": "CartPole-v0",
            })

        self.assertTrue(os.path.isdir(local_dir))
        expdir = os.path.join(local_dir, "TuneAbsoluteLocalDirTest")
        self.assertTrue(os.path.isdir(expdir))

        for i in os.listdir(expdir):
            if i.startswith("PG") \
                    and os.path.isdir(os.path.join(expdir, i)):
                expdir = os.path.join(expdir, i)
                break
        self.assertTrue(
            os.path.isfile(os.path.join(expdir, "checkpoint_1/checkpoint-1")))
        shutil.rmtree(local_dir)

    def testTildeAbsolutePath(self):
        local_dir = "~/test_tilde_absolute_local_dir"
        local_dir = os.path.expanduser(local_dir)

        tune.run(
            "PG",
            name="TildeAbsolutePath",
            stop={"training_iteration": 1},
            checkpoint_freq=1,
            local_dir="~/test_tilde_absolute_local_dir",
            config={
                "env": "CartPole-v0",
            })

        self.assertTrue(os.path.isdir(local_dir))
        expdir = os.path.join(local_dir, "TildeAbsolutePath")
        self.assertTrue(os.path.isdir(expdir))

        for i in os.listdir(expdir):
            if i.startswith("PG") and os.path.isdir(
                    os.path.join(expdir, i)):
                expdir = os.path.join(expdir, i)
                break
        self.assertTrue(
            os.path.isfile(os.path.join(expdir, "checkpoint_1/checkpoint-1")))
        shutil.rmtree(local_dir)


if __name__ == "__main__":
    unittest.main(verbosity=2)
