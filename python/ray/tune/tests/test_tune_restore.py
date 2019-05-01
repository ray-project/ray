# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import unittest

import ray
from ray import tune
from ray.rllib.agents.registry import get_agent_class
from ray.tune.registry import register_trainable
from ray.tune.trial import has_trainable


class TuneRestoreTest(unittest.TestCase):
    def setUp(self):
        self.algo = 'PG'

        register_trainable(self.algo, get_agent_class(self.algo))
        self.assertTrue(has_trainable(self.algo))

        ray.init(num_cpus=1, num_gpus=0, local_mode=True)
        tune.run(
            self.algo,
            name="TuneRestoreTest",
            stop={"training_iteration": 1},
            checkpoint_freq=1,
            config={
                "env": "CartPole-v0",
            },
        )

        logdir = os.path.expanduser("~/ray_results/TuneRestoreTest/")
        expdir = None
        for i in os.listdir(logdir):
            if i.startswith(self.algo) and os.path.isdir(os.path.join(logdir, i)):
                expdir = os.path.join(logdir, i)
                break
        self.logdir = logdir  # should like: /user/../ray_results/TuneRestoreTest
        self.expdir = expdir  # should like: /user/../ray_results/TuneRestoreTest/PG_CartPole_codes
        self.checkpoint_path = os.path.join(self.expdir, "checkpoint_1/checkpoint-1")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.logdir)
        ray.shutdown()

    def testCheckpointPath(self):
        self.assertIsNotNone(self.expdir)
        self.assertTrue(os.path.isfile(self.checkpoint_path))

    def testTuneRestore(self):
        self.assertTrue(has_trainable(self.algo))
        tune.run(
            self.algo,
            name="TuneRestoreTest",
            stop={"training_iteration": 2},  # train one more iteration.
            checkpoint_freq=1,
            restore=self.checkpoint_path,  # Restore the checkpoint
            config={
                "env": "CartPole-v0",
            },
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
