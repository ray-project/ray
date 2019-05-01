# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import tempfile
import unittest
import glob

import ray
from ray import tune
from ray.rllib import _register_all


class TuneRestoreTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=1, num_gpus=0, local_mode=True)
        _register_all()
        tmpdir = tempfile.mkdtemp()
        test_name = "TuneRestoreTest"
        tune.run(
            "PG",
            name=test_name,
            stop={"training_iteration": 1},
            checkpoint_freq=1,
            local_dir=tmpdir,
            config={
                "env": "CartPole-v0",
            },
        )

        logdir = os.path.expanduser(os.path.join(tmpdir, test_name))
        self.logdir = logdir
        self.checkpoint_path = glob.glob(
            os.path.join(logdir, "**/checkpoint_1/checkpoint-1"),
            recursive=True)[0]

    def tearDown(self):
        import shutil
        shutil.rmtree(self.logdir)
        ray.shutdown()

    def testCheckpointPath(self):
        self.assertTrue(os.path.isfile(self.checkpoint_path))

    def testTuneRestore(self):
        tune.run(
            "PG",
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
