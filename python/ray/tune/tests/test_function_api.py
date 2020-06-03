import shutil

import copy
import os
import time
import unittest
from unittest.mock import patch

import ray
from ray.rllib import _register_all

from ray import tune
from ray.tune.function_runner import wrap_function
from ray.tune.result import TRAINING_ITERATION


class FunctionApiTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4, num_gpus=0, object_store_memory=150 * 1024 * 1024)

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def testFunctionNoCheckpointing(self):
        def train(config, checkpoint=None):
            for i in range(10):
                tune.report(test=i)

        wrapped = wrap_function(train)

        new_trainable = wrapped()
        result = new_trainable.train()
        checkpoint = new_trainable.save()
        new_trainable.stop()

        new_trainable2 = wrapped()
        new_trainable2.restore(checkpoint)
        result = new_trainable2.train()
        self.assertEquals(result[TRAINING_ITERATION], 1)
        checkpoint = new_trainable2.save()
        new_trainable2.stop()

    def testCheckpointFunction(self):
        def train(config, checkpoint=False):
            for i in range(10):
                tune.report(test=i)
            checkpoint_dir = tune.make_checkpoint_dir(step=10)
            checkpoint_path = os.path.join(checkpoint_dir, "hello")
            with open(checkpoint_path, "w") as f:
                f.write("hello")
            tune.save_checkpoint(checkpoint_path)
            print("finished")

        [trial] = tune.run(train).trials
        assert "hello" in trial.checkpoint.value
