import json
import os
import unittest

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
        def train(config, checkpoint_dir=None):
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

    def testFunctionRecurringSave(self):
        """This tests that save and restore are commutative."""

        def train(config, checkpoint_dir=None):
            for step in range(10):
                if step % 3 == 0:
                    with tune.checkpoint_dir(step=step) as checkpoint_dir:
                        path = os.path.join(checkpoint_dir, "checkpoint")
                        with open(path, "w") as f:
                            f.write(json.dumps({"step": step}))
                tune.report(test=step)

        wrapped = wrap_function(train)

        new_trainable = wrapped()
        new_trainable.train()
        checkpoint_obj = new_trainable.save_to_object()
        new_trainable.restore_from_object(checkpoint_obj)
        checkpoint = new_trainable.save()
        new_trainable.stop()

        new_trainable2 = wrapped()
        new_trainable2.restore(checkpoint)
        new_trainable2.train()
        new_trainable2.stop()

    def testCheckpointFunctionAtEnd(self):
        def train(config, checkpoint_dir=False):
            for i in range(10):
                tune.report(test=i)
            with tune.checkpoint_dir(step=10) as checkpoint_dir:
                checkpoint_path = os.path.join(checkpoint_dir, "ckpt.log")
                with open(checkpoint_path, "w") as f:
                    f.write("hello")

        [trial] = tune.run(train).trials
        assert os.path.exists(os.path.join(trial.checkpoint.value, "ckpt.log"))

    def testCheckpointFunctionAtEndContext(self):
        def train(config, checkpoint_dir=False):
            for i in range(10):
                tune.report(test=i)
            with tune.checkpoint_dir(step=10) as checkpoint_dir:
                checkpoint_path = os.path.join(checkpoint_dir, "ckpt.log")
                with open(checkpoint_path, "w") as f:
                    f.write("hello")

        [trial] = tune.run(train).trials
        assert os.path.exists(os.path.join(trial.checkpoint.value, "ckpt.log"))

    def testVariousCheckpointFunctionAtEnd(self):
        def train(config, checkpoint_dir=False):
            for i in range(10):
                with tune.checkpoint_dir() as checkpoint_dir:
                    checkpoint_path = os.path.join(checkpoint_dir, "ckpt.log")
                    with open(checkpoint_path, "w") as f:
                        f.write("hello")
                tune.report(test=i)
            with tune.checkpoint_dir() as checkpoint_dir:
                checkpoint_path = os.path.join(checkpoint_dir, "ckpt.log2")
                with open(checkpoint_path, "w") as f:
                    f.write("goodbye")

        [trial] = tune.run(train, keep_checkpoints_num=3).trials
        assert os.path.exists(
            os.path.join(trial.checkpoint.value, "ckpt.log2"))

    def testReuseCheckpoint(self):
        def train(config, checkpoint_dir=None):
            itr = 0
            if checkpoint_dir:
                with open(os.path.join(checkpoint_dir, "ckpt.log"), "r") as f:
                    itr = int(f.read()) + 1

            for i in range(itr, config["max_iter"]):
                with tune.checkpoint_dir(step=i) as checkpoint_dir:
                    checkpoint_path = os.path.join(checkpoint_dir, "ckpt.log")
                    with open(checkpoint_path, "w") as f:
                        f.write(str(i))
                tune.report(test=i, training_iteration=i)

        [trial] = tune.run(
            train,
            config={
                "max_iter": 5
            },
        ).trials
        last_ckpt = trial.checkpoint.value
        assert os.path.exists(os.path.join(trial.checkpoint.value, "ckpt.log"))
        analysis = tune.run(train, config={"max_iter": 10}, restore=last_ckpt)
        trial_dfs = list(analysis.trial_dataframes.values())
        assert len(trial_dfs[0]["training_iteration"]) == 5

    def testRetry(self):
        def train(config, checkpoint_dir=None):
            restored = bool(checkpoint_dir)
            itr = 0
            if checkpoint_dir:
                with open(os.path.join(checkpoint_dir, "ckpt.log"), "r") as f:
                    itr = int(f.read()) + 1

            for i in range(itr, 10):
                if i == 5 and not restored:
                    raise Exception("try to fail me")
                with tune.checkpoint_dir(step=i) as checkpoint_dir:
                    checkpoint_path = os.path.join(checkpoint_dir, "ckpt.log")
                    with open(checkpoint_path, "w") as f:
                        f.write(str(i))
                tune.report(test=i, training_iteration=i)

        analysis = tune.run(train, max_failures=3)
        last_ckpt = analysis.trials[0].checkpoint.value
        assert os.path.exists(os.path.join(last_ckpt, "ckpt.log"))
        trial_dfs = list(analysis.trial_dataframes.values())
        assert len(trial_dfs[0]["training_iteration"]) == 10

    def testBlankCheckpoint(self):
        def train(config, checkpoint_dir=None):
            restored = bool(checkpoint_dir)
            itr = 0
            if checkpoint_dir:
                with open(os.path.join(checkpoint_dir, "ckpt.log"), "r") as f:
                    itr = int(f.read()) + 1

            for i in range(itr, 10):
                if i == 5 and not restored:
                    raise Exception("try to fail me")
                with tune.checkpoint_dir() as checkpoint_dir:
                    checkpoint_path = os.path.join(checkpoint_dir, "ckpt.log")
                    with open(checkpoint_path, "w") as f:
                        f.write(str(i))
                tune.report(test=i, training_iteration=i)

        analysis = tune.run(train, max_failures=3)
        trial_dfs = list(analysis.trial_dataframes.values())
        assert len(trial_dfs[0]["training_iteration"]) == 10
