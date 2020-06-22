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

    def testFunctionRecurringSave(self):
        """This tests that save and restore are commutative."""

        def train(config, checkpoint=None):
            for step in range(10):
                if step % 3 == 0:
                    checkpoint_dir = tune.make_checkpoint_dir(step=step)
                    path = os.path.join(checkpoint_dir, "checkpoint")
                    with open(path, "w") as f:
                        f.write(json.dumps({"step": step}))
                    tune.save_checkpoint(path)
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
        def train(config, checkpoint=False):
            for i in range(10):
                tune.report(test=i)
            checkpoint_dir = tune.make_checkpoint_dir(step=10)
            checkpoint_path = os.path.join(checkpoint_dir, "hello")
            with open(checkpoint_path, "w") as f:
                f.write("hello")
            tune.save_checkpoint(checkpoint_path)

        [trial] = tune.run(train).trials
        assert "hello" in trial.checkpoint.value

    def testVariousCheckpointFunctionAtEnd(self):
        def train(config, checkpoint=False):
            for i in range(10):
                checkpoint_dir = tune.make_checkpoint_dir()
                checkpoint_path = os.path.join(checkpoint_dir, "hello")
                with open(checkpoint_path, "w") as f:
                    f.write("hello")
                tune.save_checkpoint(checkpoint_path)
                tune.report(test=i)
            checkpoint_dir = tune.make_checkpoint_dir()
            checkpoint_path = os.path.join(checkpoint_dir, "goodbye")
            with open(checkpoint_path, "w") as f:
                f.write("goodbye")
            tune.save_checkpoint(checkpoint_path)

        [trial] = tune.run(train, keep_checkpoints_num=3).trials
        assert "goodbye" in trial.checkpoint.value

    def testReuseCheckpoint(self):
        def train(config, checkpoint=False):
            itr = 0
            if checkpoint:
                with open(checkpoint, "r") as f:
                    itr = int(f.read()) + 1

            for i in range(itr, config["max_iter"]):
                checkpoint_dir = tune.make_checkpoint_dir(step=i)
                checkpoint_path = os.path.join(checkpoint_dir, "goodbye")
                with open(checkpoint_path, "w") as f:
                    f.write(str(i))
                tune.save_checkpoint(checkpoint_path)
                tune.report(test=i, training_iteration=i)

        [trial] = tune.run(
            train,
            config={
                "max_iter": 5
            },
        ).trials
        last_ckpt = trial.checkpoint.value
        assert "goodbye" in last_ckpt
        analysis = tune.run(train, config={"max_iter": 10}, restore=last_ckpt)
        trial_dfs = list(analysis.trial_dataframes.values())
        assert len(trial_dfs[0]["training_iteration"]) == 5

    def testRetry(self):
        def train(config, checkpoint=None):
            restored = bool(checkpoint)
            itr = 0
            if checkpoint:
                with open(checkpoint, "r") as f:
                    itr = int(f.read()) + 1

            for i in range(itr, 10):
                if i == 5 and not restored:
                    raise Exception("try to fail me")
                checkpoint_dir = tune.make_checkpoint_dir(step=i)
                checkpoint_path = os.path.join(checkpoint_dir, "goodbye")
                with open(checkpoint_path, "w") as f:
                    f.write(str(i))
                tune.save_checkpoint(checkpoint_path)
                tune.report(test=i, training_iteration=i)

        analysis = tune.run(train, max_failures=3)
        last_ckpt = analysis.trials[0].checkpoint.value
        assert "goodbye" in last_ckpt
        trial_dfs = list(analysis.trial_dataframes.values())
        assert len(trial_dfs[0]["training_iteration"]) == 10

    def testBlankCheckpoint(self):
        def train(config, checkpoint=None):
            restored = bool(checkpoint)
            itr = 0
            if checkpoint:
                with open(checkpoint, "r") as f:
                    itr = int(f.read()) + 1

            for i in range(itr, 10):
                if i == 5 and not restored:
                    raise Exception("try to fail me")
                checkpoint_dir = tune.make_checkpoint_dir()
                checkpoint_path = os.path.join(checkpoint_dir, "goodbye")
                with open(checkpoint_path, "w") as f:
                    f.write(str(i))
                tune.save_checkpoint(checkpoint_path)
                tune.report(test=i, training_iteration=i)

        analysis = tune.run(train, max_failures=3)
        trial_dfs = list(analysis.trial_dataframes.values())
        assert len(trial_dfs[0]["training_iteration"]) == 10
