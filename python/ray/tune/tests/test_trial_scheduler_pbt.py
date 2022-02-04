import numpy as np
import os
import pickle
import random
import unittest
import sys
import time
from unittest.mock import MagicMock

import ray
from ray import tune
from ray.tune import Trainable
from ray.tune.trial import Trial, Checkpoint
from ray.tune.trial_runner import TrialRunner
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.schedulers import PopulationBasedTraining
from ray._private.test_utils import object_memory_usage

# Import psutil after ray so the packaged version is used.
import psutil

MB = 1024 ** 2


class MockParam(object):
    def __init__(self, params):
        self._params = params
        self._index = 0

    def __call__(self, *args, **kwargs):
        val = self._params[self._index % len(self._params)]
        self._index += 1
        return val


class PopulationBasedTrainingMemoryTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=1, object_store_memory=100 * MB)

    def tearDown(self):
        ray.shutdown()

    def testMemoryCheckpointFree(self):
        class MyTrainable(Trainable):
            def setup(self, config):
                # Make sure this is large enough so ray uses object store
                # instead of in-process store.
                self.large_object = random.getrandbits(int(10e6))
                self.iter = 0
                self.a = config["a"]

            def step(self):
                self.iter += 1
                return {"metric": self.iter + self.a}

            def save_checkpoint(self, checkpoint_dir):
                file_path = os.path.join(checkpoint_dir, "model.mock")

                with open(file_path, "wb") as fp:
                    pickle.dump((self.large_object, self.iter, self.a), fp)
                return file_path

            def load_checkpoint(self, path):
                with open(path, "rb") as fp:
                    self.large_object, self.iter, self.a = pickle.load(fp)

        class CustomExecutor(RayTrialExecutor):
            def save(self, *args, **kwargs):
                checkpoint = super(CustomExecutor, self).save(*args, **kwargs)
                assert object_memory_usage() <= (12 * 80e6)
                return checkpoint

        param_a = MockParam([1, -1])

        pbt = PopulationBasedTraining(
            time_attr="training_iteration",
            metric="metric",
            mode="max",
            perturbation_interval=1,
            hyperparam_mutations={"b": [-1]},
        )

        tune.run(
            MyTrainable,
            name="ray_demo",
            scheduler=pbt,
            stop={"training_iteration": 10},
            num_samples=3,
            checkpoint_freq=1,
            fail_fast=True,
            config={"a": tune.sample_from(lambda _: param_a())},
            trial_executor=CustomExecutor(reuse_actors=False),
        )


class PopulationBasedTrainingFileDescriptorTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=2)
        os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "0"

    def tearDown(self):
        ray.shutdown()

    def testFileFree(self):
        class MyTrainable(Trainable):
            def setup(self, config):
                self.iter = 0
                self.a = config["a"]

            def step(self):
                self.iter += 1
                return {"metric": self.iter + self.a}

            def save_checkpoint(self, checkpoint_dir):
                file_path = os.path.join(checkpoint_dir, "model.mock")

                with open(file_path, "wb") as fp:
                    pickle.dump((self.iter, self.a), fp)
                return file_path

            def load_checkpoint(self, path):
                with open(path, "rb") as fp:
                    self.iter, self.a = pickle.load(fp)

        from ray.tune.callback import Callback

        class FileCheck(Callback):
            def __init__(self, verbose=False):
                self.iter_ = 0
                self.process = psutil.Process()
                self.verbose = verbose

            def on_trial_result(self, *args, **kwargs):
                self.iter_ += 1
                all_files = self.process.open_files()
                if self.verbose:
                    print("Iteration", self.iter_)
                    print("=" * 10)
                    print("Object memory use: ", object_memory_usage())
                    print("Virtual Mem:", self.get_virt_mem() >> 30, "gb")
                    print("File Descriptors:", len(all_files))
                assert len(all_files) < 20

            @classmethod
            def get_virt_mem(cls):
                return psutil.virtual_memory().used

        param_a = MockParam([1, -1])

        pbt = PopulationBasedTraining(
            time_attr="training_iteration",
            metric="metric",
            mode="max",
            perturbation_interval=1,
            quantile_fraction=0.5,
            hyperparam_mutations={"b": [-1]},
        )

        tune.run(
            MyTrainable,
            name="ray_demo",
            scheduler=pbt,
            stop={"training_iteration": 10},
            num_samples=4,
            checkpoint_freq=2,
            keep_checkpoints_num=1,
            verbose=False,
            fail_fast=True,
            config={"a": tune.sample_from(lambda _: param_a())},
            callbacks=[FileCheck()],
        )


class PopulationBasedTrainingSynchTest(unittest.TestCase):
    def setUp(self):
        os.environ["TUNE_TRIAL_STARTUP_GRACE_PERIOD"] = "0"
        ray.init(num_cpus=2)

        def MockTrainingFuncSync(config, checkpoint_dir=None):
            iter = 0

            if checkpoint_dir:
                checkpoint_path = os.path.join(checkpoint_dir, "checkpoint")
                with open(checkpoint_path, "rb") as fp:
                    a, iter = pickle.load(fp)

            a = config["a"]  # Use the new hyperparameter if perturbed.

            while True:
                iter += 1
                with tune.checkpoint_dir(step=iter) as checkpoint_dir:
                    checkpoint_path = os.path.join(checkpoint_dir, "checkpoint")
                    with open(checkpoint_path, "wb") as fp:
                        pickle.dump((a, iter), fp)
                # Different sleep times so that asynch test runs do not
                # randomly succeed. If well performing trials finish later,
                # then bad performing trials will already have continued
                # to train, which is exactly what we want to test when
                # comparing sync vs. async.
                time.sleep(a / 20)
                # Score gets better every iteration.
                tune.report(mean_accuracy=iter + a, a=a)

        self.MockTrainingFuncSync = MockTrainingFuncSync

    def tearDown(self):
        ray.shutdown()

    def synchSetup(self, synch, param=None):
        if param is None:
            param = [10, 20, 40]

        scheduler = PopulationBasedTraining(
            time_attr="training_iteration",
            metric="mean_accuracy",
            mode="max",
            perturbation_interval=1,
            log_config=True,
            hyperparam_mutations={"c": lambda: 1},
            synch=synch,
        )

        param_a = MockParam(param)

        random.seed(100)
        np.random.seed(100)
        analysis = tune.run(
            self.MockTrainingFuncSync,
            config={"a": tune.sample_from(lambda _: param_a()), "c": 1},
            fail_fast=True,
            num_samples=3,
            scheduler=scheduler,
            name="testPBTSync",
            stop={"training_iteration": 3},
        )
        return analysis

    def testAsynchFail(self):
        analysis = self.synchSetup(False)
        self.assertTrue(
            any(
                analysis.dataframe(metric="mean_accuracy", mode="max")["mean_accuracy"]
                != 43
            )
        )

    def testSynchPass(self):
        analysis = self.synchSetup(True)
        self.assertTrue(
            all(
                analysis.dataframe(metric="mean_accuracy", mode="max")["mean_accuracy"]
                == 43
            )
        )

    def testSynchPassLast(self):
        analysis = self.synchSetup(True, param=[30, 20, 10])
        self.assertTrue(
            all(
                analysis.dataframe(metric="mean_accuracy", mode="max")["mean_accuracy"]
                == 33
            )
        )


class PopulationBasedTrainingConfigTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=2)

    def tearDown(self):
        ray.shutdown()

    def testNoConfig(self):
        def MockTrainingFunc(config):
            a = config["a"]
            b = config["b"]
            c1 = config["c"]["c1"]
            c2 = config["c"]["c2"]

            while True:
                tune.report(mean_accuracy=a * b * (c1 + c2))

        scheduler = PopulationBasedTraining(
            time_attr="training_iteration",
            metric="mean_accuracy",
            mode="max",
            perturbation_interval=1,
            hyperparam_mutations={
                "a": tune.uniform(0, 0.3),
                "b": [1, 2, 3],
                "c": {
                    "c1": lambda: np.random.uniform(0.5),
                    "c2": tune.choice([2, 3, 4]),
                },
            },
        )

        tune.run(
            MockTrainingFunc,
            fail_fast=True,
            num_samples=4,
            scheduler=scheduler,
            name="testNoConfig",
            stop={"training_iteration": 3},
        )


class PopulationBasedTrainingResumeTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=2)

    def tearDown(self):
        ray.shutdown()

    def testPermutationContinuation(self):
        """
        Tests continuation of runs after permutation.
        Sometimes, runs were continued from deleted checkpoints.
        This deterministic initialisation would fail when the
        fix was not applied.
        See issues #9036, #9036
        """

        class MockTrainable(tune.Trainable):
            def setup(self, config):
                self.iter = 0
                self.a = config["a"]
                self.b = config["b"]
                self.c = config["c"]

            def step(self):
                self.iter += 1
                return {"mean_accuracy": (self.a - self.iter) * self.b}

            def save_checkpoint(self, tmp_checkpoint_dir):
                checkpoint_path = os.path.join(tmp_checkpoint_dir, "model.mock")
                with open(checkpoint_path, "wb") as fp:
                    pickle.dump((self.a, self.b, self.iter), fp)
                return tmp_checkpoint_dir

            def load_checkpoint(self, tmp_checkpoint_dir):
                checkpoint_path = os.path.join(tmp_checkpoint_dir, "model.mock")
                with open(checkpoint_path, "rb") as fp:
                    self.a, self.b, self.iter = pickle.load(fp)

        scheduler = PopulationBasedTraining(
            time_attr="training_iteration",
            metric="mean_accuracy",
            mode="max",
            perturbation_interval=1,
            log_config=True,
            hyperparam_mutations={"c": lambda: 1},
        )

        param_a = MockParam([10, 20, 30, 40])
        param_b = MockParam([1.2, 0.9, 1.1, 0.8])

        random.seed(100)
        np.random.seed(1000)
        tune.run(
            MockTrainable,
            config={
                "a": tune.sample_from(lambda _: param_a()),
                "b": tune.sample_from(lambda _: param_b()),
                "c": 1,
            },
            fail_fast=True,
            num_samples=4,
            checkpoint_freq=1,
            checkpoint_at_end=True,
            keep_checkpoints_num=1,
            checkpoint_score_attr="min-training_iteration",
            scheduler=scheduler,
            name="testPermutationContinuation",
            stop={"training_iteration": 3},
        )

    def testPermutationContinuationFunc(self):
        def MockTrainingFunc(config, checkpoint_dir=None):
            iter = 0
            a = config["a"]
            b = config["b"]

            if checkpoint_dir:
                checkpoint_path = os.path.join(checkpoint_dir, "model.mock")
                with open(checkpoint_path, "rb") as fp:
                    a, b, iter = pickle.load(fp)

            while True:
                iter += 1
                with tune.checkpoint_dir(step=iter) as checkpoint_dir:
                    checkpoint_path = os.path.join(checkpoint_dir, "model.mock")
                    with open(checkpoint_path, "wb") as fp:
                        pickle.dump((a, b, iter), fp)
                tune.report(mean_accuracy=(a - iter) * b)

        scheduler = PopulationBasedTraining(
            time_attr="training_iteration",
            metric="mean_accuracy",
            mode="max",
            perturbation_interval=1,
            log_config=True,
            hyperparam_mutations={"c": lambda: 1},
        )
        param_a = MockParam([10, 20, 30, 40])
        param_b = MockParam([1.2, 0.9, 1.1, 0.8])
        random.seed(100)
        np.random.seed(1000)
        tune.run(
            MockTrainingFunc,
            config={
                "a": tune.sample_from(lambda _: param_a()),
                "b": tune.sample_from(lambda _: param_b()),
                "c": 1,
            },
            fail_fast=True,
            num_samples=4,
            keep_checkpoints_num=1,
            checkpoint_score_attr="min-training_iteration",
            scheduler=scheduler,
            name="testPermutationContinuationFunc",
            stop={"training_iteration": 3},
        )

    def testBurnInPeriod(self):
        runner = TrialRunner(trial_executor=MagicMock())

        scheduler = PopulationBasedTraining(
            time_attr="training_iteration",
            metric="error",
            mode="min",
            perturbation_interval=5,
            burn_in_period=50,
            log_config=True,
            synch=True,
        )

        class MockTrial(Trial):
            @property
            def checkpoint(self):
                return Checkpoint(Checkpoint.MEMORY, "None", {})

            @property
            def status(self):
                return Trial.PAUSED

            @status.setter
            def status(self, status):
                pass

        trial1 = MockTrial("PPO", config=dict(num=1))
        trial2 = MockTrial("PPO", config=dict(num=2))
        trial3 = MockTrial("PPO", config=dict(num=3))
        trial4 = MockTrial("PPO", config=dict(num=4))

        runner.add_trial(trial1)
        runner.add_trial(trial2)
        runner.add_trial(trial3)
        runner.add_trial(trial4)

        scheduler.on_trial_add(runner, trial1)
        scheduler.on_trial_add(runner, trial2)
        scheduler.on_trial_add(runner, trial3)
        scheduler.on_trial_add(runner, trial4)

        # Add initial results.
        scheduler.on_trial_result(
            runner, trial1, result=dict(training_iteration=1, error=50)
        )
        scheduler.on_trial_result(
            runner, trial2, result=dict(training_iteration=1, error=50)
        )
        scheduler.on_trial_result(
            runner, trial3, result=dict(training_iteration=1, error=10)
        )
        scheduler.on_trial_result(
            runner, trial4, result=dict(training_iteration=1, error=100)
        )

        # Add more results. Without burn-in, this would now exploit
        scheduler.on_trial_result(
            runner, trial1, result=dict(training_iteration=30, error=50)
        )
        scheduler.on_trial_result(
            runner, trial2, result=dict(training_iteration=30, error=50)
        )
        scheduler.on_trial_result(
            runner, trial3, result=dict(training_iteration=30, error=10)
        )
        scheduler.on_trial_result(
            runner, trial4, result=dict(training_iteration=30, error=100)
        )

        self.assertEqual(trial4.config["num"], 4)

        # Add more results. Since this is after burn-in, it should now exploit
        scheduler.on_trial_result(
            runner, trial1, result=dict(training_iteration=50, error=50)
        )
        scheduler.on_trial_result(
            runner, trial2, result=dict(training_iteration=50, error=50)
        )
        scheduler.on_trial_result(
            runner, trial3, result=dict(training_iteration=50, error=10)
        )
        scheduler.on_trial_result(
            runner, trial4, result=dict(training_iteration=50, error=100)
        )

        self.assertEqual(trial4.config["num"], 3)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
