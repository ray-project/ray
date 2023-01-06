from functools import partial
import numpy as np
import os
import pickle
import pytest
import random
import unittest
import sys
import time
from unittest.mock import MagicMock


import ray
from ray import tune
from ray.air import Checkpoint
from ray.air._internal.checkpoint_manager import _TrackedCheckpoint, CheckpointStorage
from ray.air.config import FailureConfig, RunConfig, CheckpointConfig
from ray.tune import Trainable
from ray.tune.experiment import Trial
from ray.tune.execution.trial_runner import TrialRunner
from ray.tune.execution.ray_trial_executor import RayTrialExecutor
from ray.tune.schedulers import PopulationBasedTraining
from ray.tune.schedulers.pbt import _filter_mutated_params_from_config
from ray.tune.schedulers.pb2 import PB2
from ray.tune.schedulers.pb2_utils import UCB
from ray.tune.tune_config import TuneConfig
from ray._private.test_utils import object_memory_usage
from ray.tune.utils.util import flatten_dict


# Import psutil after ray so the packaged version is used.
import psutil

MB = 1024**2


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

    def testExploitWhileSavingTrial(self):
        """Tests a synch PBT failure mode where a trial misses its `SAVING_RESULT` event
        book-keeping due to being stopped by the PBT algorithm (to exploit another
        trial).

        Trials checkpoint ever N iterations, and the perturbation interval is every N
        iterations. (N = 2 in the test.)

        Raises a `TimeoutError` if hanging for a specified `timeout`.

        1. Trial 0 comes in with training result
        2. Trial 0 begins saving checkpoint (which may take a long time, 5s here)
        3. Trial 1 comes in with result
        4. Trial 1 forcefully stops Trial 0 via exploit, while trial_0.is_saving
        5. Trial 0 should resume training properly with Trial 1's checkpoint
        """

        class MockTrainable(tune.Trainable):
            def setup(self, config):
                self.reset_config(config)

            def step(self):
                time.sleep(self.training_time)
                return {"score": self.score}

            def save_checkpoint(self, checkpoint_dir):
                checkpoint = Checkpoint.from_dict({"a": self.a})
                checkpoint_path = checkpoint.to_directory(path=checkpoint_dir)
                time.sleep(self.saving_time)
                return checkpoint_path

            def load_checkpoint(self, checkpoint_dir):
                checkpoint_dict = Checkpoint.from_directory(checkpoint_dir).to_dict()
                self.a = checkpoint_dict["a"]

            def reset_config(self, new_config):
                self.a = new_config["a"]
                self.score = new_config["score"]
                self.training_time = new_config["training_time"]
                self.saving_time = new_config["saving_time"]
                return True

        perturbation_interval = 2
        scheduler = PopulationBasedTraining(
            time_attr="training_iteration",
            metric="score",
            mode="max",
            perturbation_interval=perturbation_interval,
            hyperparam_mutations={
                "a": tune.uniform(0, 1),
            },
            synch=True,
        )

        class TimeoutExceptionStopper(tune.stopper.TimeoutStopper):
            def stop_all(self):
                decision = super().stop_all()
                if decision:
                    raise TimeoutError("Trials are hanging! Timeout reached...")
                return decision

        timeout = 30.0
        training_times = [0.1, 0.15]
        saving_times = [5.0, 0.1]
        tuner = tune.Tuner(
            MockTrainable,
            param_space={
                "a": tune.uniform(0, 1),
                "score": tune.grid_search([0, 1]),
                "training_time": tune.sample_from(
                    lambda spec: training_times[spec.config["score"]]
                ),
                "saving_time": tune.sample_from(
                    lambda spec: saving_times[spec.config["score"]]
                ),
            },
            tune_config=TuneConfig(
                num_samples=1,
                scheduler=scheduler,
            ),
            run_config=RunConfig(
                stop=tune.stopper.CombinedStopper(
                    tune.stopper.MaximumIterationStopper(5),
                    TimeoutExceptionStopper(timeout),
                ),
                failure_config=FailureConfig(fail_fast=True),
                checkpoint_config=CheckpointConfig(
                    # Match `checkpoint_interval` with `perturbation_interval`
                    checkpoint_frequency=perturbation_interval,
                ),
            ),
        )
        random.seed(100)
        np.random.seed(1000)
        tuner.fit()


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
            hyperparam_mutations={"ignored": [1]},
            burn_in_period=50,
            log_config=True,
            synch=True,
        )

        class MockTrial(Trial):
            @property
            def checkpoint(self):
                return _TrackedCheckpoint(
                    dir_or_data={"data": "None"},
                    storage_mode=CheckpointStorage.MEMORY,
                    metrics={},
                )

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

        # Assert that trials do not hang after `burn_in_period`
        self.assertTrue(all(t.status == "PAUSED" for t in runner.get_trials()))
        self.assertTrue(scheduler.choose_trial_to_run(runner))

        # Assert that trials do not hang when a terminated trial is added
        trial5 = Trial("PPO", config=dict(num=5))
        runner.add_trial(trial5)
        scheduler.on_trial_add(runner, trial5)
        trial5.set_status(Trial.TERMINATED)
        self.assertTrue(scheduler.choose_trial_to_run(runner))


class PopulationBasedTrainingLoggingTest(unittest.TestCase):
    def testFilterHyperparamConfig(self):
        filtered_params = _filter_mutated_params_from_config(
            {
                "training_loop_config": {
                    "lr": 0.1,
                    "momentum": 0.9,
                    "batch_size": 32,
                    "test_mode": True,
                    "ignore_nested": {
                        "b": 0.1,
                    },
                },
                "other_config": {
                    "a": 0.5,
                },
            },
            {
                "training_loop_config": {
                    "lr": tune.uniform(0, 1),
                    "momentum": tune.uniform(0, 1),
                }
            },
        )
        assert filtered_params == {
            "training_loop_config": {"lr": 0.1, "momentum": 0.9}
        }, filtered_params

    def testSummarizeHyperparamChanges(self):
        class DummyTrial:
            def __init__(self, config):
                self.config = config

        def test_config(
            hyperparam_mutations,
            old_config,
            resample_probability=0.25,
            print_summary=False,
        ):
            scheduler = PopulationBasedTraining(
                time_attr="training_iteration",
                hyperparam_mutations=hyperparam_mutations,
                resample_probability=resample_probability,
            )
            new_config, operations = scheduler._get_new_config(
                None, DummyTrial(old_config)
            )

            old_params = _filter_mutated_params_from_config(
                old_config, hyperparam_mutations
            )
            new_params = _filter_mutated_params_from_config(
                new_config, hyperparam_mutations
            )

            summary = scheduler._summarize_hyperparam_changes(
                old_params, new_params, operations
            )
            if print_summary:
                print(summary)
            return scheduler, new_config, operations

        # 1. Empty hyperparam_mutations (no hyperparams mutated) should raise an error
        with self.assertRaises(tune.TuneError):
            _, new_config, operations = test_config({}, {})

        # 2. No nesting
        hyperparam_mutations = {
            "a": tune.uniform(0, 1),
            "b": list(range(5)),
        }
        scheduler, new_config, operations = test_config(
            hyperparam_mutations, {"a": 0.5, "b": 2}
        )
        assert operations["a"] in [
            f"* {factor}" for factor in scheduler._perturbation_factors
        ] + ["resample"]
        assert operations["b"] in ["shift left", "shift right", "resample"]

        # 3. With nesting
        hyperparam_mutations = {
            "a": tune.uniform(0, 1),
            "b": list(range(5)),
            "c": {
                "d": tune.uniform(2, 3),
                "e": {"f": [-1, 0, 1]},
            },
        }
        scheduler, new_config, operations = test_config(
            hyperparam_mutations,
            {
                "a": 0.5,
                "b": 2,
                "c": {
                    "d": 2.5,
                    "e": {"f": 0},
                },
            },
        )
        assert isinstance(operations["c"], dict)
        assert isinstance(operations["c"]["e"], dict)
        assert operations["c"]["d"] in [
            f"* {factor}" for factor in scheduler._perturbation_factors
        ] + ["resample"]
        assert operations["c"]["e"]["f"] in ["shift left", "shift right", "resample"]

        # 4. Test shift that results in noop
        hyperparam_mutations = {"a": [1]}
        scheduler, new_config, operations = test_config(
            hyperparam_mutations, {"a": 1}, resample_probability=0
        )
        assert operations["a"] in ["shift left (noop)", "shift right (noop)"]

        # 5. Test that missing keys in inputs raises an error
        with self.assertRaises(AssertionError):
            scheduler._summarize_hyperparam_changes(
                {"a": 1, "b": {"c": 2}},
                {"a": 1, "b": {}},
                {"a": "noop", "b": {"c": "noop"}},
            )
        # It's ok to have missing operations (just fill in the ones that are present)
        scheduler._summarize_hyperparam_changes(
            {"a": 1, "b": {"c": 2}}, {"a": 1, "b": {"c": 2}}, {"a": "noop"}
        )
        scheduler._summarize_hyperparam_changes(
            {"a": 1, "b": {"c": 2}}, {"a": 1, "b": {"c": 2}}, {}
        )

        # Make sure that perturbation and logging work with extra keys that aren't
        # included in hyperparam_mutations (both should ignore the keys)
        hyperparam_mutations = {
            "train_loop_config": {
                "lr": tune.uniform(0, 1),
                "momentum": tune.uniform(0, 1),
            }
        }
        test_config(
            hyperparam_mutations,
            {
                "train_loop_config": {
                    "lr": 0.1,
                    "momentum": 0.9,
                    "batch_size": 32,
                    "test_mode": True,
                }
            },
            resample_probability=0,
        )


def create_pb2_scheduler(
    metric="score",
    mode="max",
    perturbation_interval=1,
    hyperparam_bounds=None,
) -> PB2:
    hyperparam_bounds = hyperparam_bounds or {"a": [0.0, 1.0]}
    return PB2(
        metric=metric,
        mode=mode,
        time_attr="training_iteration",
        perturbation_interval=perturbation_interval,
        quantile_fraction=0.25,
        hyperparam_bounds=hyperparam_bounds,
    )


def save_trial_result(scheduler, trial, time, result):
    scheduler._save_trial_state(scheduler._trial_state[trial], time, result, trial)


def result(time, val):
    return {"training_iteration": time, "score": val}


def test_pb2_perturbation(monkeypatch):
    hyperparam_bounds = {"a": [1.0, 2.0]}
    pb2 = create_pb2_scheduler(
        metric="score", mode="max", hyperparam_bounds=hyperparam_bounds
    )

    mock_runner = MagicMock()

    # One trial at each end of the hyperparam bounds, one performing better than the
    # other. We expect a perturbed value to be closer to the better performing one.
    trials = [
        Trial("pb2_test", stub=True, config={"a": 1.0}),
        Trial("pb2_test", stub=True, config={"a": 2.0}),
    ]
    for trial in trials:
        pb2.on_trial_add(mock_runner, trial)

    # Collect 10 timesteps of data
    # PB2 fits a model to estimate the increase in score between timesteps
    # Each timestep, trial 1's score increases by 10, trial 2's score increases by 20
    for t in range(1, 11):
        for i, trial in enumerate(trials):
            save_trial_result(pb2, trial, t, result(time=t, val=t * (i + 1) * 10))

    # Ignoring variance (kappa=0) and only optimizing for exploitation,
    # we expect the next point suggested to be close to higher-performing trial
    monkeypatch.setattr(ray.tune.schedulers.pb2_utils, "UCB", partial(UCB, kappa=0.0))
    new_config, _ = pb2._get_new_config(trials[0], trials[1])
    assert new_config["a"] > 1.5
    assert pb2._quantiles() == ([trials[0]], [trials[1]])


def test_pb2_nested_hyperparams():
    """Test that PB2 with nested hyperparams behaves the same as without nesting."""
    hyperparam_bounds = {"a": [1.0, 2.0], "b": {"c": [2.0, 4.0], "d": [4.0, 10.0]}}
    pb2_nested = create_pb2_scheduler(
        metric="score",
        mode="max",
        hyperparam_bounds=hyperparam_bounds,
    )
    pb2_flat = create_pb2_scheduler(
        metric="score",
        mode="max",
        hyperparam_bounds=flatten_dict(hyperparam_bounds, delimiter=""),
    )

    mock_runner = MagicMock()

    trials_nested = [Trial("pb2_test", stub=True) for _ in range(3)]
    trials_flat = [Trial("pb2_test", stub=True) for _ in range(3)]

    np.random.seed(2023)

    for trial_nested, trial_flat in zip(trials_nested, trials_flat):
        pb2_nested.on_trial_add(mock_runner, trial_nested)
        # Let PB2 generate the initial config randomly, then use the same
        # initial values for the flattened version
        flattened_init_config = flatten_dict(trial_nested.config, delimiter="")
        trial_flat.config = flattened_init_config
        pb2_flat.on_trial_add(mock_runner, trial_flat)

    # Make sure that config suggestions are the same for each timestep
    for t in range(1, 10):
        for i, (trial_nested, trial_flat) in enumerate(zip(trials_nested, trials_flat)):
            res = result(time=t, val=t * (i + 1) * 10)
            save_trial_result(pb2_nested, trial_nested, t, res)
            save_trial_result(pb2_flat, trial_flat, t, res)

        new_config, _ = pb2_nested._get_new_config(trials_nested[0], trials_nested[-1])
        new_config_flat, _ = pb2_flat._get_new_config(trials_flat[0], trials_flat[-1])

        # Make sure the suggested config is still nested properly
        assert list(new_config.keys()) == ["a", "b"]
        assert list(new_config["b"].keys()) == ["c", "d"]
        assert np.allclose(
            list(flatten_dict(new_config, delimiter="").values()),
            list(new_config_flat.values()),
        )


def test_pb2_missing_hyperparam_init():
    """Test that PB2 fills in all missing hyperparameters (those that are not
    specified in param_space)."""
    hyperparam_bounds = {"a": [1.0, 2.0], "b": {"c": [2.0, 4.0], "d": [4.0, 10.0]}}
    pb2 = create_pb2_scheduler(hyperparam_bounds=hyperparam_bounds)
    mock_runner = MagicMock()

    def validate_config(config, bounds):
        for param, bound in bounds.items():
            if isinstance(bound, dict):
                validate_config(config[param], bound)
            else:
                low, high = bound
                assert config[param] >= low and config[param] < high

    trial = Trial("test_pb2", stub=True)
    pb2.on_trial_add(mock_runner, trial)
    validate_config(trial.config, hyperparam_bounds)

    trial = Trial("test_pb2", stub=True, config={"b": {"c": 3.0}})
    pb2.on_trial_add(mock_runner, trial)
    validate_config(trial.config, hyperparam_bounds)
    assert trial.config["b"]["c"] == 3.0


def test_pb2_hyperparam_bounds_validation():
    hyperparam_bounds = {"a": [1.0, 2.0], "b": {"c": [2.0, 4.0, 6.0]}}
    with pytest.raises(ValueError):
        create_pb2_scheduler(hyperparam_bounds=hyperparam_bounds)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
