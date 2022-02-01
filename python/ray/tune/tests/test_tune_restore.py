# coding: utf-8
import signal
from collections import Counter
import multiprocessing
import os
import shutil
import tempfile
import time
from typing import List
import unittest

import ray
from ray import tune
from ray._private.test_utils import recursive_fnmatch
from ray.rllib import _register_all
from ray.tune.callback import Callback
from ray.tune.suggest.basic_variant import BasicVariantGenerator
from ray.tune.suggest import Searcher
from ray.tune.trial import Trial
from ray.tune.utils import validate_save_restore
from ray.tune.utils.mock_trainable import MyTrainableClass


class TuneRestoreTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=1, num_gpus=0, local_mode=True)
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
                "framework": "tf",
            },
        )

        logdir = os.path.expanduser(os.path.join(tmpdir, test_name))
        self.logdir = logdir
        self.checkpoint_path = recursive_fnmatch(logdir, "checkpoint-1")[0]

    def tearDown(self):
        shutil.rmtree(self.logdir)
        ray.shutdown()
        _register_all()

    def testTuneRestore(self):
        self.assertTrue(os.path.isfile(self.checkpoint_path))
        tune.run(
            "PG",
            name="TuneRestoreTest",
            stop={"training_iteration": 2},  # train one more iteration.
            checkpoint_freq=1,
            restore=self.checkpoint_path,  # Restore the checkpoint
            config={
                "env": "CartPole-v0",
                "framework": "tf",
            },
        )

    def testPostRestoreCheckpointExistence(self):
        """Tests that checkpoint restored from is not deleted post-restore."""
        self.assertTrue(os.path.isfile(self.checkpoint_path))
        tune.run(
            "PG",
            name="TuneRestoreTest",
            stop={"training_iteration": 2},
            checkpoint_freq=1,
            keep_checkpoints_num=1,
            restore=self.checkpoint_path,
            config={
                "env": "CartPole-v0",
                "framework": "tf",
            },
        )
        self.assertTrue(os.path.isfile(self.checkpoint_path))


# Defining the callbacks at the file level, so they can be pickled and spawned
# in a separate process.
class SteppingCallback(Callback):
    def __init__(self, driver_semaphore, trainer_semaphore):
        self.driver_semaphore = driver_semaphore
        self.trainer_semaphore = trainer_semaphore

    def on_step_end(self, iteration, trials, **info):
        self.driver_semaphore.release()  # Driver should continue
        self.trainer_semaphore.acquire()  # Wait until released


def _run(local_dir, driver_semaphore, trainer_semaphore):
    def _train(config):
        for i in range(7):
            tune.report(val=i)

    tune.run(
        _train,
        local_dir=local_dir,
        name="interrupt",
        callbacks=[SteppingCallback(driver_semaphore, trainer_semaphore)],
    )


class TuneInterruptionTest(unittest.TestCase):
    def setUp(self) -> None:
        # Wait up to five seconds for placement groups when starting a trial
        os.environ["TUNE_PLACEMENT_GROUP_WAIT_S"] = "5"
        # Block for results even when placement groups are pending
        os.environ["TUNE_TRIAL_STARTUP_GRACE_PERIOD"] = "0"
        os.environ["TUNE_TRIAL_RESULT_WAIT_TIME_S"] = "99999"

    def testExperimentInterrupted(self):
        local_dir = tempfile.mkdtemp()
        # Unix platforms may default to "fork", which is problematic with
        # multithreading and GRPC. The child process should always be spawned.
        mp_ctx = multiprocessing.get_context("spawn")
        driver_semaphore = mp_ctx.Semaphore()
        trainer_semaphore = mp_ctx.Semaphore()
        process = mp_ctx.Process(
            target=_run,
            args=(local_dir, driver_semaphore, trainer_semaphore),
            name="tune_interrupt",
        )
        process.daemon = False
        process.start()

        exp_dir = os.path.join(local_dir, "interrupt")

        # Skip first five steps
        for i in range(5):
            driver_semaphore.acquire()  # Wait for callback
            trainer_semaphore.release()  # Continue training

        driver_semaphore.acquire()

        experiment_state_file = None
        for file in os.listdir(exp_dir):
            if file.startswith("experiment_state"):
                experiment_state_file = os.path.join(exp_dir, file)
                break

        self.assertTrue(experiment_state_file)
        last_mtime = os.path.getmtime(experiment_state_file)

        # Now send kill signal
        os.kill(process.pid, signal.SIGINT)
        # Release trainer. It should handle the signal and try to
        # checkpoint the experiment
        trainer_semaphore.release()

        time.sleep(2)  # Wait for checkpoint
        new_mtime = os.path.getmtime(experiment_state_file)

        self.assertNotEqual(last_mtime, new_mtime)

        shutil.rmtree(local_dir)


class TuneFailResumeGridTest(unittest.TestCase):
    class FailureInjectorCallback(Callback):
        """Adds random failure injection to the TrialExecutor."""

        def __init__(self, num_trials=20):
            self.num_trials = num_trials

        def on_step_end(self, trials, **kwargs):
            if len(trials) == self.num_trials:
                print(f"Failing after {self.num_trials} trials.")
                raise RuntimeError

    class CheckStateCallback(Callback):
        """Checks state for the experiment initialization."""

        def __init__(self, expected_trials=20):
            self.expected_trials = expected_trials
            self._checked = False

        def on_step_begin(self, iteration, trials, **kwargs):
            if not self._checked:
                assert len(trials) == self.expected_trials
                self._checked = True

    class CheckTrialResourcesCallback(Callback):
        """Checks if pending trials are requesting the right amount of
        resources.

        The check happens exactly once after `check_after` number of calls
        to on_step_begin(). Note, we deliberately delay the check to after
        `check_after` number of steps. This is because when we start a
        tuning job from fresh (rather than restored), trial list is still
        empty - any check now would be trivial and thus wasted.
        """

        def __init__(self, expected_cpu: int, check_after: int = 1):
            self._expected_cpu = expected_cpu
            self._checked = False
            self._check_after = check_after

        def on_step_begin(self, iteration: int, trials: List["Trial"], **info):
            if not self._checked and iteration >= self._check_after:
                for trial in trials:
                    if trial.status == Trial.PENDING:
                        assert (
                            trial.placement_group_factory.required_resources.get(
                                "CPU", 0
                            )
                            == self._expected_cpu
                        )
                self._checked = True

    def setUp(self):
        self.logdir = tempfile.mkdtemp()
        os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "0"
        # Wait up to 1.5 seconds for placement groups when starting a trial
        os.environ["TUNE_PLACEMENT_GROUP_WAIT_S"] = "1.5"
        # Block for results even when placement groups are pending
        os.environ["TUNE_TRIAL_STARTUP_GRACE_PERIOD"] = "0"
        os.environ["TUNE_TRIAL_RESULT_WAIT_TIME_S"] = "99999"

        # Change back to local_mode=True after this is resolved:
        # https://github.com/ray-project/ray/issues/13932
        ray.init(local_mode=False, num_cpus=2)

        from ray.tune import register_trainable

        register_trainable("trainable", MyTrainableClass)

    def tearDown(self):
        os.environ.pop("TUNE_GLOBAL_CHECKPOINT_S")
        shutil.rmtree(self.logdir)
        ray.shutdown()

    def testFailResumeGridSearch(self):
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

        config = dict(
            num_samples=3,
            fail_fast=True,
            config={
                "test": tune.grid_search([1, 2, 3]),
                "test2": tune.grid_search([1, 2, 3]),
            },
            stop={"training_iteration": 2},
            local_dir=self.logdir,
            verbose=1,
        )

        with self.assertRaises(RuntimeError):
            tune.run("trainable", callbacks=[self.FailureInjectorCallback()], **config)

        analysis = tune.run(
            "trainable", resume=True, callbacks=[self.CheckStateCallback()], **config
        )
        assert len(analysis.trials) == 27
        test_counter = Counter([t.config["test"] for t in analysis.trials])
        assert all(v == 9 for v in test_counter.values())
        test2_counter = Counter([t.config["test2"] for t in analysis.trials])
        assert all(v == 9 for v in test2_counter.values())

    # Unfinished trials' resources should be updated.
    def testResourceUpdateInResume(self):
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

        config = dict(
            num_samples=3,
            fail_fast=True,
            config={
                "test": tune.grid_search([1, 2, 3]),
                "test2": tune.grid_search([1, 2, 3]),
            },
            stop={"training_iteration": 2},
            local_dir=self.logdir,
            verbose=1,
        )

        with self.assertRaises(RuntimeError):
            tune.run(
                "trainable",
                callbacks=[
                    self.FailureInjectorCallback(),
                    self.CheckTrialResourcesCallback(1),
                ],
                **config,
            )

        analysis = tune.run(
            "trainable",
            resume=True,
            resources_per_trial={"cpu": 2},
            callbacks=[self.CheckTrialResourcesCallback(2)],
            **config,
        )
        assert len(analysis.trials) == 27

    def testFailResumeWithPreset(self):
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

        search_alg = BasicVariantGenerator(
            points_to_evaluate=[{"test": -1, "test2": -1}, {"test": -1}, {"test2": -1}]
        )

        config = dict(
            num_samples=3 + 3,  # 3 preset, 3 samples
            fail_fast=True,
            config={
                "test": tune.grid_search([1, 2, 3]),
                "test2": tune.grid_search([1, 2, 3]),
            },
            stop={"training_iteration": 2},
            local_dir=self.logdir,
            verbose=1,
        )
        with self.assertRaises(RuntimeError):
            tune.run(
                "trainable",
                callbacks=[self.FailureInjectorCallback(5)],
                search_alg=search_alg,
                **config,
            )

        analysis = tune.run(
            "trainable",
            resume=True,
            callbacks=[self.CheckStateCallback(expected_trials=5)],
            search_alg=search_alg,
            **config,
        )
        assert len(analysis.trials) == 34
        test_counter = Counter([t.config["test"] for t in analysis.trials])
        assert test_counter.pop(-1) == 4
        assert all(v == 10 for v in test_counter.values())
        test2_counter = Counter([t.config["test2"] for t in analysis.trials])
        assert test2_counter.pop(-1) == 4
        assert all(v == 10 for v in test2_counter.values())

    def testFailResumeAfterPreset(self):
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

        search_alg = BasicVariantGenerator(
            points_to_evaluate=[{"test": -1, "test2": -1}, {"test": -1}, {"test2": -1}]
        )

        config = dict(
            num_samples=3 + 3,  # 3 preset, 3 samples
            fail_fast=True,
            config={
                "test": tune.grid_search([1, 2, 3]),
                "test2": tune.grid_search([1, 2, 3]),
            },
            stop={"training_iteration": 2},
            local_dir=self.logdir,
            verbose=1,
        )

        with self.assertRaises(RuntimeError):
            tune.run(
                "trainable",
                callbacks=[self.FailureInjectorCallback(15)],
                search_alg=search_alg,
                **config,
            )

        analysis = tune.run(
            "trainable",
            resume=True,
            callbacks=[self.CheckStateCallback(expected_trials=15)],
            search_alg=search_alg,
            **config,
        )
        assert len(analysis.trials) == 34
        test_counter = Counter([t.config["test"] for t in analysis.trials])
        assert test_counter.pop(-1) == 4
        assert all(v == 10 for v in test_counter.values())
        test2_counter = Counter([t.config["test2"] for t in analysis.trials])
        assert test2_counter.pop(-1) == 4
        assert all(v == 10 for v in test2_counter.values())

    def testMultiExperimentFail(self):
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

        experiments = []
        for i in range(3):
            experiments.append(
                tune.Experiment(
                    run=MyTrainableClass,
                    name="trainable",
                    num_samples=2,
                    config={
                        "test": tune.grid_search([1, 2, 3]),
                    },
                    stop={"training_iteration": 1},
                    local_dir=self.logdir,
                )
            )

        with self.assertRaises(RuntimeError):
            tune.run(
                experiments,
                callbacks=[self.FailureInjectorCallback(10)],
                fail_fast=True,
            )

        analysis = tune.run(
            experiments,
            resume=True,
            callbacks=[self.CheckStateCallback(expected_trials=10)],
            fail_fast=True,
        )
        assert len(analysis.trials) == 18

    def testWarningLargeGrid(self):
        config = dict(
            num_samples=3,
            fail_fast=True,
            config={
                "test": tune.grid_search(list(range(20))),
                "test2": tune.grid_search(list(range(20))),
                "test3": tune.grid_search(list(range(20))),
                "test4": tune.grid_search(list(range(20))),
                "test5": tune.grid_search(list(range(20))),
            },
            stop={"training_iteration": 2},
            local_dir=self.logdir,
            verbose=1,
        )
        with self.assertWarnsRegex(UserWarning, "exceeds the serialization threshold"):
            with self.assertRaises(RuntimeError):
                tune.run(
                    "trainable", callbacks=[self.FailureInjectorCallback(10)], **config
                )


class TuneExampleTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=2)

    def tearDown(self):
        ray.shutdown()
        _register_all()

    def testPBTKeras(self):
        from ray.tune.examples.pbt_tune_cifar10_with_keras import Cifar10Model
        from tensorflow.python.keras.datasets import cifar10

        cifar10.load_data()
        validate_save_restore(Cifar10Model)
        validate_save_restore(Cifar10Model, use_object_store=True)

    def testPyTorchMNIST(self):
        from ray.tune.examples.mnist_pytorch_trainable import TrainMNIST
        from torchvision import datasets

        datasets.MNIST("~/data", train=True, download=True)
        validate_save_restore(TrainMNIST)
        validate_save_restore(TrainMNIST, use_object_store=True)

    def testHyperbandExample(self):
        validate_save_restore(MyTrainableClass)
        validate_save_restore(MyTrainableClass, use_object_store=True)

    def testAsyncHyperbandExample(self):
        validate_save_restore(MyTrainableClass)
        validate_save_restore(MyTrainableClass, use_object_store=True)


class AutoInitTest(unittest.TestCase):
    def testTuneRestore(self):
        self.assertFalse(ray.is_initialized())
        tune.run("__fake", name="TestAutoInit", stop={"training_iteration": 1})
        self.assertTrue(ray.is_initialized())

    def tearDown(self):
        ray.shutdown()
        _register_all()


class SearcherTest(unittest.TestCase):
    class MockSearcher(Searcher):
        def __init__(self, data):
            self.data = data

        def save(self, path):
            with open(path, "w") as f:
                f.write(self.data)

        def restore(self, path):
            with open(path, "r") as f:
                self.data = f.read()

    def testSaveRestoreDir(self):
        tmpdir = tempfile.mkdtemp()
        original_data = "hello-its-me"
        searcher = self.MockSearcher(original_data)
        searcher.save_to_dir(tmpdir)
        searcher_2 = self.MockSearcher("no-its-not-me")
        searcher_2.restore_from_dir(tmpdir)
        assert searcher_2.data == original_data


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
