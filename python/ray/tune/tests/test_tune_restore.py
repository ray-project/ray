# coding: utf-8
import multiprocessing
import os
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import unittest
from collections import Counter
from typing import List
from unittest import mock

import pytest

import ray
import ray.train
from ray import tune
from ray._private.test_utils import run_string_as_driver
from ray.exceptions import RayTaskError
from ray.train._internal.session import _TrainingResult
from ray.tune import Checkpoint, TuneError
from ray.tune.callback import Callback
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Trial
from ray.tune.search import Searcher
from ray.tune.search.basic_variant import BasicVariantGenerator
from ray.tune.utils import validate_save_restore
from ray.tune.utils.mock_trainable import MyTrainableClass


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
            ray.tune.report(dict(val=i))

    tune.run(
        _train,
        storage_path=local_dir,
        name="interrupt",
        callbacks=[SteppingCallback(driver_semaphore, trainer_semaphore)],
    )


class TuneInterruptionTest(unittest.TestCase):
    # Todo(krfricke): Investigate and fix on CI
    @unittest.skip("Spawn seems to have a malfunction on Python 3.8 CI")
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

    def testInterruptDisabledInWorkerThread(self):
        # https://github.com/ray-project/ray/issues/22295
        # This test will hang without the proper patch because tune.run will fail.

        event = threading.Event()

        def run_in_thread():
            def _train(config):
                for i in range(7):
                    ray.tune.report(dict(val=i))

            tune.run(_train)
            event.set()

        thread = threading.Thread(target=run_in_thread)
        thread.start()
        event.wait()
        thread.join()

        ray.shutdown()
        os.environ.pop("TUNE_DISABLE_SIGINT_HANDLER", None)


class TuneFailResumeGridTest(unittest.TestCase):
    class FailureInjectorCallback(Callback):
        """Adds random failure injection to the TrialExecutor."""

        def __init__(self, num_trials=20, delay_s=0.3):
            self.num_trials = num_trials
            self.delay_s = delay_s
            self.fail_at = None

        def on_step_end(self, trials, **kwargs):
            if self.fail_at:
                if time.monotonic() >= self.fail_at:
                    raise RuntimeError(f"Failing after {self.delay_s}")
                return

            if len(trials) >= self.num_trials:
                print(
                    f"Reached {self.num_trials} trials. "
                    f"Scheduling failure in {self.delay_s} seconds."
                )
                self.fail_at = time.monotonic() + self.delay_s

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

        # These tests need driver syncing to happen before the crash happens
        # so that they can pick up from the *exact* state it left off at.
        # We do this by failing after a delay of 0.3s > TUNE_GLOBAL_CHECKPOINT_S
        os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "0.1"

        # Change back to local_mode=True after this is resolved:
        # https://github.com/ray-project/ray/issues/13932
        ray.init(local_mode=False, num_cpus=2)

        from ray.tune import register_trainable

        register_trainable("trainable", MyTrainableClass)

    def tearDown(self):
        os.environ.pop("TUNE_GLOBAL_CHECKPOINT_S")
        os.environ.pop("TUNE_MAX_PENDING_TRIALS_PG", None)
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
            name="testFailResumeGridSearch",
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
            name="testResourceUpdateInResume",
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

    @mock.patch.dict(os.environ, {"TUNE_MAX_PENDING_TRIALS_PG": "1"})
    def testConfigUpdateInResume(self):
        class FakeDataset:
            def __init__(self, name):
                self.name = name

        config = dict(
            num_samples=1,
            fail_fast=True,
            config={
                "test": tune.grid_search(
                    [FakeDataset("1"), FakeDataset("2"), FakeDataset("3")]
                ),
                "test2": tune.grid_search(
                    [
                        FakeDataset("4"),
                        FakeDataset("5"),
                        FakeDataset("6"),
                        FakeDataset("7"),
                    ]
                ),
            },
            stop={"training_iteration": 2},
            name="testConfigUpdateInResume",
            verbose=1,
        )

        with self.assertRaises(RuntimeError):
            tune.run(
                "trainable",
                callbacks=[
                    self.FailureInjectorCallback(num_trials=1),
                    self.CheckTrialResourcesCallback(1),
                ],
                **config,
            )

        config["config"] = {
            "test": tune.grid_search(
                [FakeDataset("8"), FakeDataset("9"), FakeDataset("10")]
            ),
            "test2": tune.grid_search(
                [
                    FakeDataset("11"),
                    FakeDataset("12"),
                    FakeDataset("13"),
                    FakeDataset("14"),
                ]
            ),
        }

        analysis = tune.run(
            "trainable",
            resume=True,
            **config,
        )
        assert len(analysis.trials) == 12
        for t in analysis.trials:
            # Make sure that test and test2 are updated.
            assert t.config["test"].name in ["8", "9", "10"]
            assert t.config["test2"].name in ["11", "12", "13", "14"]

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
            name="testFailResumeWithPreset",
            verbose=1,
        )
        with self.assertRaises(RuntimeError):
            tune.run(
                "trainable",
                callbacks=[self.FailureInjectorCallback(5)],
                search_alg=search_alg,
                **config,
            )

        print("---- RESTARTING RUN ----")

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
            name="testFailResumeAfterPreset",
            verbose=1,
        )

        with self.assertRaises(RuntimeError):
            tune.run(
                "trainable",
                callbacks=[self.FailureInjectorCallback(15)],
                search_alg=search_alg,
                **config,
            )

        print("---- RESTARTING RUN ----")

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
                    name="testMultiExperimentFail",
                    num_samples=2,
                    config={
                        "test": tune.grid_search([1, 2, 3]),
                    },
                    stop={"training_iteration": 1},
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
            name="testWarningLargeGrid",
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

    def testPBTKeras(self):
        from tensorflow.keras.datasets import cifar10

        from ray.tune.examples.pbt_tune_cifar10_with_keras import Cifar10Model

        cifar10.load_data()
        validate_save_restore(Cifar10Model)

    def testPyTorchMNIST(self):
        from torchvision import datasets

        from ray.tune.examples.mnist_pytorch_trainable import TrainMNIST

        datasets.MNIST("~/data", train=True, download=True)
        validate_save_restore(TrainMNIST)

    def testHyperbandExample(self):
        validate_save_restore(MyTrainableClass)

    def testAsyncHyperbandExample(self):
        validate_save_restore(MyTrainableClass)


class AutoInitTest(unittest.TestCase):
    def testTuneRestore(self):
        self.assertFalse(ray.is_initialized())
        tune.run(MyTrainableClass, name="TestAutoInit", stop={"training_iteration": 1})
        self.assertTrue(ray.is_initialized())

    def tearDown(self):
        ray.shutdown()


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


class WorkingDirectoryTest(unittest.TestCase):
    def testWorkingDir(self):
        """Trainables should know the original working dir through env variable."""

        os.environ.pop("TUNE_ORIG_WORKING_DIR", None)
        working_dir = os.getcwd()

        def f(config):
            assert os.environ.get("TUNE_ORIG_WORKING_DIR") == working_dir

        ray.init(num_cpus=1)
        tune.run(f)
        ray.shutdown()


class TrainableCrashWithFailFast(unittest.TestCase):
    def test(self):
        """Trainable crashes with fail_fast flag and the original crash message
        should bubble up."""

        def f(config):
            ray.tune.report({"a": 1})
            time.sleep(0.1)
            raise RuntimeError("Error happens in trainable!!")

        with self.assertRaisesRegex(RayTaskError, "Error happens in trainable!!"):
            tune.run(f, fail_fast=TuneController.RAISE)


@pytest.mark.parametrize(
    "trial_config", [{}, {"attr": 4}, {"nested": {"key": "value"}}]
)
def test_trial_last_result_restore(trial_config):
    metrics = {"metric1": 4, "nested2": {"metric3": 6}}
    metrics["config"] = trial_config

    trial = Trial(trainable_name="stub", config=trial_config, stub=True)
    trial.update_last_result(metrics)

    result = _TrainingResult(
        checkpoint=Checkpoint(path="file:///tmp/no_data"), metrics=metrics
    )

    trial.temporary_state.restoring_from = result
    trial.on_restore()
    assert trial.run_metadata.last_result == metrics


def test_stacktrace():
    """Test proper stacktrace is printed for RayTaskError."""
    CMD = """
from ray import tune

def train_fn(config):
    raise Exception("Inducing exception for testing purposes.")

tune.run(train_fn, num_samples=1)
    """
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        run_string_as_driver(CMD)
    assert "Inducing exception for testing purposes." in exc_info.value.output.decode()


@pytest.mark.parametrize(
    "resume",
    [
        True,
        "AUTO",
        "AUTO+ERRORED",
        "AUTO+ERRORED_ONLY",
        "AUTO+RESTART_ERRORED",
        "AUTO+RESTART_ERRORED_ONLY",
    ],
)
def test_resume_options(tmp_path, resume):
    tmp_path.joinpath("dummy_ckpt").mkdir()

    def train_fn(config):
        checkpoint = ray.tune.get_checkpoint()
        if not checkpoint:
            ray.tune.report(
                {"finish_marker": False},
                checkpoint=Checkpoint.from_directory(tmp_path / "dummy_ckpt"),
            )
            raise RuntimeError("failing on the first run!!")
        ray.tune.report({"finish_marker": True})

    analysis = tune.run(
        train_fn,
        storage_path=str(tmp_path),
        name="test_resume_options",
        raise_on_failed_trial=False,
    )
    results = ray.tune.ResultGrid(analysis)
    assert not results[0].metrics.get("finish_marker", False)
    analysis = tune.run(
        train_fn,
        storage_path=str(tmp_path),
        name="test_resume_options",
        resume=resume,
        raise_on_failed_trial=False,
    )
    results = ray.tune.ResultGrid(analysis)
    if resume in [True, "AUTO", "AUTO+RESTART_ERRORED", "AUTO+RESTART_ERRORED_ONLY"]:
        # These options either don't resume the errored trial,
        # or restart it without a checkpoint --> leading to the RuntimeError again
        assert not results[0].metrics.get("finish_marker")
    else:
        assert results[0].metrics.get("finish_marker")


# For some reason, different tests are coupled through tune.registry.
# After running `ResourceExhaustedTest`, there is always a super huge `training_func` to
# be put through GCS, which will fail subsequent tests.
# tldr, make sure that this test is the last test in the file.
class ResourceExhaustedTest(unittest.TestCase):
    def test_resource_exhausted_info(self):
        """This is to test if helpful information is displayed when
        the objects captured in trainable/training function are too
        large and RESOURCES_EXHAUSTED error of gRPC is triggered."""

        # generate some random data to be captured implicitly in training func.
        from sklearn.datasets import fetch_olivetti_faces

        a_large_array = []
        for i in range(50):
            a_large_array.append(fetch_olivetti_faces())

        def training_func(config):
            for item in a_large_array:
                assert item

        with self.assertRaisesRegex(
            TuneError,
            "The Trainable/training function is too large for grpc resource limit.",
        ):
            tune.run(training_func)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
