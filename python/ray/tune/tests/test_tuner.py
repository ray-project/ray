import os
from pathlib import Path
from unittest.mock import patch

import pytest
import shutil
import unittest
from typing import Optional

import ray.air
from sklearn.datasets import load_breast_cancer
from sklearn.utils import shuffle

from ray import tune
from ray.air import session
from ray.air.config import RunConfig, ScalingConfig, FailureConfig
from ray.train.examples.pytorch.torch_linear_example import (
    train_func as linear_train_func,
)
from ray.data import Dataset, Datasource, ReadTask, from_pandas, read_datasource
from ray.data.block import BlockMetadata
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.torch import TorchTrainer
from ray.train.trainer import BaseTrainer
from ray.train.xgboost import XGBoostTrainer
from ray.tune import Callback, CLIReporter
from ray.tune.result import DEFAULT_RESULTS_DIR
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner


@pytest.fixture
def shutdown_only():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def chdir_tmpdir(tmpdir):
    old_cwd = os.getcwd()
    os.chdir(tmpdir)
    yield tmpdir
    os.chdir(old_cwd)


class DummyTrainer(BaseTrainer):
    _scaling_config_allowed_keys = BaseTrainer._scaling_config_allowed_keys + [
        "num_workers",
        "use_gpu",
        "resources_per_worker",
        "placement_strategy",
    ]

    def training_loop(self) -> None:
        for i in range(5):
            with tune.checkpoint_dir(step=i) as checkpoint_dir:
                path = os.path.join(checkpoint_dir, "checkpoint")
                with open(path, "w") as f:
                    f.write(str(i))
            tune.report(step=i)


class FailingTrainer(DummyTrainer):
    def training_loop(self) -> None:
        raise RuntimeError("There is an error in trainer!")


class TestDatasource(Datasource):
    def __init__(self, do_shuffle: bool):
        self._shuffle = do_shuffle

    def prepare_read(self, parallelism: int, **read_args):
        import pyarrow as pa

        def load_data():
            data_raw = load_breast_cancer(as_frame=True)
            dataset_df = data_raw["data"]
            dataset_df["target"] = data_raw["target"]
            if self._shuffle:
                dataset_df = shuffle(dataset_df)
            return [pa.Table.from_pandas(dataset_df)]

        meta = BlockMetadata(
            num_rows=None,
            size_bytes=None,
            schema=None,
            input_files=None,
            exec_stats=None,
        )
        return [ReadTask(load_data, meta)]


def gen_dataset_func(do_shuffle: Optional[bool] = False) -> Dataset:
    test_datasource = TestDatasource(do_shuffle)
    return read_datasource(test_datasource)


def gen_dataset_func_eager():
    data_raw = load_breast_cancer(as_frame=True)
    dataset_df = data_raw["data"]
    dataset_df["target"] = data_raw["target"]
    dataset = from_pandas(dataset_df)
    return dataset


class TunerTest(unittest.TestCase):
    """The e2e test for hparam tuning using Tuner API."""

    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()

    def test_tuner_with_xgboost_trainer(self):
        """Test a successful run."""
        shutil.rmtree(
            os.path.join(DEFAULT_RESULTS_DIR, "test_tuner"), ignore_errors=True
        )
        trainer = XGBoostTrainer(
            label_column="target",
            params={},
            datasets={"train": gen_dataset_func_eager()},
        )
        # prep_v1 = StandardScaler(["worst radius", "worst area"])
        # prep_v2 = StandardScaler(["worst concavity", "worst smoothness"])
        param_space = {
            "scaling_config": ScalingConfig(num_workers=tune.grid_search([1, 2])),
            # "preprocessor": tune.grid_search([prep_v1, prep_v2]),
            "datasets": {
                "train": tune.grid_search(
                    [gen_dataset_func(), gen_dataset_func(do_shuffle=True)]
                ),
            },
            "params": {
                "objective": "binary:logistic",
                "tree_method": "approx",
                "eval_metric": ["logloss", "error"],
                "eta": tune.loguniform(1e-4, 1e-1),
                "subsample": tune.uniform(0.5, 1.0),
                "max_depth": tune.randint(1, 9),
            },
        }
        tuner = Tuner(
            trainable=trainer,
            run_config=RunConfig(name="test_tuner"),
            param_space=param_space,
            tune_config=TuneConfig(mode="min", metric="train-error"),
            # limiting the number of trials running at one time.
            # As the unit test only has access to 4 CPUs on Buildkite.
            _tuner_kwargs={"max_concurrent_trials": 1},
        )
        results = tuner.fit()
        assert len(results) == 4

    def test_tuner_with_xgboost_trainer_driver_fail_and_resume(self):
        # So that we have some global checkpointing happening.
        os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "1"
        shutil.rmtree(
            os.path.join(DEFAULT_RESULTS_DIR, "test_tuner_driver_fail"),
            ignore_errors=True,
        )
        trainer = XGBoostTrainer(
            label_column="target",
            params={},
            datasets={"train": gen_dataset_func_eager()},
        )
        # prep_v1 = StandardScaler(["worst radius", "worst area"])
        # prep_v2 = StandardScaler(["worst concavity", "worst smoothness"])
        param_space = {
            "scaling_config": ScalingConfig(num_workers=tune.grid_search([1, 2])),
            # "preprocessor": tune.grid_search([prep_v1, prep_v2]),
            "datasets": {
                "train": tune.grid_search(
                    [gen_dataset_func(), gen_dataset_func(do_shuffle=True)]
                ),
            },
            "params": {
                "objective": "binary:logistic",
                "tree_method": "approx",
                "eval_metric": ["logloss", "error"],
                "eta": tune.loguniform(1e-4, 1e-1),
                "subsample": tune.uniform(0.5, 1.0),
                "max_depth": tune.randint(1, 9),
            },
        }

        class FailureInjectionCallback(Callback):
            """Inject failure at the configured iteration number."""

            def __init__(self, num_iters=10):
                self.num_iters = num_iters

            def on_step_end(self, iteration, trials, **kwargs):
                if iteration == self.num_iters:
                    print(f"Failing after {self.num_iters} iters.")
                    raise RuntimeError

        tuner = Tuner(
            trainable=trainer,
            run_config=RunConfig(
                name="test_tuner_driver_fail", callbacks=[FailureInjectionCallback()]
            ),
            param_space=param_space,
            tune_config=TuneConfig(mode="min", metric="train-error"),
            # limiting the number of trials running at one time.
            # As the unit test only has access to 4 CPUs on Buildkite.
            _tuner_kwargs={"max_concurrent_trials": 1},
        )
        with self.assertRaises(RuntimeError):
            tuner.fit()

        # Test resume
        restore_path = os.path.join(DEFAULT_RESULTS_DIR, "test_tuner_driver_fail")
        tuner = Tuner.restore(restore_path)
        # A hack before we figure out RunConfig semantics across resumes.
        tuner._local_tuner._run_config.callbacks = None
        results = tuner.fit()
        assert len(results) == 4

    def test_tuner_trainer_fail(self):
        trainer = FailingTrainer()
        param_space = {
            "scaling_config": ScalingConfig(num_workers=tune.grid_search([1, 2]))
        }
        tuner = Tuner(
            trainable=trainer,
            run_config=RunConfig(name="test_tuner_trainer_fail"),
            param_space=param_space,
            tune_config=TuneConfig(mode="max", metric="iteration"),
        )
        results = tuner.fit()
        assert len(results) == 2
        for i in range(2):
            assert results[i].error

    def test_tuner_fail_fast_true(self):
        trainer = FailingTrainer()
        param_space = {
            "scaling_config": ScalingConfig(num_workers=tune.grid_search([1, 2]))
        }

        failure_config = FailureConfig(fail_fast=True)

        tuner = Tuner(
            trainable=trainer,
            run_config=RunConfig(
                name="test_tuner_trainer_fail", failure_config=failure_config
            ),
            param_space=param_space,
            tune_config=TuneConfig(mode="max", metric="iteration"),
        )

        results = tuner.fit()
        errors = [result.error for result in results if result.error]
        assert len(errors) == 1

    def test_tuner_fail_fast_raise(self):
        trainer = FailingTrainer()
        param_space = {
            "scaling_config": ScalingConfig(num_workers=tune.grid_search([1, 2]))
        }

        failure_config = FailureConfig(fail_fast="raise")

        tuner = Tuner(
            trainable=trainer,
            run_config=RunConfig(
                name="test_tuner_trainer_fail", failure_config=failure_config
            ),
            param_space=param_space,
            tune_config=TuneConfig(mode="max", metric="iteration"),
        )
        with self.assertRaises(RuntimeError):
            tuner.fit()

    def test_tuner_with_torch_trainer(self):
        """Test a successful run using torch trainer."""
        shutil.rmtree(
            os.path.join(DEFAULT_RESULTS_DIR, "test_tuner_torch"), ignore_errors=True
        )
        # The following two should be tunable.
        config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": 10}
        scaling_config = ScalingConfig(num_workers=1, use_gpu=False)
        trainer = TorchTrainer(
            train_loop_per_worker=linear_train_func,
            train_loop_config=config,
            scaling_config=scaling_config,
        )
        param_space = {
            "scaling_config": ScalingConfig(num_workers=tune.grid_search([1, 2])),
            "train_loop_config": {
                "batch_size": tune.grid_search([4, 8]),
                "epochs": tune.grid_search([5, 10]),
            },
        }
        tuner = Tuner(
            trainable=trainer,
            run_config=RunConfig(name="test_tuner"),
            param_space=param_space,
            tune_config=TuneConfig(mode="min", metric="loss"),
        )
        results = tuner.fit()
        assert len(results) == 8

    def test_tuner_run_config_override(self):
        trainer = DummyTrainer(run_config=RunConfig(stop={"metric": 4}))
        tuner = Tuner(trainer)

        assert tuner._local_tuner._run_config.stop == {"metric": 4}


@pytest.mark.parametrize(
    "params_expected",
    [
        (
            {"run_config": RunConfig(progress_reporter=CLIReporter())},
            lambda kw: isinstance(kw["progress_reporter"], CLIReporter),
        ),
        (
            {"tune_config": TuneConfig(reuse_actors=True)},
            lambda kw: kw["reuse_actors"] is True,
        ),
        (
            {"run_config": RunConfig(log_to_file="some_file")},
            lambda kw: kw["log_to_file"] == "some_file",
        ),
        (
            {"tune_config": TuneConfig(max_concurrent_trials=3)},
            lambda kw: kw["max_concurrent_trials"] == 3,
        ),
        (
            {"tune_config": TuneConfig(time_budget_s=60)},
            lambda kw: kw["time_budget_s"] == 60,
        ),
    ],
)
def test_tuner_api_kwargs(shutdown_only, params_expected):
    tuner_params, assertion = params_expected

    tuner = Tuner(lambda config: 1, **tuner_params)

    caught_kwargs = {}

    def catch_kwargs(**kwargs):
        caught_kwargs.update(kwargs)

    with patch("ray.tune.impl.tuner_internal.run", catch_kwargs):
        tuner.fit()

    assert assertion(caught_kwargs)


def test_tuner_fn_trainable_invalid_checkpoint_config(shutdown_only):
    tuner = Tuner(
        lambda config: 1,
        run_config=ray.air.RunConfig(
            checkpoint_config=ray.air.CheckpointConfig(checkpoint_at_end=True)
        ),
    )
    with pytest.raises(ValueError):
        tuner.fit()

    tuner = Tuner(
        lambda config: 1,
        run_config=ray.air.RunConfig(
            checkpoint_config=ray.air.CheckpointConfig(checkpoint_frequency=1)
        ),
    )
    with pytest.raises(ValueError):
        tuner.fit()


def test_tuner_trainer_checkpoint_config(shutdown_only):
    custom_training_loop_trainer = DataParallelTrainer(
        train_loop_per_worker=lambda config: 1
    )
    tuner = Tuner(
        custom_training_loop_trainer,
        run_config=ray.air.RunConfig(
            checkpoint_config=ray.air.CheckpointConfig(checkpoint_at_end=True)
        ),
    )
    with pytest.raises(ValueError):
        tuner.fit()

    tuner = Tuner(
        custom_training_loop_trainer,
        run_config=ray.air.RunConfig(
            checkpoint_config=ray.air.CheckpointConfig(checkpoint_frequency=1)
        ),
    )
    with pytest.raises(ValueError):
        tuner.fit()

    handles_checkpoints_trainer = XGBoostTrainer(
        label_column="target",
        params={},
        datasets={"train": ray.data.from_items(list(range(5)))},
    )
    tuner = Tuner(
        handles_checkpoints_trainer,
        run_config=ray.air.RunConfig(
            checkpoint_config=ray.air.CheckpointConfig(
                checkpoint_at_end=True, checkpoint_frequency=1
            )
        ),
    )._local_tuner
    # Check that validation passes for a Trainer that does handle checkpointing
    tuner._get_tune_run_arguments(tuner.converted_trainable)


def test_tuner_fn_trainable_checkpoint_at_end_false(shutdown_only):
    tuner = Tuner(
        lambda config, checkpoint_dir: 1,
        run_config=ray.air.RunConfig(
            checkpoint_config=ray.air.CheckpointConfig(checkpoint_at_end=False)
        ),
    )
    tuner.fit()


def test_tuner_fn_trainable_checkpoint_at_end_none(shutdown_only):
    tuner = Tuner(
        lambda config, checkpoint_dir: 1,
        run_config=ray.air.RunConfig(
            checkpoint_config=ray.air.CheckpointConfig(checkpoint_at_end=None)
        ),
    )
    tuner.fit()


def test_nonserializable_trainable(capsys):
    import threading

    lock = threading.Lock()
    with pytest.raises(TypeError):
        Tuner(lambda config: print(lock))

    # Check that the `inspect_serializability` trace was printed
    out, _ = capsys.readouterr()
    assert "was found to be non-serializable." in out


@pytest.mark.parametrize("runtime_env", [{}, {"working_dir": "."}])
def test_tuner_no_chdir_to_trial_dir(shutdown_only, chdir_tmpdir, runtime_env):
    """Tests that setting `chdir_to_trial_dir=False` in `TuneConfig` allows for
    reading relatives paths to the original working directory.
    Also tests that `session.get_trial_dir()` env variable can be used as the directory
    to write data to within the Trainable.
    """
    # Write a data file that we want to read in our training loop
    with open("./read.txt", "w") as f:
        f.write("data")

    ray.init(num_cpus=1, runtime_env=runtime_env)

    def train_func(config):
        orig_working_dir = Path(os.environ["TUNE_ORIG_WORKING_DIR"])
        assert str(orig_working_dir) == os.getcwd(), (
            "Working directory should not have changed from "
            f"{orig_working_dir} to {os.getcwd()}"
        )
        # Make sure we can access the data from the original working dir
        assert os.path.exists("./read.txt") and open("./read.txt", "r").read() == "data"

        # Write operations should happen in each trial's independent logdir to
        # prevent write conflicts
        trial_dir = Path(session.get_trial_dir())
        with open(trial_dir / "write.txt", "w") as f:
            f.write(f"{config['id']}")

    tuner = Tuner(
        train_func,
        tune_config=TuneConfig(chdir_to_trial_dir=False),
        param_space={"id": tune.grid_search(list(range(4)))},
    )
    results = tuner.fit()
    assert not results.errors
    for result in results:
        artifact_data = open(result.log_dir / "write.txt", "r").read()
        assert artifact_data == f"{result.config['id']}"


@pytest.mark.parametrize("runtime_env", [{}, {"working_dir": "."}])
def test_tuner_relative_pathing_with_env_vars(shutdown_only, chdir_tmpdir, runtime_env):
    """Tests that `TUNE_ORIG_WORKING_DIR` environment variable can be used to access
    relative paths to the original working directory.
    """
    # Write a data file that we want to read in our training loop
    with open("./read.txt", "w") as f:
        f.write("data")

    # Even if we set our runtime_env `{"working_dir": "."}` to the current directory,
    # Tune should still chdir to the trial directory, since we didn't disable the
    # `chdir_to_trial_dir` flag.
    ray.init(num_cpus=1, runtime_env=runtime_env)

    def train_func(config):
        orig_working_dir = Path(os.environ["TUNE_ORIG_WORKING_DIR"])
        assert (
            str(orig_working_dir) != os.getcwd()
        ), f"Working directory should have changed from {orig_working_dir}"

        # Make sure we can access the data from the original working dir
        # Different from above: create an absolute path using the env variable
        data_path = orig_working_dir / "read.txt"
        assert os.path.exists(data_path) and open(data_path, "r").read() == "data"

        trial_dir = Path(session.get_trial_dir())
        # Tune should have changed the working directory to the trial directory
        assert str(trial_dir) == os.getcwd()

        with open(trial_dir / "write.txt", "w") as f:
            f.write(f"{config['id']}")

    tuner = Tuner(
        train_func,
        tune_config=TuneConfig(chdir_to_trial_dir=True),
        param_space={"id": tune.grid_search(list(range(4)))},
    )
    results = tuner.fit()
    assert not results.errors
    for result in results:
        artifact_data = open(result.log_dir / "write.txt", "r").read()
        assert artifact_data == f"{result.config['id']}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
