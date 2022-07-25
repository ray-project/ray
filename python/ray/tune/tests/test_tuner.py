import os
from unittest.mock import patch

import pytest
import shutil
import unittest
from typing import Optional

import ray.air
from sklearn.datasets import load_breast_cancer
from sklearn.utils import shuffle

from ray import tune
from ray.air.config import RunConfig, ScalingConfig
from ray.air.examples.pytorch.torch_linear_example import (
    train_func as linear_train_func,
)
from ray.data import Dataset, Datasource, ReadTask, from_pandas, read_datasource
from ray.data.block import BlockMetadata
from ray.train.torch import TorchTrainer
from ray.train.trainer import BaseTrainer
from ray.train.xgboost import XGBoostTrainer
from ray.tune import Callback, TuneError, CLIReporter
from ray.tune.result import DEFAULT_RESULTS_DIR
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner


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
        with self.assertRaises(TuneError):
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
def test_tuner_api_kwargs(params_expected):
    tuner_params, assertion = params_expected

    tuner = Tuner(lambda config: 1, **tuner_params)

    caught_kwargs = {}

    def catch_kwargs(**kwargs):
        caught_kwargs.update(kwargs)

    with patch("ray.tune.impl.tuner_internal.run", catch_kwargs):
        tuner.fit()

    assert assertion(caught_kwargs)


def test_tuner_fn_trainable_checkpoint_at_end_true():
    tuner = Tuner(
        lambda config, checkpoint_dir: 1,
        run_config=ray.air.RunConfig(
            checkpoint_config=ray.air.CheckpointConfig(checkpoint_at_end=True)
        ),
    )
    with pytest.raises(TuneError):
        tuner.fit()


def test_tuner_fn_trainable_checkpoint_at_end_false():
    tuner = Tuner(
        lambda config, checkpoint_dir: 1,
        run_config=ray.air.RunConfig(
            checkpoint_config=ray.air.CheckpointConfig(checkpoint_at_end=False)
        ),
    )
    tuner.fit()


def test_tuner_fn_trainable_checkpoint_at_end_none():
    tuner = Tuner(
        lambda config, checkpoint_dir: 1,
        run_config=ray.air.RunConfig(
            checkpoint_config=ray.air.CheckpointConfig(checkpoint_at_end=None)
        ),
    )
    tuner.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
