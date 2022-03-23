import os
import shutil
from typing import Optional
import unittest

from sklearn.datasets import load_breast_cancer
from sklearn.utils import shuffle

from ray import tune
from ray.data import from_pandas, read_datasource, Dataset, Datasource, ReadTask
from ray.data.block import BlockMetadata
from ray.ml.config import RunConfig
from ray.ml.train.integrations.xgboost import XGBoostTrainer
from ray.ml.train import Trainer
from ray.tune import Callback, TuneError
from ray.tune.cloud import TrialCheckpoint
from ray.tune.result import DEFAULT_RESULTS_DIR
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner


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
            # TODO(xwjiang): change when dataset out-of-band ser/des is landed.
            datasets={"train": gen_dataset_func_eager()},
        )
        # prep_v1 = StandardScaler(["worst radius", "worst area"])
        # prep_v2 = StandardScaler(["worst concavity", "worst smoothness"])
        param_space = {
            "scaling_config": {
                "num_workers": tune.grid_search([1, 2]),
            },
            # TODO(xwjiang): Add when https://github.com/ray-project/ray/issues/23363
            #  is resolved.
            # "preprocessor": tune.grid_search([prep_v1, prep_v2]),
            # "datasets": {
            #     "train": tune.choice(
            #         [gen_dataset_func(), gen_dataset_func(do_shuffle=True)]
            #     ),
            # },
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
        )
        results = tuner.fit()
        assert not isinstance(results.get_best_result().checkpoint, TrialCheckpoint)
        assert len(results) == 2

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
            # TODO(xwjiang): change when dataset out-of-band ser/des is landed.
            datasets={"train": gen_dataset_func_eager()},
        )
        # prep_v1 = StandardScaler(["worst radius", "worst area"])
        # prep_v2 = StandardScaler(["worst concavity", "worst smoothness"])
        param_space = {
            "scaling_config": {
                "num_workers": tune.grid_search([1, 2]),
            },
            # TODO(xwjiang): Add when https://github.com/ray-project/ray/issues/23363
            #  is resolved.
            # "preprocessor": tune.grid_search([prep_v1, prep_v2]),
            # "datasets": {
            #     "train": tune.choice(
            #         [gen_dataset_func(), gen_dataset_func(do_shuffle=True)]
            #     ),
            # },
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
        )
        with self.assertRaises(TuneError):
            tuner.fit()

        # Test resume
        restore_path = os.path.join(DEFAULT_RESULTS_DIR, "test_tuner_driver_fail")
        tuner = Tuner.restore(restore_path)
        # A hack before we figure out RunConfig semantics across resumes.
        tuner._local_tuner._run_config.callbacks = None
        results = tuner.fit()
        assert len(results) == 2

    def test_tuner_trainer_fail(self):
        class DummyTrainer(Trainer):
            def training_loop(self) -> None:
                raise RuntimeError("There is an error in trainer!")

        trainer = DummyTrainer()
        param_space = {
            "scaling_config": {
                "num_workers": tune.grid_search([1, 2]),
            }
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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
