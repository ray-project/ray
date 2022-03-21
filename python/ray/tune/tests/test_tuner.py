import unittest
from typing import Optional

from sklearn.datasets import load_breast_cancer
from sklearn.utils import shuffle

from ray import tune
from ray.data import Dataset, Datasource, ReadTask, read_datasource
from ray.data.block import BlockMetadata
from ray.ml.config import RunConfig
from ray.ml.train.integrations.xgboost import XGBoostTrainer
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


# TODO(xwjiang): Add when dataset out-of-band ser/des is landed.
class TunerTest(unittest.TestCase):
    """The e2e test for hparam tuning using Tuner API."""

    def test_tuner_with_xgboost_trainer(self):
        trainer = XGBoostTrainer(
            label_column="target",
            params={},
            datasets={"train": gen_dataset_func()},
        )
            # prep_v1 = StandardScaler(["worst radius", "worst area"])
            # prep_v2 = StandardScaler(["worst concavity", "worst smoothness"])
        param_space = {
            "scaling_config": {
                "num_workers": tune.choice([1, 2, 4]),
            },
            # TODO(xwjiang): Add when https://github.com/ray-project/ray/issues/23363
            #  is resolved.
            # "preprocessor": tune.grid_search([prep_v1, prep_v2]),
            "datasets": {
                "train": tune.choice(
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
        )
        results = tuner.fit()
        assert results.get_best_result().checkpoint


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
