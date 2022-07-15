import pytest
import json
import pandas as pd

import xgboost as xgb

import ray
from ray import tune
from ray.air.checkpoint import Checkpoint
from ray.train.constants import TRAIN_DATASET_KEY

from ray.train.xgboost import XGBoostTrainer, load_checkpoint
from ray.data.preprocessor import Preprocessor

from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


scale_config = {"num_workers": 2}

data_raw = load_breast_cancer()
dataset_df = pd.DataFrame(data_raw["data"], columns=data_raw["feature_names"])
dataset_df["target"] = data_raw["target"]
train_df, test_df = train_test_split(dataset_df, test_size=0.3)

params = {
    "tree_method": "approx",
    "objective": "binary:logistic",
    "eval_metric": ["logloss", "error"],
}


def get_num_trees(booster: xgb.Booster) -> int:
    data = [json.loads(d) for d in booster.get_dump(dump_format="json")]
    return len(data)


def test_fit(ray_start_4_cpus):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = XGBoostTrainer(
        scaling_config=scale_config,
        label_column="target",
        params=params,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    trainer.fit()


def test_resume_from_checkpoint(ray_start_4_cpus, tmpdir):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = XGBoostTrainer(
        scaling_config=scale_config,
        label_column="target",
        params=params,
        num_boost_round=5,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    result = trainer.fit()
    checkpoint = result.checkpoint
    xgb_model, _ = load_checkpoint(checkpoint)
    assert get_num_trees(xgb_model) == 5

    # Move checkpoint to a different directory.
    checkpoint_dict = result.checkpoint.to_dict()
    checkpoint = Checkpoint.from_dict(checkpoint_dict)
    checkpoint_path = checkpoint.to_directory(tmpdir)
    resume_from = Checkpoint.from_directory(checkpoint_path)

    trainer = XGBoostTrainer(
        scaling_config=scale_config,
        label_column="target",
        params=params,
        num_boost_round=5,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        resume_from_checkpoint=resume_from,
    )
    result = trainer.fit()
    checkpoint = result.checkpoint
    model, _ = load_checkpoint(checkpoint)
    assert get_num_trees(model) == 10


def test_preprocessor_in_checkpoint(ray_start_4_cpus, tmpdir):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)

    class DummyPreprocessor(Preprocessor):
        def __init__(self):
            super().__init__()
            self.is_same = True

        def fit(self, dataset):
            self.fitted_ = True

        def _transform_pandas(self, df: "pd.DataFrame") -> "pd.DataFrame":
            return df

    trainer = XGBoostTrainer(
        scaling_config=scale_config,
        label_column="target",
        params=params,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        preprocessor=DummyPreprocessor(),
    )
    result = trainer.fit()

    # Move checkpoint to a different directory.
    checkpoint_dict = result.checkpoint.to_dict()
    checkpoint = Checkpoint.from_dict(checkpoint_dict)
    checkpoint_path = checkpoint.to_directory(tmpdir)
    resume_from = Checkpoint.from_directory(checkpoint_path)

    model, preprocessor = load_checkpoint(resume_from)
    assert get_num_trees(model) == 10
    assert preprocessor.is_same
    assert preprocessor.fitted_


def test_tune(ray_start_4_cpus):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = XGBoostTrainer(
        scaling_config=scale_config,
        label_column="target",
        params={**params, **{"max_depth": 1}},
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )

    tune.run(
        trainer.as_trainable(),
        config={"params": {"max_depth": tune.randint(2, 4)}},
        num_samples=2,
    )

    # Make sure original Trainer is not affected.
    assert trainer.params["max_depth"] == 1


def test_validation(ray_start_4_cpus):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    with pytest.raises(KeyError, match=TRAIN_DATASET_KEY):
        XGBoostTrainer(
            scaling_config={"num_workers": 2},
            label_column="target",
            params=params,
            datasets={"valid": valid_dataset},
        )
    with pytest.raises(KeyError, match="dmatrix_params"):
        XGBoostTrainer(
            scaling_config={"num_workers": 2},
            label_column="target",
            params=params,
            dmatrix_params={"data": {}},
            datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
