import math
import pytest
import pandas as pd

import lightgbm as lgbm

import ray
from ray import tune
from ray.air.checkpoint import Checkpoint
from ray.train.constants import TRAIN_DATASET_KEY

from ray.data.preprocessor import Preprocessor
from ray.train.lightgbm import LightGBMTrainer, load_checkpoint

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
    "objective": "binary",
    "metric": ["binary_logloss", "binary_error"],
}


def get_num_trees(booster: lgbm.Booster) -> int:
    return booster.current_iteration()


def test_fit_with_categoricals(ray_start_4_cpus):
    train_df_with_cat = train_df.copy()
    test_df_with_cat = test_df.copy()
    train_df_with_cat["categorical_column"] = pd.Series(
        (["A", "B"] * math.ceil(len(train_df_with_cat) / 2))[: len(train_df_with_cat)]
    ).astype("category")
    test_df_with_cat["categorical_column"] = pd.Series(
        (["A", "B"] * math.ceil(len(test_df_with_cat) / 2))[: len(test_df_with_cat)]
    ).astype("category")

    train_dataset = ray.data.from_pandas(train_df_with_cat)
    valid_dataset = ray.data.from_pandas(test_df_with_cat)
    trainer = LightGBMTrainer(
        scaling_config=scale_config,
        label_column="target",
        params=params,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    result = trainer.fit()
    checkpoint = result.checkpoint
    model, _ = load_checkpoint(checkpoint)
    assert model.pandas_categorical == [["A", "B"]]


def test_resume_from_checkpoint(ray_start_4_cpus, tmpdir):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = LightGBMTrainer(
        scaling_config=scale_config,
        label_column="target",
        params=params,
        num_boost_round=5,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    result = trainer.fit()
    checkpoint = result.checkpoint
    model, _ = load_checkpoint(checkpoint)
    assert get_num_trees(model) == 5

    # Move checkpoint to a different directory.
    checkpoint_dict = result.checkpoint.to_dict()
    checkpoint = Checkpoint.from_dict(checkpoint_dict)
    checkpoint_path = checkpoint.to_directory(tmpdir)
    resume_from = Checkpoint.from_directory(checkpoint_path)

    trainer = LightGBMTrainer(
        scaling_config=scale_config,
        label_column="target",
        params=params,
        num_boost_round=5,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        resume_from_checkpoint=resume_from,
    )
    result = trainer.fit()
    checkpoint = result.checkpoint
    xgb_model, _ = load_checkpoint(checkpoint)
    assert get_num_trees(xgb_model) == 10


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

    trainer = LightGBMTrainer(
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
    trainer = LightGBMTrainer(
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
        LightGBMTrainer(
            scaling_config={"num_workers": 2},
            label_column="target",
            params=params,
            datasets={"valid": valid_dataset},
        )
    with pytest.raises(KeyError, match="dmatrix_params"):
        LightGBMTrainer(
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
