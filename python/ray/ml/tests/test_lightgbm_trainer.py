import os
import pytest
import pandas as pd

import lightgbm as lgbm

import ray
from ray import tune
from ray.ml.checkpoint import Checkpoint
from ray.ml.constants import MODEL_KEY, PREPROCESSOR_KEY, TRAIN_DATASET_KEY

from ray.ml.train.integrations.lightgbm import LightGBMTrainer
from ray.ml.preprocessor import Preprocessor
from ray.ml.utils.gbdt_utils import DMATRIX_PARAMS_KEY, PARAMS_KEY

from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


scale_config = {"num_workers": 2}

data_raw = load_breast_cancer(as_frame=True)
dataset_df = data_raw["data"]
dataset_df["target"] = data_raw["target"]
train_df, test_df = train_test_split(dataset_df, test_size=0.3)

params = {
    "objective": "binary",
    "metric": ["binary_logloss", "binary_error"],
}


def get_num_trees(booster: lgbm.Booster) -> int:
    return booster.current_iteration()


def load_model_from_checkpoint(checkpoint: Checkpoint) -> lgbm.Booster:
    checkpoint_path = checkpoint.to_directory()
    lgbm_model = lgbm.Booster(model_file=os.path.join(checkpoint_path, MODEL_KEY))
    return lgbm_model


def test_fit(ray_start_4_cpus):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = LightGBMTrainer(
        scaling_config={"num_workers": 2},
        label_column="target",
        lightgbm_config={PARAMS_KEY: params},
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    trainer.fit()


def test_resume_from_checkpoint(ray_start_4_cpus, tmpdir):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = LightGBMTrainer(
        scaling_config=scale_config,
        label_column="target",
        lightgbm_config={PARAMS_KEY: params, "num_boost_round": 5},
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    result = trainer.fit()
    checkpoint = result.checkpoint
    lgbm_model = load_model_from_checkpoint(checkpoint)
    assert get_num_trees(lgbm_model) == 5

    # Move checkpoint to a different directory.
    checkpoint_dict = result.checkpoint.to_dict()
    checkpoint = Checkpoint.from_dict(checkpoint_dict)
    checkpoint_path = checkpoint.to_directory(tmpdir)
    resume_from = Checkpoint.from_directory(checkpoint_path)

    trainer = LightGBMTrainer(
        scaling_config=scale_config,
        label_column="target",
        lightgbm_config={PARAMS_KEY: params, "num_boost_round": 5},
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        resume_from_checkpoint=resume_from,
    )
    result = trainer.fit()
    checkpoint = result.checkpoint
    lgbm_model = load_model_from_checkpoint(checkpoint)
    assert get_num_trees(lgbm_model) == 10


def test_preprocessor_in_checkpoint(ray_start_4_cpus):
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
        lightgbm_config={PARAMS_KEY: params},
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        preprocessor=DummyPreprocessor(),
    )
    result = trainer.fit()
    lgbm_model = load_model_from_checkpoint(result.checkpoint)
    assert get_num_trees(lgbm_model) == 10
    assert result.checkpoint.to_dict()[PREPROCESSOR_KEY].is_same
    assert result.checkpoint.to_dict()[PREPROCESSOR_KEY].fitted_


def test_tune(ray_start_4_cpus):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = LightGBMTrainer(
        scaling_config=scale_config,
        label_column="target",
        lightgbm_config={PARAMS_KEY: {**params, **{"max_depth": 1}}},
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )

    tune.run(
        trainer.as_trainable(),
        config={"lightgbm_config": {PARAMS_KEY: {"max_depth": tune.randint(2, 4)}}},
        num_samples=2,
    )

    # Make sure original Trainer is not affected.
    assert trainer.lightgbm_config[PARAMS_KEY]["max_depth"] == 1

def test_validation(ray_start_4_cpus):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    with pytest.raises(KeyError, match=TRAIN_DATASET_KEY):
        LightGBMTrainer(
            scaling_config={"num_workers": 2},
            label_column="target",
            lightgbm_config={PARAMS_KEY: params},
            datasets={"valid": valid_dataset},
        )
    with pytest.raises(KeyError, match=PARAMS_KEY):
        LightGBMTrainer(
            scaling_config={"num_workers": 2},
            label_column="target",
            lightgbm_config={},
            datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        )
    with pytest.raises(KeyError, match=DMATRIX_PARAMS_KEY):
        LightGBMTrainer(
            scaling_config={"num_workers": 2},
            label_column="target",
            lightgbm_config={PARAMS_KEY: params, DMATRIX_PARAMS_KEY: {"data": {}}},
            datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        )

if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
